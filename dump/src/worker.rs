use std::sync::Arc;

use backon::{ExponentialBuilder, Retryable};
use common::{config::Config, BoxError};
use dataset_store::DatasetStore;
use futures::TryStreamExt as _;
use metadata_db::{JobId, JobNotifAction as Action, JobNotifRecvError, MetadataDb, WorkerNodeId};
use tokio_util::task::AbortOnDropHandle;

mod job;
mod job_set;

pub use self::job::JobDesc;
use self::{
    job::{Job, JobCtx},
    job_set::{JobSet, JoinError as JobSetJoinError},
};

pub struct Worker {
    node_id: WorkerNodeId,
    config: Arc<Config>,
    metadata_db: Arc<MetadataDb>,
    job_set: JobSet,
}

impl Worker {
    /// Create a new worker instance
    pub fn new(config: Arc<Config>, metadata_db: Arc<MetadataDb>, node_id: WorkerNodeId) -> Self {
        Self {
            node_id,
            config,
            metadata_db,
            job_set: Default::default(),
        }
    }

    pub async fn run(mut self) -> Result<(), WorkerError> {
        // Register the worker in the Metadata DB. and update the latest heartbeat timestamp
        // Retry on failure
        (|| self.metadata_db.register_worker(&self.node_id))
            .retry(ExponentialBuilder::default())
            .await
            .map_err(WorkerError::RegistrationFailed)?;

        // Establish a dedicated connection to the metadata DB, and start the heartbeat loop
        let mut heartbeat_loop_handle = {
            let fut = (|| self.metadata_db.worker_heartbeat_loop(self.node_id.clone()))
                .retry(ExponentialBuilder::default())
                .await
                .map_err(WorkerError::HeartbeatConnectionError)?;
            AbortOnDropHandle::new(tokio::spawn(fut))
        };

        // Start listening for job notifications. Retry on initial connection failure
        let mut job_notification_stream = {
            let listener = (|| self.metadata_db.listen_for_job_notifications())
                .retry(ExponentialBuilder::default())
                .await
                .map_err(WorkerError::JobNotifListenerConnectionError)?;

            let stream = listener
                .into_stream()
                .map_err(WorkerError::JobNotifRecvError);
            std::pin::pin!(stream)
        };

        // TODO: Review the bootstrap logic (LNSD)
        // let scheduled_jobs = self.metadata_db.scheduled_jobs(&self.node_id).await?;
        // for job_id in scheduled_jobs {
        //     let job = Job::load(job_ctx.clone(), &job_id)
        //         .await
        //         .map_err(WorkerError::JobLoadError)?;
        //     spawn_job(job);
        // }

        loop {
            tokio::select! { biased;
                // 1. Poll the heartbeat loop task handle
                res = &mut heartbeat_loop_handle => {
                    match res {
                        Ok(Ok(_)) => {
                            // The heartbeat loop must never exit, this is a fatal error.
                            // Exit the worker
                            return Err(WorkerError::HeartbeatTaskFailed(
                                "heartbeat loop task exited".into(),
                            ));
                        }
                        Ok(Err(err)) => {
                            // A fatal error occurred in the heartbeat loop.
                            // Exit the worker
                            return Err(WorkerError::HeartbeatUpdateFailed(err));
                        }
                        Err(err) => {
                            // A fatal error occurred in the heartbeat loop task.
                            // Exit the worker
                            return Err(WorkerError::HeartbeatTaskFailed(err.into()));
                        }
                    }
                },

                // 2. Poll the job notifications stream
                next = job_notification_stream.try_next() => {
                    let next = match next {
                        Ok(next) => next,
                        Err(WorkerError::JobNotifRecvError(JobNotifRecvError::DeserializationFailed(err))) => {
                            tracing::error!(node_id=%self.node_id, error=%err, "job notification deserialization failed, skipping");
                            continue;
                        }
                        // A fatal error occurred in the job notification stream, exit the worker
                        Err(err) => return Err(err),
                    };

                    let (job_id, action) = match next {
                        Some(notification) => {
                            // Ignore notifications targeting other workers
                            if self.node_id != notification.node_id {
                                continue;
                            }
                            (notification.job_id, notification.action)
                        }
                        None => {
                            tracing::error!(node_id=%self.node_id, "job notification stream closed");
                            continue;
                        }
                    };

                    self.handle_job_action(job_id, action).await?;
                },

                // 3. Poll the job set for job execution results
                Some((job_id, result)) = self.job_set.join_next_with_id() => {
                    self.handle_job_result(job_id, result).await?;
                }
            }
        }
    }

    async fn handle_job_action(
        &mut self,
        job_id: JobId,
        action: Action,
    ) -> Result<(), WorkerError> {
        match action {
            Action::Start => {
                tracing::info!(node_id=%self.node_id, %job_id, "job start requested");

                // Load the job from the database. Retry on failure
                // TODO: Make Job struct load agnostic (LNSD)
                let job = (|| {
                    let ctx = JobCtx {
                        config: self.config.clone(),
                        metadata_db: self.metadata_db.clone(),
                        dataset_store: DatasetStore::new(
                            self.config.clone(),
                            self.metadata_db.clone(),
                        ),
                        data_store: self.config.data_store.clone(),
                    };
                    Job::load(ctx, &job_id)
                })
                .retry(ExponentialBuilder::default())
                .await
                .map_err(WorkerError::JobLoadFailed)?;

                // Mark the job as RUNNING. Retry on failure
                (|| self.metadata_db.mark_job_running(&job_id))
                    .retry(ExponentialBuilder::default())
                    .await
                    .map_err(WorkerError::JobStatusUpdateFailed)?;

                // Spawn the job in the job set
                self.job_set.spawn(job_id, job);
            }
            Action::Stop => {
                // To avoid job state update races, mark it as STOPPING before actually aborting the job
                tracing::info!(node_id=%self.node_id, %job_id, "job stop requested");

                // Mark the job as STOPPING. Retry on failure
                (|| self.metadata_db.mark_job_stopping(&job_id))
                    .retry(ExponentialBuilder::default())
                    .await
                    .map_err(WorkerError::JobStatusUpdateFailed)?;

                self.job_set.abort(&job_id);
            }
            _ => {
                tracing::warn!(node_id=%self.node_id, %job_id, %action, "job action unhandled");
            }
        }

        Ok(())
    }

    async fn handle_job_result(
        &mut self,
        job_id: JobId,
        result: Result<(), JobSetJoinError>,
    ) -> Result<(), WorkerError> {
        match result {
            Ok(_) => {
                tracing::info!(node_id=%self.node_id, %job_id, "job completed successfully");

                // Mark the job as COMPLETED. Retry on failure
                (|| self.metadata_db.mark_job_completed(&job_id))
                    .retry(ExponentialBuilder::default())
                    .await
                    .map_err(WorkerError::JobStatusUpdateFailed)?;
            }
            Err(JobSetJoinError::Failed(err)) => {
                tracing::error!(node_id=%self.node_id, %job_id, "job failed: {:?}", err);

                // Mark the job as FAILED. Retry on failure
                (|| self.metadata_db.mark_job_failed(&job_id))
                    .retry(ExponentialBuilder::default())
                    .await
                    .map_err(WorkerError::JobStatusUpdateFailed)?;
            }
            Err(JobSetJoinError::Aborted) => {
                tracing::info!(node_id=%self.node_id, %job_id, "job cancelled");

                // Mark the job as STOPPED. Retry on failure
                (|| self.metadata_db.mark_job_stopped(&job_id))
                    .retry(ExponentialBuilder::default())
                    .await
                    .map_err(WorkerError::JobStatusUpdateFailed)?;
            }
            Err(JobSetJoinError::Panicked(err)) => {
                tracing::error!(node_id=%self.node_id, %job_id, error=%err, "job panicked");

                // Mark the job as FAILED. Retry on failure
                (|| self.metadata_db.mark_job_failed(&job_id))
                    .retry(ExponentialBuilder::default())
                    .await
                    .map_err(WorkerError::JobStatusUpdateFailed)?;
            }
        }

        Ok(())
    }
}

/// Worker errors
///
/// These are fatal errors that should cause the worker to exit.
#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    /// The worker failed to register in the metadata DB
    #[error("worker registration failed: {0}")]
    RegistrationFailed(metadata_db::Error),

    /// The worker heartbeat task connection failed to establish a dedicated connection to the metadata DB
    #[error("worker heartbeat connection establishment failed: {0}")]
    HeartbeatConnectionError(metadata_db::Error),

    /// The worker failed to update its heartbeat in the metadata DB
    #[error("worker heartbeat update failed: {0}")]
    HeartbeatUpdateFailed(metadata_db::Error),

    /// The worker heartbeat task failed
    #[error("worker heartbeat task failed: {0}")]
    HeartbeatTaskFailed(BoxError),

    /// A fatal error occurred while establishing the job notification listener
    /// connection to the metadata DB
    #[error("error establishing job notification listener connection: {0}")]
    JobNotifListenerConnectionError(metadata_db::Error),

    /// A fatal error occurred while listening to job notifications
    #[error("error listening to job notifications: {0}")]
    JobNotifRecvError(metadata_db::JobNotifRecvError),

    /// A fatal error occurred while loading a job
    #[error("error loading job: {0}")]
    JobLoadFailed(BoxError),

    /// A fatal error occurred while updating a job status
    #[error("failed to update job status: {0}")]
    JobStatusUpdateFailed(metadata_db::Error),
}

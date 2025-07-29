use std::{sync::Arc, time::Duration};

use common::{BoxError, config::Config, notification_multiplexer};
use dataset_store::DatasetStore;
use futures::TryStreamExt as _;
use metadata_db::MetadataDb;
use tokio::time::{Interval, MissedTickBehavior};
use tokio_util::task::AbortOnDropHandle;

mod job;
mod job_set;
mod meta;

pub use self::job::JobDesc;
use self::{
    job::Job,
    job_set::{JobSet, JoinError as JobSetJoinError},
    meta::{
        JobId, JobMeta, JobStatus, NotifAction as Action, NotifRecvError, WorkerMetadataDb,
        WorkerNodeId,
    },
};
use crate::Ctx;

pub struct Worker {
    node_id: WorkerNodeId,
    reconcile_interval: Interval,

    meta: WorkerMetadataDb,
    job_ctx: Ctx,
    job_set: JobSet,
}

impl Worker {
    /// Create a new worker instance
    pub fn new(config: Arc<Config>, metadata_db: Arc<MetadataDb>, node_id: WorkerNodeId) -> Self {
        let dataset_store = DatasetStore::new(config.clone(), metadata_db.clone());
        let data_store = config.data_store.clone();
        let meta = WorkerMetadataDb::new(metadata_db.clone());

        let mut reconcile_interval = tokio::time::interval(Duration::from_secs(60));
        reconcile_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let notification_multiplexer =
            Arc::new(notification_multiplexer::spawn((*metadata_db).clone()));

        Self {
            node_id,
            reconcile_interval,
            meta,
            job_ctx: Ctx {
                config,
                metadata_db,
                dataset_store,
                data_store,
                notification_multiplexer,
            },
            job_set: Default::default(),
        }
    }

    pub async fn run(mut self) -> Result<(), WorkerError> {
        // Register the worker in the Metadata DB. and update the latest heartbeat timestamp.
        // Retry on failure
        self.meta
            .register_worker(&self.node_id)
            .await
            .map_err(WorkerError::RegistrationFailed)?;

        // Establish a dedicated connection to the metadata DB, and start the heartbeat loop
        let mut heartbeat_loop_handle = {
            let fut = self
                .meta
                .worker_heartbeat_loop(&self.node_id)
                .await
                .map_err(WorkerError::HeartbeatConnectionError)?;
            AbortOnDropHandle::new(tokio::spawn(fut))
        };

        // Start listening for job notifications. Retry on initial connection failure
        let mut job_notification_stream = {
            let listener = self
                .meta
                .listen_for_job_notifications()
                .await
                .map_err(WorkerError::JobNotifListenerConnectionError)?;

            let stream = listener
                .into_stream()
                .map_err(WorkerError::JobNotifRecvError);
            std::pin::pin!(stream)
        };

        // Worker bootstrap: If the worker is restarted, it needs to be able to resume its state
        // from the last known state.
        //
        // This is done by fetching all the active jobs (jobs in [`JobStatus::Scheduled`] or
        // [`JobStatus::Running`]) from the Metadata DB's jobs table and spawning them in the jobs
        // set, ensuring the worker is in sync with the Metadata DB's jobs table state.
        let scheduled_jobs = self
            .meta
            .get_scheduled_jobs(&self.node_id)
            .await
            .map_err(WorkerError::ActiveJobsFetchFailed)?;
        for job in scheduled_jobs {
            self.spawn_job(job).await?;
        }

        // Main loop
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
                        Err(WorkerError::JobNotifRecvError(NotifRecvError::DeserializationFailed(err))) => {
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

                    self.handle_job_notification_action(job_id, action).await?;
                },

                // 3. Poll the job set for job execution results
                Some((job_id, result)) = self.job_set.join_next_with_id() => {
                    self.handle_job_result(job_id, result).await?;
                }

                // 4. Periodically reconcile the jobs state with the Metadata DB jobs table state
                _ = self.reconcile_interval.tick() => {
                    self.reconcile_jobs().await?;
                }
            }
        }
    }

    /// Handles a job notification action
    ///
    /// This method is called when a job notification is received from the Metadata DB job
    /// notifications channel.
    async fn handle_job_notification_action(
        &mut self,
        job_id: JobId,
        action: Action,
    ) -> Result<(), WorkerError> {
        match action {
            Action::Start => {
                // Load the job from the metadata DB (retry on failure)
                let job = match self
                    .meta
                    .get_job(&job_id)
                    .await
                    .map_err(|err| WorkerError::JobLoadFailed(err.into()))?
                {
                    Some(job) => job,
                    None => {
                        // If the job is not found, just ignore the notification
                        tracing::warn!(node_id=%self.node_id, %job_id, "job not found");
                        return Ok(());
                    }
                };

                self.spawn_job(job).await
            }
            Action::Stop => self.abort_job(job_id).await,
            _ => {
                tracing::warn!(node_id=%self.node_id, %job_id, %action, "unhandled job action");
                Ok(())
            }
        }
    }

    /// Handles the results returned by a job in the job set
    ///
    /// This method is called when a job in the job set completes, fails, or is aborted.
    /// It updates the job status in the Metadata DB.
    async fn handle_job_result(
        &mut self,
        job_id: JobId,
        result: Result<(), JobSetJoinError>,
    ) -> Result<(), WorkerError> {
        match result {
            Ok(_) => {
                tracing::info!(node_id=%self.node_id, %job_id, "job completed successfully");

                // Mark the job as COMPLETED (retry on failure)
                self.meta
                    .mark_job_completed(&job_id)
                    .await
                    .map_err(WorkerError::JobStatusUpdateFailed)?;
            }
            Err(JobSetJoinError::Failed(err)) => {
                tracing::error!(node_id=%self.node_id, %job_id, "job failed: {:?}", err);

                // Mark the job as FAILED (retry on failure)
                self.meta
                    .mark_job_failed(&job_id)
                    .await
                    .map_err(WorkerError::JobStatusUpdateFailed)?;
            }
            Err(JobSetJoinError::Aborted) => {
                tracing::info!(node_id=%self.node_id, %job_id, "job cancelled");

                // Mark the job as STOPPED (retry on failure)
                self.meta
                    .mark_job_stopped(&job_id)
                    .await
                    .map_err(WorkerError::JobStatusUpdateFailed)?;
            }
            Err(JobSetJoinError::Panicked(err)) => {
                tracing::error!(node_id=%self.node_id, %job_id, error=%err, "job panicked");

                // Mark the job as FAILED (retry on failure)
                self.meta
                    .mark_job_failed(&job_id)
                    .await
                    .map_err(WorkerError::JobStatusUpdateFailed)?;
            }
        }

        Ok(())
    }

    /// Synchronizes the worker's jobs state with the Metadata DB jobs table state.
    ///
    /// This method ensures the worker remains in sync with the authoritative job state stored
    /// in the metadata database, compensating for potentially missed job notifications.
    ///
    /// The list of active jobs is fetched from the Metadata DB and the worker job set is updated to
    /// reflect the DB state.
    ///
    /// This method is called periodically to maintain consistency between the worker and database state.
    async fn reconcile_jobs(&mut self) -> Result<(), WorkerError> {
        let active_jobs = match self
            .meta
            .get_active_jobs(&self.node_id)
            .await
            .map_err(WorkerError::ActiveJobsFetchFailed)
        {
            Ok(active_jobs) => active_jobs,
            Err(err) => {
                // If any error occurs while fetching active jobs, we consider the worker to be
                // in a degraded state and we skip the reconciliation
                tracing::warn!(node_id=%self.node_id, error=%err, "failed to fetch active jobs");
                return Ok(());
            }
        };

        for job in active_jobs {
            match job.status {
                JobStatus::Scheduled | JobStatus::Running => {
                    self.spawn_job(job).await?;
                }
                JobStatus::StopRequested => {
                    self.abort_job(job.id).await?;
                }
                _ => {} // No-op
            }
        }

        Ok(())
    }

    /// Spawns a job in the job set
    ///
    /// This method is called when a job is started by the user or the scheduler.
    /// It marks the job as RUNNING and spawns the job in the job set.
    async fn spawn_job(&mut self, job: JobMeta) -> Result<(), WorkerError> {
        tracing::info!(node_id=%self.node_id, %job.id, "job start requested");

        // Mark the job as RUNNING (retry on failure)
        if job.status != JobStatus::Running {
            self.meta
                .mark_job_running(&job.id)
                .await
                .map_err(WorkerError::JobStatusUpdateFailed)?;
        }

        // Construct the job instance and spawn it in the job set
        let job_id = job.id;
        let job_desc = serde_json::from_value(job.desc).map_err(|err| {
            WorkerError::JobLoadFailed(format!("invalid job descriptor: {}", err).into())
        })?;

        let job = Job::try_from_descriptor(self.job_ctx.clone(), job_id, job_desc)
            .await
            .map_err(WorkerError::JobLoadFailed)?;
        self.job_set.spawn(job_id, job);

        Ok(())
    }

    /// Aborts a job in the job set
    ///
    /// To avoid job state update races, this method marks the job as STOPPING and
    /// then notifies the job set to abort the job.
    async fn abort_job(&mut self, job_id: JobId) -> Result<(), WorkerError> {
        tracing::info!(node_id=%self.node_id, %job_id, "job stop requested");

        // Mark the job as STOPPING (retry on failure)
        self.meta
            .mark_job_stopping(&job_id)
            .await
            .map_err(WorkerError::JobStatusUpdateFailed)?;

        self.job_set.abort(&job_id);
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

    /// A fatal error occurred while fetching active jobs at worker startup
    #[error("failed to fetch active jobs: {0}")]
    ActiveJobsFetchFailed(metadata_db::Error),

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

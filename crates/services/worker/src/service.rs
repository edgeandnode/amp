use std::{future::Future, sync::Arc, time::Duration};

use backon::{ExponentialBuilder, Retryable};
use common::{config::Config, store::Store as DataStore};
use dataset_store::{
    DatasetStore, manifests::DatasetManifestsStore, providers::ProviderConfigsStore,
};
use futures::{TryStreamExt as _, future::BoxFuture};
use metadata_db::{
    Error as MetadataDbError, MetadataDb, NotificationMultiplexerHandle,
    WorkerNotifListener as NotifListener, notification_multiplexer,
};
use monitoring::{logging, telemetry::metrics::Meter};
use tokio::time::MissedTickBehavior;
use tokio_util::task::AbortOnDropHandle;
use tracing::instrument;

mod error;
mod job_impl;
mod job_queue;
mod job_set;

pub use self::error::{
    AbortJobError, HeartbeatTaskError, InitError, JobCreationError, JobResultError,
    NotificationError, ReconcileError, RuntimeError, SpawnJobError, StartActionError,
};
use self::{
    job_queue::JobQueue,
    job_set::{JobSet, JoinError as JobSetJoinError},
};
use crate::{
    info::WorkerInfo,
    job::{Job, JobAction, JobId, JobNotification, JobStatus},
    node_id::NodeId,
};

/// Run the worker service
///
/// This function creates and runs a worker instance with the provided configuration
/// using a two-phase initialization pattern:
///
/// - **Phase 1 (Initialization)**: Worker registration, heartbeat setup, notification
///   listener setup, and bootstrap. Errors in this phase are returned as [`InitError`].
/// - **Phase 2 (Runtime)**: Main event loop handling heartbeats, job notifications,
///   job results, and reconciliation. Errors in this phase are returned as [`RuntimeError`].
pub async fn new(
    node_id: NodeId,
    config: Arc<Config>,
    metadata_db: MetadataDb,
    meter: Option<Meter>,
) -> Result<impl Future<Output = Result<(), RuntimeError>>, InitError> {
    // Create worker info from build information
    let worker_info = WorkerInfo {
        version: Some(config.build_info.version.clone()),
        commit_sha: Some(config.build_info.commit_sha.clone()),
        commit_timestamp: Some(config.build_info.commit_timestamp.clone()),
        build_date: Some(config.build_info.build_date.clone()),
    };

    // Register the worker in the Metadata DB and update the latest heartbeat timestamp.
    // Retry on failure
    register_worker_with_retry(&metadata_db, &node_id, &worker_info)
        .await
        .map_err(InitError::Registration)?;

    // Establish a dedicated connection to the metadata DB, and start the heartbeat loop
    let heartbeat_fut = worker_heartbeat_loop_with_retry(&metadata_db, node_id.clone())
        .await
        .map_err(InitError::HeartbeatSetup)?;
    let mut heartbeat_loop_handle = AbortOnDropHandle::new(tokio::spawn(heartbeat_fut));

    // Start listening for job notifications. Retry on initial connection failure
    let listener = listen_for_job_notifications_with_retry(&metadata_db, &node_id)
        .await
        .map_err(InitError::NotificationSetup)?;

    // Create the job queue
    let queue = JobQueue::new(metadata_db.clone());

    // Construct dataset store
    let dataset_store = {
        let provider_configs_store =
            ProviderConfigsStore::new(config.providers_store.prefixed_store());
        let dataset_manifests_store =
            DatasetManifestsStore::new(config.manifests_store.prefixed_store());
        DatasetStore::new(
            metadata_db.clone(),
            provider_configs_store,
            dataset_manifests_store,
        )
    };

    // Clone data store
    let data_store = config.data_store.clone();

    // Create notification multiplexer
    let notification_multiplexer = Arc::new(notification_multiplexer::spawn(metadata_db.clone()));

    // Worker bootstrap: If the worker is restarted, it needs to be able to resume its state
    // from the last known state.
    //
    // This is done by fetching all the active jobs (jobs in [`JobStatus::Scheduled`] or
    // [`JobStatus::Running`]) from the queue and spawning them in the jobs
    // set, ensuring the worker is in sync with the queue state.
    let scheduled_jobs = queue
        .get_scheduled_jobs(&node_id)
        .await
        .map_err(InitError::BootstrapFetchScheduledJobs)?;

    // Create the worker instance with pre-constructed dependencies
    let mut worker = Worker::new(
        node_id,
        queue,
        WorkerJobCtx {
            config,
            metadata_db: metadata_db.clone(),
            dataset_store,
            data_store,
            notification_multiplexer,
            meter,
        },
    );

    // Spawn all scheduled jobs
    for job in scheduled_jobs {
        let job_id = job.id;
        worker
            .spawn_job(job)
            .await
            .map_err(|source| InitError::BootstrapSpawnJob { job_id, source })?;
    }

    // The worker service future
    let fut = async move {
        // Setup reconcile interval
        let mut reconcile_interval = tokio::time::interval(Duration::from_secs(60));
        reconcile_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        // Setup job notification stream (must be inside the async move block to avoid borrow issues)
        let stream = listener
            .into_stream::<JobNotification>()
            .map_err(NotificationError::DeserializationFailed);
        let mut job_notification_stream = std::pin::pin!(stream);

        // Main loop
        loop {
            tokio::select! { biased;
                // 1. Poll the heartbeat loop task handle
                res = &mut heartbeat_loop_handle => {
                    match res {
                        Ok(Ok(())) => {
                            // The heartbeat loop must never exit, this is a fatal error.
                            // Exit the worker
                            return Err(RuntimeError::HeartbeatTaskDied(
                                HeartbeatTaskError::UnexpectedExit,
                            ));
                        }
                        Ok(Err(err)) => {
                            // A fatal error occurred in the heartbeat loop.
                            // Exit the worker
                            return Err(RuntimeError::HeartbeatTaskDied(
                                HeartbeatTaskError::UpdateFailed(err),
                            ));
                        }
                        Err(err) => {
                            // A fatal error occurred in the heartbeat loop task.
                            // Exit the worker
                            return Err(RuntimeError::HeartbeatTaskDied(
                                HeartbeatTaskError::Panicked(err.into()),
                            ));
                        }
                    }
                },

                // 2. Poll the job notifications stream
                next = job_notification_stream.try_next() => {
                    let notif = match next {
                        Ok(Some(notif)) => notif,
                        Ok(None) => {
                            tracing::error!(node_id=%worker.node_id, "job notification stream closed");
                            // The stream has been closed, which is unexpected.
                            // Attempt to re-establish the listener after a short delay
                            tokio::time::sleep(Duration::from_secs(2)).await;
                            continue;
                        }
                        Err(NotificationError::DeserializationFailed(err)) => {
                            tracing::error!(node_id=%worker.node_id, error=%err, error_source = logging::error_source(&err), "job notification deserialization failed, skipping");
                            continue;
                        }
                        // These should never occur from the stream itself
                        Err(err) => {
                            tracing::error!(node_id=%worker.node_id, error=%err, error_source = logging::error_source(&err), "unexpected notification error");
                            continue;
                        }
                    };

                    worker.handle_job_notification_action(notif.job_id, notif.action)
                        .await
                        .map_err(RuntimeError::NotificationHandling)?;
                },

                // 3. Poll the job set for job execution results
                Some((job_id, result)) = worker.job_set.join_next_with_id() => {
                    worker.handle_job_result(job_id, result)
                        .await
                        .map_err(RuntimeError::JobResultHandling)?;
                }

                // 4. Periodically reconcile the jobs state with the Metadata DB jobs table state
                _ = reconcile_interval.tick() => {
                    worker.reconcile_jobs()
                        .await
                        .map_err(RuntimeError::Reconciliation)?;
                }
            }
        }
    };

    Ok(fut)
}

/// Worker job context parameters
#[derive(Clone)]
pub(crate) struct WorkerJobCtx {
    pub config: Arc<Config>,
    pub metadata_db: MetadataDb,
    pub dataset_store: Arc<DatasetStore>,
    pub data_store: Arc<DataStore>,
    pub notification_multiplexer: Arc<NotificationMultiplexerHandle>,
    pub meter: Option<Meter>,
}

pub struct Worker {
    node_id: NodeId,
    queue: JobQueue,
    job_ctx: WorkerJobCtx,
    job_set: JobSet,
}

impl Worker {
    /// Create a new worker instance
    #[must_use]
    pub(crate) fn new(node_id: NodeId, queue: JobQueue, job_ctx: WorkerJobCtx) -> Self {
        Self {
            node_id,
            queue,
            job_ctx,
            job_set: JobSet::default(),
        }
    }

    /// Handles a job notification action
    ///
    /// This method is called when a job notification is received from the Metadata DB job
    /// notifications channel.
    async fn handle_job_notification_action(
        &mut self,
        job_id: JobId,
        action: JobAction,
    ) -> Result<(), NotificationError> {
        match action {
            JobAction::Start => {
                // Load the job from the queue (retry on failure)
                let job = self.queue.get_job(job_id).await.map_err(|err| {
                    NotificationError::StartActionFailed {
                        job_id,
                        source: StartActionError::JobLoadFailed(err),
                    }
                })?;

                let Some(job) = job else {
                    // If the job is not found, just ignore the notification
                    tracing::warn!(node_id=%self.node_id, %job_id, "job not found");
                    return Ok(());
                };

                self.spawn_job(job)
                    .await
                    .map_err(|error| NotificationError::StartActionFailed {
                        job_id,
                        source: StartActionError::SpawnFailed(error),
                    })
            }
            JobAction::Stop => {
                self.abort_job(job_id)
                    .await
                    .map_err(|error| NotificationError::StopActionFailed {
                        job_id,
                        source: error,
                    })
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
    ) -> Result<(), JobResultError> {
        match result {
            Ok(()) => {
                tracing::info!(node_id=%self.node_id, %job_id, "job completed successfully");

                // Mark the job as COMPLETED (retry on failure)
                self.queue
                    .mark_job_completed(job_id)
                    .await
                    .map_err(|error| JobResultError::MarkCompletedFailed {
                        job_id,
                        source: error,
                    })?;
            }
            Err(JobSetJoinError::Failed(err)) => {
                tracing::error!(node_id=%self.node_id, %job_id, error=%err, error_source = logging::error_source(&*err), "job failed");

                // Mark the job as FAILED (retry on failure)
                self.queue.mark_job_failed(job_id).await.map_err(|error| {
                    JobResultError::MarkFailedFailed {
                        job_id,
                        source: error,
                    }
                })?;
            }
            Err(JobSetJoinError::Aborted) => {
                tracing::info!(node_id=%self.node_id, %job_id, "job cancelled");

                // Mark the job as STOPPED (retry on failure)
                self.queue.mark_job_stopped(job_id).await.map_err(|error| {
                    JobResultError::MarkStoppedFailed {
                        job_id,
                        source: error,
                    }
                })?;
            }
            Err(JobSetJoinError::Panicked(err)) => {
                tracing::error!(node_id=%self.node_id, %job_id, error=%err, error_source = logging::error_source(&*err), "job panicked");

                // Mark the job as FAILED (retry on failure)
                self.queue.mark_job_failed(job_id).await.map_err(|error| {
                    JobResultError::MarkFailedFailed {
                        job_id,
                        source: error,
                    }
                })?;
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
    async fn reconcile_jobs(&mut self) -> Result<(), ReconcileError> {
        let active_jobs = match self.queue.get_active_jobs(&self.node_id).await {
            Ok(active_jobs) => active_jobs,
            Err(err) => {
                // If any error occurs while fetching active jobs, we consider the worker to be
                // in a degraded state and we skip the reconciliation
                tracing::warn!(node_id=%self.node_id, error=%err, error_source = logging::error_source(&err), "failed to fetch active jobs");
                return Ok(());
            }
        };

        for job in active_jobs {
            match job.status {
                JobStatus::Scheduled | JobStatus::Running => {
                    let job_id = job.id;
                    self.spawn_job(job)
                        .await
                        .map_err(|error| ReconcileError::SpawnJobFailed {
                            job_id,
                            source: error,
                        })?;
                }
                JobStatus::StopRequested => {
                    self.abort_job(job.id).await.map_err(|error| {
                        ReconcileError::AbortJobFailed {
                            job_id: job.id,
                            source: error,
                        }
                    })?;
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
    #[instrument(skip(self, job), fields(node_id=%self.node_id, %job.id))]
    async fn spawn_job(&mut self, job: Job) -> Result<(), SpawnJobError> {
        if self.job_set.job_running(&job.id) {
            tracing::trace!("Job already spawned, skipping.");
            return Ok(());
        }

        tracing::debug!("job start requested");

        // Mark the job as RUNNING (retry on failure)
        if job.status != JobStatus::Running {
            self.queue
                .mark_job_running(job.id)
                .await
                .map_err(SpawnJobError::StatusUpdateFailed)?;
        }

        // Construct the job instance and spawn it in the job set
        let job_id = job.id;
        let job_desc: crate::job::JobDescriptor =
            serde_json::from_value(job.desc).map_err(SpawnJobError::DescriptorParseFailed)?;

        let job_fut = job_impl::new(self.job_ctx.clone(), job_id, job_desc)
            .await
            .map_err(SpawnJobError::JobInitializationFailed)?;

        self.job_set.spawn(job_id, job_fut);

        Ok(())
    }

    /// Aborts a job in the job set
    ///
    /// To avoid job state update races, this method marks the job as STOPPING and
    /// then notifies the job set to abort the job.
    async fn abort_job(&mut self, job_id: JobId) -> Result<(), AbortJobError> {
        tracing::info!(node_id=%self.node_id, %job_id, "job stop requested");

        // Mark the job as STOPPING (retry on failure)
        self.queue
            .mark_job_stopping(job_id)
            .await
            .map_err(AbortJobError)?;

        self.job_set.abort(job_id);
        Ok(())
    }
}

/// Registers a worker in the metadata DB.
///
/// This operation is idempotent. Retries on connection errors.
async fn register_worker_with_retry(
    metadata_db: &MetadataDb,
    node_id: &NodeId,
    info: &WorkerInfo,
) -> Result<(), MetadataDbError> {
    (|| metadata_db::workers::register(metadata_db, node_id, info))
        .retry(with_policy())
        .when(MetadataDbError::is_connection_error)
        .notify(|err, dur| {
            tracing::warn!(
                node_id = %node_id,
                error = %err, error_source = logging::error_source(&err),
                "Connection error while registering worker. Retrying in {:.1}s",
                dur.as_secs_f32()
            );
        })
        .await
}

/// Establishes a connection to the metadata DB and returns a future that runs the worker
/// heartbeat loop.
///
/// If the initial connection fails, the method will retry the reconnection with an exponential
/// backoff. Retries on connection errors.
async fn worker_heartbeat_loop_with_retry(
    metadata_db: &MetadataDb,
    node_id: NodeId,
) -> Result<BoxFuture<'static, Result<(), MetadataDbError>>, MetadataDbError> {
    let node_id_str = node_id.to_string();
    (move || metadata_db::workers::heartbeat_loop(metadata_db, node_id.clone()))
        .retry(with_policy())
        .when(MetadataDbError::is_connection_error)
        .notify(|err, dur| {
            tracing::warn!(
                node_id = %node_id_str,
                error = %err, error_source = logging::error_source(&err),
                "Worker heartbeat connection establishment failed. Retrying in {:.1}s",
                dur.as_secs_f32()
            );
        })
        .await
}

/// Listens for job notifications from the metadata DB.
///
/// This method establishes a connection to the metadata DB and returns a future that runs the
/// job notification listener loop. The listener will only yield notifications targeted to the
/// specified `node_id`. If the initial connection fails, the method will retry with an
/// exponential backoff. Retries on connection errors.
async fn listen_for_job_notifications_with_retry(
    metadata_db: &MetadataDb,
    node_id: &NodeId,
) -> Result<NotifListener, MetadataDbError> {
    let node_id = node_id.to_owned();
    (|| async {
        metadata_db::workers::listen_for_job_notif(metadata_db, node_id.clone()).await
    })
    .retry(with_policy())
    .when(MetadataDbError::is_connection_error)
    .notify(|err, dur| {
        tracing::warn!(
            node_id = %node_id,
            error = %err, error_source = logging::error_source(&err),
            "Failed to establish connection to listen for job notifications. Retrying in {:.1}s",
            dur.as_secs_f32()
        );
    })
    .await
}

/// A retry policy for the worker metadata DB operations.
///
/// The retry policy is an exponential backoff with:
/// - jitter: false
/// - factor: 2
/// - `min_delay`: 1s
/// - `max_delay`: 60s
/// - `max_times`: 3
#[inline]
fn with_policy() -> ExponentialBuilder {
    ExponentialBuilder::default()
}

use std::{sync::Arc, time::Duration};

use common::config::Config;
use dataset_store::{
    DatasetStore, manifests::DatasetManifestsStore, providers::ProviderConfigsStore,
};
use dump::Ctx;
use futures::TryStreamExt as _;
use metadata_db::{MetadataDb, notification_multiplexer};
use tokio::time::{Interval, MissedTickBehavior};
use tokio_util::task::AbortOnDropHandle;

mod job_set;

use self::job_set::{JobSet, JoinError as JobSetJoinError};
use crate::{
    AbortJobError, BootstrapError, Error, HeartbeatSetupError, HeartbeatTaskError, JobId,
    JobResultError, MainLoopError, NodeId, NotificationError, NotificationSetupError,
    ReconcileError, RegistrationError, SpawnJobError, StartActionError, WorkerInfo,
    db::{JobMeta, JobStatus, MetadataDbRetryExt as _},
    jobs::{Action, Job, Notification},
};

pub struct Worker {
    node_id: NodeId,
    worker_info: WorkerInfo,
    reconcile_interval: Interval,

    meta: MetadataDb,
    job_ctx: Ctx,
    job_set: JobSet,
    metrics: Option<Arc<dump::metrics::MetricsRegistry>>,
    meter: Option<monitoring::telemetry::metrics::Meter>,
}

impl Worker {
    /// Create a new worker instance
    #[must_use]
    pub fn new(
        config: Arc<Config>,
        metadata_db: MetadataDb,
        node_id: NodeId,
        meter: Option<monitoring::telemetry::metrics::Meter>,
    ) -> Self {
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
        let data_store = config.data_store.clone();

        let mut reconcile_interval = tokio::time::interval(Duration::from_secs(60));
        reconcile_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let notification_multiplexer =
            Arc::new(notification_multiplexer::spawn(metadata_db.clone()));

        // Create metrics registry if meter is available
        let metrics = meter
            .as_ref()
            .map(|m| Arc::new(dump::metrics::MetricsRegistry::new(m)));

        // Create worker info with build information
        let worker_info = WorkerInfo {
            version: Some(config.build_info.version.clone()),
            commit_sha: Some(config.build_info.commit_sha.clone()),
            commit_timestamp: Some(config.build_info.commit_timestamp.clone()),
            build_date: Some(config.build_info.build_date.clone()),
        };

        Self {
            node_id,
            worker_info,
            reconcile_interval,
            meta: metadata_db.clone(),
            job_ctx: Ctx {
                config,
                metadata_db,
                dataset_store,
                data_store,
                notification_multiplexer,
            },
            job_set: JobSet::default(),
            metrics,
            meter,
        }
    }

    /// Run the worker main loop
    ///
    /// # Errors
    ///
    /// Returns an error if a fatal condition occurs that prevents the worker from continuing.
    pub async fn run(mut self) -> Result<(), Error> {
        // Register the worker in the Metadata DB. and update the latest heartbeat timestamp.
        // Retry on failure
        self.meta
            .register_worker_with_retry(&self.node_id, &self.worker_info)
            .await
            .map_err(|err| Error::Registration(RegistrationError(err)))?;

        // Establish a dedicated connection to the metadata DB, and start the heartbeat loop
        let mut heartbeat_loop_handle = {
            let fut = self
                .meta
                .worker_heartbeat_loop_with_retry(self.node_id.clone())
                .await
                .map_err(|err| Error::HeartbeatSetup(HeartbeatSetupError(err)))?;
            AbortOnDropHandle::new(tokio::spawn(fut))
        };

        // Start listening for job notifications. Retry on initial connection failure
        let mut job_notification_stream = {
            let listener = self
                .meta
                .listen_for_job_notifications_with_retry(&self.node_id)
                .await
                .map_err(|err| Error::NotificationSetup(NotificationSetupError(err)))?;

            let stream = listener
                .into_stream::<Notification>()
                .map_err(NotificationError::DeserializationFailed);
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
            .get_scheduled_jobs_with_retry(&self.node_id)
            .await
            .map_err(|err| Error::Bootstrap(BootstrapError::FetchScheduledJobsFailed(err)))?;
        for job in scheduled_jobs {
            let job_id = job.id.into();
            self.spawn_job(job).await.map_err(|error| {
                Error::Bootstrap(BootstrapError::SpawnJobFailed {
                    job_id,
                    source: error,
                })
            })?;
        }

        // Main loop
        loop {
            tokio::select! { biased;
                // 1. Poll the heartbeat loop task handle
                res = &mut heartbeat_loop_handle => {
                    match res {
                        Ok(Ok(())) => {
                            // The heartbeat loop must never exit, this is a fatal error.
                            // Exit the worker
                            return Err(Error::MainLoop(MainLoopError::HeartbeatTaskDied(
                                HeartbeatTaskError::UnexpectedExit,
                            )));
                        }
                        Ok(Err(err)) => {
                            // A fatal error occurred in the heartbeat loop.
                            // Exit the worker
                            return Err(Error::MainLoop(MainLoopError::HeartbeatTaskDied(
                                HeartbeatTaskError::UpdateFailed(err),
                            )));
                        }
                        Err(err) => {
                            // A fatal error occurred in the heartbeat loop task.
                            // Exit the worker
                            return Err(Error::MainLoop(MainLoopError::HeartbeatTaskDied(
                                HeartbeatTaskError::Panicked(err.into()),
                            )));
                        }
                    }
                },

                // 2. Poll the job notifications stream
                next = job_notification_stream.try_next() => {
                    let notif = match next {
                        Ok(Some(notif)) => notif,
                        Ok(None) => {
                            tracing::error!(node_id=%self.node_id, "job notification stream closed");
                            tokio::time::sleep(Duration::from_secs(2)).await;
                            continue;
                        }
                        Err(NotificationError::DeserializationFailed(err)) => {
                            tracing::error!(node_id=%self.node_id, error=%err, "job notification deserialization failed, skipping");
                            continue;
                        }
                        // These should never occur from the stream itself
                        Err(err) => {
                            tracing::error!(node_id=%self.node_id, error=%err, "unexpected notification error");
                            continue;
                        }
                    };

                    self.handle_job_notification_action(notif.job_id, notif.action)
                        .await
                        .map_err(|err| Error::MainLoop(MainLoopError::NotificationHandling(err)))?;
                },

                // 3. Poll the job set for job execution results
                Some((job_id, result)) = self.job_set.join_next_with_id() => {
                    self.handle_job_result(job_id, result)
                        .await
                        .map_err(|err| Error::MainLoop(MainLoopError::JobResultHandling(err)))?;
                }

                // 4. Periodically reconcile the jobs state with the Metadata DB jobs table state
                _ = self.reconcile_interval.tick() => {
                    self.reconcile_jobs()
                        .await
                        .map_err(|err| Error::MainLoop(MainLoopError::Reconciliation(err)))?;
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
    ) -> Result<(), NotificationError> {
        match action {
            Action::Start => {
                // Load the job from the metadata DB (retry on failure)
                let job = self.meta.get_job_with_retry(&job_id).await.map_err(|err| {
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
            Action::Stop => {
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
                self.meta
                    .mark_job_completed_with_retry(&job_id)
                    .await
                    .map_err(|error| JobResultError::MarkCompletedFailed {
                        job_id,
                        source: error,
                    })?;
            }
            Err(JobSetJoinError::Failed(err)) => {
                tracing::error!(node_id=%self.node_id, %job_id, "job failed: {:?}", err);

                // Mark the job as FAILED (retry on failure)
                self.meta
                    .mark_job_failed_with_retry(&job_id)
                    .await
                    .map_err(|error| JobResultError::MarkFailedFailed {
                        job_id,
                        source: error,
                    })?;
            }
            Err(JobSetJoinError::Aborted) => {
                tracing::info!(node_id=%self.node_id, %job_id, "job cancelled");

                // Mark the job as STOPPED (retry on failure)
                self.meta
                    .mark_job_stopped_with_retry(&job_id)
                    .await
                    .map_err(|error| JobResultError::MarkStoppedFailed {
                        job_id,
                        source: error,
                    })?;
            }
            Err(JobSetJoinError::Panicked(err)) => {
                tracing::error!(node_id=%self.node_id, %job_id, error=%err, "job panicked");

                // Mark the job as FAILED (retry on failure)
                self.meta
                    .mark_job_failed_with_retry(&job_id)
                    .await
                    .map_err(|error| JobResultError::MarkFailedFailed {
                        job_id,
                        source: error,
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
        let active_jobs = match self
            .meta
            .get_active_jobs_with_retry(self.node_id.clone())
            .await
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
                    let job_id = job.id.into();
                    self.spawn_job(job)
                        .await
                        .map_err(|error| ReconcileError::SpawnJobFailed {
                            job_id,
                            source: error,
                        })?;
                }
                JobStatus::StopRequested => {
                    let job_id = job.id.into();
                    self.abort_job(job_id).await.map_err(|error| {
                        ReconcileError::AbortJobFailed {
                            job_id,
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
    async fn spawn_job(&mut self, job: JobMeta) -> Result<(), SpawnJobError> {
        tracing::info!(node_id=%self.node_id, %job.id, "job start requested");

        // Mark the job as RUNNING (retry on failure)
        if job.status != JobStatus::Running {
            self.meta
                .mark_job_running_with_retry(&job.id.into())
                .await
                .map_err(SpawnJobError::StatusUpdateFailed)?;
        }

        // Construct the job instance and spawn it in the job set
        let job_id = job.id;
        let job_desc =
            serde_json::from_value(job.desc).map_err(SpawnJobError::DescriptorParseFailed)?;

        let job = Job::try_from_descriptor(
            self.job_ctx.clone(),
            job_id.into(),
            job_desc,
            self.metrics.clone(),
            self.meter.clone(),
        )
        .await
        .map_err(SpawnJobError::JobCreationFailed)?;

        self.job_set.spawn(job_id.into(), job);

        Ok(())
    }

    /// Aborts a job in the job set
    ///
    /// To avoid job state update races, this method marks the job as STOPPING and
    /// then notifies the job set to abort the job.
    async fn abort_job(&mut self, job_id: JobId) -> Result<(), AbortJobError> {
        tracing::info!(node_id=%self.node_id, %job_id, "job stop requested");

        // Mark the job as STOPPING (retry on failure)
        self.meta
            .mark_job_stopping_with_retry(&job_id)
            .await
            .map_err(AbortJobError)?;

        self.job_set.abort(job_id);
        Ok(())
    }
}

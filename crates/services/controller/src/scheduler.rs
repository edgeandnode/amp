//! Scheduler implementation for the controller service
//!
//! This module provides the concrete implementation of the `JobScheduler` trait defined in
//! `admin-api`. It handles job scheduling, management, and coordination with worker nodes.
//!
//! ## Architecture
//!
//! The scheduler follows a dependency inversion pattern:
//! - `admin-api` defines the `JobScheduler` trait (abstraction)
//! - `controller` provides `SchedulerImpl` (implementation)
//! - Admin API handlers depend on the trait, not the implementation
//!
//! ## Key Responsibilities
//!
//! - **Job Scheduling**: Select available workers and create new dataset dump jobs
//! - **Job Control**: Stop running jobs with transactional guarantees
//! - **Job Queries**: Retrieve job status and list jobs with pagination
//! - **Job Cleanup**: Delete jobs in terminal states (completed, stopped, failed)
//! - **Worker Coordination**: Send notifications to workers via PostgreSQL LISTEN/NOTIFY
//!
//! ## Implementation Details
//!
//! - Uses PostgreSQL for metadata storage and job state tracking
//! - Implements atomic job operations with database transactions
//! - Selects workers randomly from active worker pool

use std::time::Duration;

use admin_api::scheduler::{
    DeleteJobError, DeleteJobsByStatusError, GetJobError, GetWorkerError, JobDescriptor,
    ListJobsByDatasetError, ListJobsError, ListWorkersError, NodeSelector, ScheduleJobError,
    SchedulerJobs, SchedulerWorkers, StopJobError,
};
use async_trait::async_trait;
use datasets_common::{
    hash::Hash, hash_reference::HashReference, name::Name, namespace::Namespace,
};
use metadata_db::{
    Error as MetadataDbError, MetadataDb, jobs::JobStatusUpdateError, workers::Worker,
};
use monitoring::logging;
use rand::seq::IndexedRandom as _;
use worker::{
    job::{Job, JobId, JobNotification, JobStatus},
    node_id::NodeId,
};

/// A worker is considered active if it has sent a heartbeat in this period
///
/// The scheduler will only schedule new jobs on workers that have sent a heartbeat within
/// this interval. Workers that haven't sent heartbeats are considered dead or unavailable.
const DEAD_WORKER_INTERVAL: Duration = Duration::from_secs(5);

/// Concrete implementation of the `JobScheduler` trait
///
/// Manages job scheduling and lifecycle operations for the controller service.
/// This implementation coordinates with worker nodes to execute dataset extraction jobs.
///
/// Thread-safe for sharing across async tasks via `Arc<dyn JobScheduler>`.
pub struct Scheduler {
    metadata_db: MetadataDb,
}

impl Scheduler {
    /// Create a new scheduler instance
    pub fn new(metadata_db: MetadataDb) -> Self {
        Self { metadata_db }
    }

    /// Schedule a job with a pre-built descriptor
    ///
    /// Checks for existing scheduled or running jobs to avoid duplicates, selects an available
    /// worker node (either randomly, by exact worker_id, or by matching a glob pattern) and registers the job in the metadata database.
    async fn schedule_job_impl(
        &self,
        hash_reference: HashReference,
        job_descriptor: JobDescriptor,
        worker_id: Option<NodeSelector>,
    ) -> Result<JobId, ScheduleJobError> {
        // Avoid re-scheduling jobs in a scheduled or running state.
        // TODO: Deduplicate jobs based on an idempotency key (not in the job descriptor)
        let existing_jobs =
            metadata_db::jobs::get_by_dataset(&self.metadata_db, hash_reference.hash())
                .await
                .map_err(ScheduleJobError::CheckExistingJobs)?;
        for job in existing_jobs {
            if matches!(job.status.into(), JobStatus::Scheduled | JobStatus::Running) {
                return Ok(job.id.into());
            }
        }

        // Scheduling procedure for a new `DumpDataset` job:
        // 1. Choose a responsive node.
        // 2. Register the job in the metadata db.
        // 3. Send a `Start` command through `worker_actions` for that job.
        //
        // The worker node should then receive the notification and start the dump run.

        let candidates = metadata_db::workers::list_active(&self.metadata_db, DEAD_WORKER_INTERVAL)
            .await
            .map_err(ScheduleJobError::ListActiveWorkers)?;

        // If a specific worker_id is provided, use it.
        // If a glob pattern is provided, choose a random worker node that matches the pattern.
        // If no worker_id or glob pattern is provided, choose a random worker node from the list of active workers.
        let node_id = match worker_id {
            Some(NodeSelector::Exact(worker_id)) => {
                let worker_id_ref = metadata_db::workers::WorkerNodeId::from(&worker_id);
                if !candidates.contains(&worker_id_ref) {
                    return Err(ScheduleJobError::WorkerNotAvailable(worker_id));
                }
                worker_id_ref.to_owned()
            }
            Some(NodeSelector::Glob(pattern)) => {
                let matching: Vec<_> = candidates
                    .iter()
                    .filter(|c| pattern.matches_str(c.as_str()))
                    .collect();
                if matching.is_empty() {
                    return Err(ScheduleJobError::NoMatchingWorkers(pattern));
                }
                let Some(node_id) = matching.choose(&mut rand::rng()).cloned() else {
                    return Err(ScheduleJobError::NoMatchingWorkers(pattern));
                };
                node_id.to_owned()
            }
            None => {
                let Some(node_id) = candidates.choose(&mut rand::rng()).cloned() else {
                    return Err(ScheduleJobError::NoWorkersAvailable);
                };
                node_id
            }
        };

        let mut tx = self
            .metadata_db
            .begin_txn()
            .await
            .map_err(ScheduleJobError::BeginTransaction)?;

        let job_id = metadata_db::jobs::register(&mut tx, &node_id, job_descriptor)
            .await
            .map(Into::into)
            .map_err(ScheduleJobError::RegisterJob)?;

        metadata_db::workers::send_job_notif(&mut tx, node_id, &JobNotification::start(job_id))
            .await
            .map_err(ScheduleJobError::NotifyWorker)?;

        tx.commit()
            .await
            .map_err(ScheduleJobError::CommitTransaction)?;

        Ok(job_id)
    }

    /// Stop a running job with transactional guarantees
    ///
    /// Fetches the job, validates its state, updates it to stop-requested, and notifies the
    /// worker within a single database transaction for atomicity.
    async fn stop_job_impl(&self, job_id: JobId) -> Result<(), StopJobError> {
        // Begin a transaction to ensure atomicity
        let mut tx = self
            .metadata_db
            .begin_txn()
            .await
            .map_err(StopJobError::BeginTransaction)?;

        // Fetch the job to get its node_id and validate it exists
        let job = metadata_db::jobs::get_by_id(&mut tx, &job_id)
            .await
            .map_err(StopJobError::GetJob)?
            .ok_or(StopJobError::JobNotFound)?;

        // Attempt to stop the job
        metadata_db::jobs::request_stop(&mut tx, &job_id)
            .await
            .map_err(|err| match err {
                MetadataDbError::JobStatusUpdate(JobStatusUpdateError::NotFound) => {
                    StopJobError::JobNotFound
                }
                MetadataDbError::JobStatusUpdate(JobStatusUpdateError::StateConflict {
                    actual,
                    ..
                }) => match actual.into() {
                    JobStatus::Stopped
                    | JobStatus::Completed
                    | JobStatus::FailedRecoverable
                    | JobStatus::FailedFatal => StopJobError::JobAlreadyTerminated {
                        status: actual.into(),
                    },
                    _ => StopJobError::StateConflict {
                        current_status: actual.into(),
                    },
                },
                other => StopJobError::UpdateJobStatus(other),
            })?;

        // Notify the worker about the stop request (within the transaction)
        metadata_db::workers::send_job_notif(&mut tx, job.node_id, &JobNotification::stop(job_id))
            .await
            .map_err(StopJobError::SendNotification)?;

        // Commit the transaction
        tx.commit().await.map_err(StopJobError::CommitTransaction)?;

        Ok(())
    }

    /// Reconcile failed jobs by retrying recoverable failures with exponential backoff
    ///
    /// This method:
    /// 1. Queries failed recoverable jobs that are ready for retry (based on exponential backoff timing)
    /// 2. For each job: reschedules it on the same worker and sends notification
    ///
    /// Only `FailedRecoverable` jobs are retried. `FailedFatal` jobs remain in the database
    /// until manually removed by operators using `ampctl job prune --status error`.
    ///
    /// Jobs retry indefinitely with exponential backoff (2^next_retry_index seconds).
    /// Retry tracking is managed via the job_attempts table.
    pub async fn reconcile_failed_jobs(&self) -> Result<(), ReconcileFailedJobsError> {
        // Reschedule failed (recoverable) jobs that are ready for retry.
        let failed_jobs = metadata_db::jobs::get_failed_jobs_ready_for_retry(&self.metadata_db)
            .await
            .map_err(ReconcileFailedJobsError::GetFailedJobsReadyForRetry)?;

        if failed_jobs.is_empty() {
            return Ok(());
        }

        for job_with_retry in failed_jobs {
            let job = &job_with_retry.job;
            let job_id: JobId = job.id.into();
            let retry_index = job_with_retry.next_retry_index;

            let result: Result<(), RescheduleJobError> = async {
                let mut tx = self
                    .metadata_db
                    .begin_txn()
                    .await
                    .map_err(RescheduleJobError::BeginTransaction)?;

                metadata_db::jobs::reschedule(&mut tx, job.id, job.node_id.clone(), retry_index)
                    .await
                    .map_err(RescheduleJobError::RescheduleJob)?;

                metadata_db::workers::send_job_notif(
                    &mut tx,
                    job.node_id.clone(),
                    &JobNotification::start(job_id),
                )
                .await
                .map_err(RescheduleJobError::SendJobNotification)?;

                tx.commit()
                    .await
                    .map_err(RescheduleJobError::CommitTransaction)?;
                Ok(())
            }
            .await;

            if let Err(err) = result {
                tracing::error!(
                    job_id = %job_id,
                    retry_index = retry_index,
                    error = %err,
                    error_source = logging::error_source(&err),
                    "failed to reschedule and notify for failed job"
                );
            }
        }

        Ok(())
    }
}

/// Errors that occur during failed job reconciliation [`Scheduler::reconcile_failed_jobs`]
#[derive(Debug, thiserror::Error)]
pub enum ReconcileFailedJobsError {
    /// Failed to query jobs that are ready for retry
    ///
    /// This occurs when the database query to retrieve recoverable failed jobs
    /// (filtered by exponential backoff timing) fails. Without this list,
    /// no retry scheduling can proceed.
    #[error("failed to get failed jobs ready for retry")]
    GetFailedJobsReadyForRetry(#[source] metadata_db::Error),
}

/// Errors that occur when rescheduling a single failed job for retry
#[derive(Debug, thiserror::Error)]
pub enum RescheduleJobError {
    /// Failed to begin a database transaction for the reschedule operation
    ///
    /// The reschedule and notification are performed atomically within a
    /// transaction. This error indicates the transaction could not be started.
    #[error("failed to begin transaction")]
    BeginTransaction(#[source] metadata_db::Error),

    /// Failed to reschedule the job in the metadata database
    ///
    /// This occurs when updating the job's status and retry metadata fails.
    /// The job remains in its previous failed state and will be retried
    /// in the next reconciliation cycle.
    #[error("failed to reschedule job")]
    RescheduleJob(#[source] metadata_db::Error),

    /// Failed to send a start notification to the assigned worker
    ///
    /// This occurs when inserting the job notification into the worker's
    /// notification queue fails. Since this occurs within a transaction,
    /// neither the reschedule nor the notification will be persisted. The
    /// job remains in its previous failed state and will be retried in the
    /// next reconciliation cycle.
    #[error("failed to send job notification")]
    SendJobNotification(#[source] metadata_db::Error),

    /// Failed to commit the reschedule transaction
    ///
    /// This occurs when the atomic commit of the reschedule and notification
    /// fails. Both operations are rolled back and the job will be retried
    /// in the next reconciliation cycle.
    #[error("failed to commit transaction")]
    CommitTransaction(#[source] metadata_db::Error),
}

#[async_trait]
impl SchedulerJobs for Scheduler {
    async fn schedule_job(
        &self,
        dataset_reference: HashReference,
        job_descriptor: JobDescriptor,
        worker_id: Option<NodeSelector>,
    ) -> Result<JobId, ScheduleJobError> {
        self.schedule_job_impl(dataset_reference, job_descriptor, worker_id)
            .await
    }

    async fn stop_job(&self, job_id: JobId) -> Result<(), StopJobError> {
        self.stop_job_impl(job_id).await
    }

    async fn get_job(&self, job_id: JobId) -> Result<Option<Job>, GetJobError> {
        let job = metadata_db::jobs::get_by_id(&self.metadata_db, &job_id)
            .await
            .map_err(GetJobError)?
            .map(Into::into);
        Ok(job)
    }

    async fn list_jobs(
        &self,
        limit: i64,
        last_id: Option<JobId>,
        statuses: Option<&[JobStatus]>,
    ) -> Result<Vec<Job>, ListJobsError> {
        let statuses = statuses.map(|statuses| {
            statuses
                .iter()
                .map(|s| (*s).into())
                .collect::<Vec<metadata_db::jobs::JobStatus>>()
        });
        let jobs = metadata_db::jobs::list(&self.metadata_db, limit, last_id, statuses.as_deref())
            .await
            .map_err(ListJobsError)?
            .into_iter()
            .map(Into::into)
            .collect();
        Ok(jobs)
    }

    async fn delete_job(&self, job_id: JobId) -> Result<bool, DeleteJobError> {
        metadata_db::jobs::delete_if_terminal(&self.metadata_db, &job_id)
            .await
            .map_err(DeleteJobError)
    }

    async fn delete_jobs_in_terminal_state(&self) -> Result<usize, DeleteJobsByStatusError> {
        let status = JobStatus::terminal_statuses();
        metadata_db::jobs::delete_all_by_status(&self.metadata_db, status.map(Into::into))
            .await
            .map_err(DeleteJobsByStatusError)
    }

    async fn delete_completed_jobs(&self) -> Result<usize, DeleteJobsByStatusError> {
        metadata_db::jobs::delete_all_by_status(&self.metadata_db, [JobStatus::Completed.into()])
            .await
            .map_err(DeleteJobsByStatusError)
    }

    async fn delete_stopped_jobs(&self) -> Result<usize, DeleteJobsByStatusError> {
        metadata_db::jobs::delete_all_by_status(&self.metadata_db, [JobStatus::Stopped.into()])
            .await
            .map_err(DeleteJobsByStatusError)
    }

    async fn delete_failed_jobs(&self) -> Result<usize, DeleteJobsByStatusError> {
        metadata_db::jobs::delete_all_by_status(
            &self.metadata_db,
            [
                JobStatus::FailedRecoverable.into(),
                JobStatus::FailedFatal.into(),
            ],
        )
        .await
        .map_err(DeleteJobsByStatusError)
    }

    async fn list_jobs_by_dataset(
        &self,
        namespace: &Namespace,
        name: &Name,
        hash: &Hash,
    ) -> Result<Vec<Job>, ListJobsByDatasetError> {
        let jobs =
            metadata_db::jobs::list_by_dataset_reference(&self.metadata_db, namespace, name, hash)
                .await
                .map_err(ListJobsByDatasetError)?
                .into_iter()
                .map(Into::into)
                .collect();
        Ok(jobs)
    }
}

#[async_trait]
impl SchedulerWorkers for Scheduler {
    async fn list_workers(&self) -> Result<Vec<Worker>, ListWorkersError> {
        metadata_db::workers::list(&self.metadata_db)
            .await
            .map_err(ListWorkersError)
    }

    async fn get_worker_by_id(&self, node_id: &NodeId) -> Result<Option<Worker>, GetWorkerError> {
        metadata_db::workers::get_by_id(&self.metadata_db, node_id)
            .await
            .map_err(GetWorkerError)
    }
}

//! Job management module for metadata database
//!
//! This module provides functionality for managing distributed job queues with state tracking
//! and coordination between multiple worker nodes.

use sqlx::types::{
    JsonValue,
    chrono::{DateTime, Utc},
};

mod job_id;
mod job_status;
pub(crate) mod sql;

pub use self::{job_id::JobId, job_status::JobStatus};
use crate::{
    ManifestHash,
    db::Executor,
    error::Error,
    physical_table::{self, LocationId},
    workers::{WorkerNodeId, WorkerNodeIdOwned},
};

/// Schedules a job on the given worker
///
/// The job will only be scheduled if the locations are successfully locked.
///
/// This function performs in a single transaction:
///
///  1. Registers the job in the workers job queue
///  2. Locks the locations
///
/// If any of these steps fail, the transaction is rolled back.
///
/// **Note:** This function does not send notifications. The caller is responsible for
/// calling `send_job_notification` after successful job scheduling if worker notification
/// is required.
// TODO: Move to Admin API layer (scheduler)
#[tracing::instrument(skip(db), err)]
pub async fn schedule(
    db: &crate::MetadataDb,
    node_id: impl Into<WorkerNodeId<'_>> + std::fmt::Debug,
    job_desc: &str,
    locations: &[LocationId],
) -> Result<JobId, Error> {
    // Use a transaction, such that the job will only be scheduled if the locations are
    // successfully locked.
    let mut tx = db.begin_txn().await?;

    // Register the job in the workers job queue
    let job_id = register(&mut tx, node_id.into(), job_desc).await?;

    // Lock the locations for this job by assigning the job ID as the writer
    physical_table::assign_job_writer(&mut tx, locations, job_id).await?;

    tx.commit().await?;

    Ok(job_id)
}

/// Register a job in the queue with the default status (Scheduled)
///
/// **Note:** This function does not send notifications. The caller is responsible for
/// calling `send_job_notification` after successful job registration if worker notification
/// is required.
#[tracing::instrument(skip(exe), err)]
pub async fn register<'c, E>(
    exe: E,
    node_id: impl Into<WorkerNodeId<'_>> + std::fmt::Debug,
    job_desc: &str,
) -> Result<JobId, Error>
where
    E: Executor<'c>,
{
    sql::insert_with_default_status(exe, node_id.into(), job_desc)
        .await
        .map_err(Error::Database)
}

/// Update job status to StopRequested
///
/// This function will only update the job status if it's currently in a valid state
/// to be stopped (Scheduled or Running). If the job is already stopping, this is
/// considered success (idempotent behavior). If the job is in a terminal state
/// (Stopped, Completed, Failed), this returns a conflict error.
///
/// Returns an error if the job doesn't exist, is in a terminal state, or if there's a database error.
///
/// **Note:** This function does not send notifications. The caller is responsible for
/// calling `send_job_notification` after successful status update if worker notification
/// is required.
#[tracing::instrument(skip(exe), err)]
pub async fn request_stop<'c, E>(
    exe: E,
    job_id: impl Into<JobId> + std::fmt::Debug,
) -> Result<(), Error>
where
    E: Executor<'c>,
{
    // Try to update job status
    match sql::update_status_if_any_state(
        exe,
        job_id.into(),
        &[JobStatus::Running, JobStatus::Scheduled],
        JobStatus::StopRequested,
    )
    .await
    {
        Ok(()) => Ok(()),
        // Check if the job is already stopping (idempotent behavior)
        Err(JobStatusUpdateError::StateConflict {
            actual: JobStatus::StopRequested | JobStatus::Stopping,
            ..
        }) => Ok(()),
        Err(err) => Err(Error::JobStatusUpdate(err)),
    }
}

/// List jobs with cursor-based pagination support, optionally filtered by status
///
/// Uses cursor-based pagination where `last_job_id` is the ID of the last job
/// from the previous page. For the first page, pass `None` for `last_job_id`.
/// If `statuses` is provided, only jobs matching those statuses are returned.
#[tracing::instrument(skip(exe), err)]
pub async fn list<'c, E>(
    exe: E,
    limit: i64,
    last_job_id: Option<impl Into<JobId> + std::fmt::Debug>,
    statuses: Option<&[JobStatus]>,
) -> Result<Vec<Job>, Error>
where
    E: Executor<'c>,
{
    match last_job_id {
        None => sql::list_first_page(exe, limit, statuses).await,
        Some(id) => sql::list_next_page(exe, limit, id.into(), statuses).await,
    }
    .map_err(Error::Database)
}

/// List jobs by dataset reference (namespace, name, and manifest hash)
///
/// Queries the job descriptor JSONB field for matching jobs, avoiding joins to physical_tables.
#[tracing::instrument(skip(exe), err)]
pub async fn list_by_dataset_reference<'c, E>(
    exe: E,
    dataset_namespace: impl Into<crate::DatasetNamespace<'_>> + std::fmt::Debug,
    dataset_name: impl Into<crate::DatasetName<'_>> + std::fmt::Debug,
    manifest_hash: impl Into<ManifestHash<'_>> + std::fmt::Debug,
) -> Result<Vec<Job>, Error>
where
    E: Executor<'c>,
{
    sql::list_by_dataset_reference(
        exe,
        dataset_namespace.into(),
        dataset_name.into(),
        manifest_hash.into(),
    )
    .await
    .map_err(Error::Database)
}

/// Given a worker node ID, return all the scheduled jobs
///
/// A job is considered scheduled if it's in one of the following non-terminal states:
/// - [`JobStatus::Scheduled`]
/// - [`JobStatus::Running`]
///
/// This function is used to fetch all the jobs that the worker should be running after a restart.
#[tracing::instrument(skip(exe), err)]
pub async fn get_scheduled<'c, E>(
    exe: E,
    node_id: impl Into<WorkerNodeId<'_>> + std::fmt::Debug,
) -> Result<Vec<Job>, Error>
where
    E: Executor<'c>,
{
    sql::get_by_node_id_and_statuses(
        exe,
        node_id.into(),
        [JobStatus::Scheduled, JobStatus::Running],
    )
    .await
    .map_err(Error::Database)
}

/// Given a worker node ID, return all the active jobs
///
/// A job is considered active if it's in one of the following non-terminal states:
/// - [`JobStatus::Scheduled`]
/// - [`JobStatus::Running`]
/// - [`JobStatus::StopRequested`]
///
/// When connection issues cause the job notification channel to miss notifications, a job reconciliation routine
/// ensures each worker's job set remains synchronized with the Metadata DB. This method fetches all jobs that a
/// worker should be tracking, enabling the worker to reconcile its state when notifications are lost.
#[tracing::instrument(skip(exe), err)]
pub async fn get_active<'c, E>(
    exe: E,
    node_id: impl Into<WorkerNodeId<'_>> + std::fmt::Debug,
) -> Result<Vec<Job>, Error>
where
    E: Executor<'c>,
{
    sql::get_by_node_id_and_statuses(
        exe,
        node_id.into(),
        [
            JobStatus::Scheduled,
            JobStatus::Running,
            JobStatus::StopRequested,
        ],
    )
    .await
    .map_err(Error::Database)
}

/// Returns the job with the given ID
#[tracing::instrument(skip(exe), err)]
pub async fn get_by_id<'c, E>(
    exe: E,
    id: impl Into<JobId> + std::fmt::Debug,
) -> Result<Option<Job>, Error>
where
    E: Executor<'c>,
{
    sql::get_by_id(exe, id.into())
        .await
        .map_err(Error::Database)
}

/// Get jobs for a given dataset
///
/// Returns all jobs that write to locations belonging to the specified dataset.
/// Jobs are deduplicated as a single job may write to multiple tables within the same dataset.
/// If `version` is `None`, all versions of the dataset are included.
#[tracing::instrument(skip(exe), err)]
pub async fn get_by_dataset<'c, E>(
    exe: E,
    manifest_hash: ManifestHash<'_>,
) -> Result<Vec<Job>, Error>
where
    E: Executor<'c>,
{
    sql::get_jobs_by_dataset(exe, manifest_hash)
        .await
        .map_err(Error::Database)
}

/// Conditionally marks a job as `RUNNING` only if it's currently `SCHEDULED`
///
/// This provides idempotent behavior - if the job is already running, completed, or failed,
/// the appropriate error will be returned indicating the state conflict.
#[tracing::instrument(skip(exe), err)]
pub async fn mark_running<'c, E>(
    exe: E,
    id: impl Into<JobId> + std::fmt::Debug,
) -> Result<(), Error>
where
    E: Executor<'c>,
{
    sql::update_status_if_any_state(exe, id.into(), &[JobStatus::Scheduled], JobStatus::Running)
        .await
        .map_err(Error::JobStatusUpdate)
}

/// Conditionally marks a job as `STOPPING` only if it's currently `STOP_REQUESTED`
///
/// This is typically used by workers to acknowledge a stop request.
#[tracing::instrument(skip(exe), err)]
pub async fn mark_stopping<'c, E>(
    exe: E,
    id: impl Into<JobId> + std::fmt::Debug,
) -> Result<(), Error>
where
    E: Executor<'c>,
{
    sql::update_status_if_any_state(
        exe,
        id.into(),
        &[JobStatus::StopRequested],
        JobStatus::Stopping,
    )
    .await
    .map_err(Error::JobStatusUpdate)
}

/// Conditionally marks a job as `STOPPED` only if it's currently `STOPPING`
///
/// This provides proper state transition from stopping to stopped.
#[tracing::instrument(skip(exe), err)]
pub async fn mark_stopped<'c, E>(
    exe: E,
    id: impl Into<JobId> + std::fmt::Debug,
) -> Result<(), Error>
where
    E: Executor<'c>,
{
    sql::update_status_if_any_state(exe, id.into(), &[JobStatus::Stopping], JobStatus::Stopped)
        .await
        .map_err(Error::JobStatusUpdate)
}

/// Conditionally marks a job as `COMPLETED` only if it's currently `RUNNING`
///
/// This ensures jobs can only be completed from a running state.
#[tracing::instrument(skip(exe), err)]
pub async fn mark_completed<'c, E>(
    exe: E,
    id: impl Into<JobId> + std::fmt::Debug,
) -> Result<(), Error>
where
    E: Executor<'c>,
{
    sql::update_status_if_any_state(exe, id.into(), &[JobStatus::Running], JobStatus::Completed)
        .await
        .map_err(Error::JobStatusUpdate)
}

/// Conditionally marks a job as `FAILED` from either `RUNNING` or `SCHEDULED` states
///
/// Jobs can fail from either scheduled (startup failure) or running (runtime failure) states.
#[tracing::instrument(skip(exe), err)]
pub async fn mark_failed<'c, E>(exe: E, id: impl Into<JobId> + std::fmt::Debug) -> Result<(), Error>
where
    E: Executor<'c>,
{
    sql::update_status_if_any_state(
        exe,
        id.into(),
        &[JobStatus::Scheduled, JobStatus::Running],
        JobStatus::Failed,
    )
    .await
    .map_err(Error::JobStatusUpdate)
}

/// Delete a job by ID if it's in a terminal state
///
/// This function will only delete the job if it exists and is in a terminal state
/// (Completed, Stopped, or Failed). Returns true if a job was deleted, false otherwise.
#[tracing::instrument(skip(exe), err)]
pub async fn delete_if_terminal<'c, E>(
    exe: E,
    id: impl Into<JobId> + std::fmt::Debug,
) -> Result<bool, Error>
where
    E: Executor<'c>,
{
    sql::delete_by_id_and_statuses(exe, id.into(), JobStatus::terminal_statuses())
        .await
        .map_err(Error::Database)
}

/// Delete all jobs that match the specified status or statuses
///
/// This function deletes all jobs that are in one of the specified statuses.
/// Returns the number of jobs that were deleted.
#[tracing::instrument(skip(exe), err)]
pub async fn delete_all_by_status<'c, E, const N: usize>(
    exe: E,
    statuses: [JobStatus; N],
) -> Result<usize, Error>
where
    E: Executor<'c>,
{
    sql::delete_by_status(exe, statuses)
        .await
        .map_err(Error::Database)
}

/// Get failed jobs that are ready for retry
///
/// Returns failed jobs where:
/// - retry_count < max_retries
/// - enough time has passed since last failure based on exponential backoff
///
/// The backoff is calculated as 2^retry_count seconds, capped at 60 seconds.
#[tracing::instrument(skip(exe), err)]
pub async fn get_failed_jobs_ready_for_retry<'c, E>(
    exe: E,
    max_retries: i32,
) -> Result<Vec<Job>, Error>
where
    E: Executor<'c>,
{
    sql::get_failed_jobs_ready_for_retry(exe, max_retries)
        .await
        .map_err(Error::Database)
}

/// Reschedule a failed job for retry
///
/// This function:
/// 1. Increments the retry_count
/// 2. Sets status to SCHEDULED
/// 3. Assigns the job to the specified worker node
/// 4. Updates the updated_at timestamp
///
/// **Note:** This function does not send notifications. The caller is responsible for
/// calling `send_job_notification` after successful rescheduling if worker notification
/// is required.
#[tracing::instrument(skip(exe), err)]
pub async fn reschedule_for_retry<'c, E>(
    exe: E,
    job_id: impl Into<JobId> + std::fmt::Debug,
    new_node_id: impl Into<WorkerNodeId<'_>> + std::fmt::Debug,
) -> Result<(), Error>
where
    E: Executor<'c>,
{
    sql::reschedule_for_retry(exe, job_id.into(), new_node_id.into())
        .await
        .map_err(Error::Database)
}

/// Error type for conditional job status updates
#[derive(Debug, thiserror::Error)]
pub enum JobStatusUpdateError {
    #[error("Job not found")]
    NotFound,

    #[error("Job state conflict: expected one of {expected:?}, but found {actual}")]
    StateConflict {
        expected: Vec<JobStatus>,
        actual: JobStatus,
    },

    #[error("Database error: {0}")]
    Database(#[source] sqlx::Error),
}

/// Represents a job with its metadata and associated node.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Job {
    /// Unique identifier for the job
    pub id: JobId,

    /// ID of the worker node this job is scheduled for
    pub node_id: WorkerNodeIdOwned,

    /// Current status of the job
    pub status: JobStatus,

    /// Job descriptor
    #[sqlx(rename = "descriptor")]
    pub desc: JsonValue,

    /// Job creation timestamp
    pub created_at: DateTime<Utc>,

    /// Job last update timestamp
    pub updated_at: DateTime<Utc>,

    /// Number of times this job has been retried
    ///
    /// Starts at 0 for the initial attempt. Incremented by the scheduler
    /// when rescheduling a failed job for retry.
    pub retry_count: i32,
}

/// In-tree integration tests
#[cfg(test)]
mod tests {
    mod it_jobs;
}

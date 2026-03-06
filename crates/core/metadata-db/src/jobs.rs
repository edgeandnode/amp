//! Job management module for metadata database
//!
//! This module provides functionality for managing distributed job queues with state tracking
//! and coordination between multiple worker nodes.

use sqlx::types::chrono::{DateTime, Utc};

mod job_descriptor;
mod job_id;
pub(crate) mod sql;

pub use self::{
    job_descriptor::{JobDescriptorRaw, JobDescriptorRawOwned},
    job_id::JobId,
    sql::JobWithRetryInfo,
};
pub use crate::job_status::JobStatus;
use crate::{
    db::Executor,
    error::Error,
    job_events, job_status,
    manifests::ManifestHash,
    workers::{WorkerNodeId, WorkerNodeIdOwned},
};

/// Register a job in the queue
///
/// Creates the job record with its descriptor. The caller is responsible for
/// separately registering the status via [`job_status::register`] and the event
/// via [`job_events::register`].
///
/// **Note:** This function does not send notifications. The caller is responsible for
/// calling `send_job_notification` after successful job registration if worker notification
/// is required.
#[tracing::instrument(skip(exe), err)]
pub async fn register<'c, E>(
    exe: E,
    node_id: impl Into<WorkerNodeId<'_>> + std::fmt::Debug,
    job_desc: impl Into<JobDescriptorRaw<'_>> + std::fmt::Debug,
) -> Result<JobId, Error>
where
    E: Executor<'c>,
{
    let job_desc = job_desc.into();
    sql::insert(exe, &node_id.into(), &job_desc)
        .await
        .map_err(Error::Database)
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
    dataset_namespace: impl Into<crate::datasets::DatasetNamespace<'_>> + std::fmt::Debug,
    dataset_name: impl Into<crate::datasets::DatasetName<'_>> + std::fmt::Debug,
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
    manifest_hash: impl Into<ManifestHash<'_>> + std::fmt::Debug,
) -> Result<Vec<Job>, Error>
where
    E: Executor<'c>,
{
    sql::get_jobs_by_dataset(exe, manifest_hash.into())
        .await
        .map_err(Error::Database)
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

/// Get failed (recoverable) jobs that are ready for retry
///
/// Returns failed jobs where enough time has passed since last failure based on
/// exponential backoff. Jobs retry indefinitely with exponentially increasing delays.
///
/// The backoff is calculated as 2^next_retry_index seconds (unbounded exponential growth).
/// The next_retry_index is derived from SCHEDULED events in the job_events table.
#[tracing::instrument(skip(exe), err)]
pub async fn get_failed_jobs_ready_for_retry<'c, E>(exe: E) -> Result<Vec<JobWithRetryInfo>, Error>
where
    E: Executor<'c>,
{
    sql::get_failed_jobs_ready_for_retry(exe)
        .await
        .map_err(Error::Database)
}

/// Reschedule a failed job for retry with atomically tracked attempt
///
/// This function performs three operations using the provided transaction:
///  1. Updates job status to SCHEDULED in `jobs_status` and assigns to the given worker node
///  2. Inserts a SCHEDULED event into `job_events`
///
/// If any operation fails, the error is returned and no further operations are attempted.
/// The caller is responsible for transaction management (commit/rollback).
///
/// **Note:** This function accepts `&mut Transaction` instead of a generic `Executor<'c>`
/// because it performs multiple sequential database operations that require re-borrowable
/// access.
///
/// This function does not send notifications. The caller is responsible
/// for sending notifications after successful rescheduling.
#[tracing::instrument(skip(tx), err)]
pub async fn reschedule(
    tx: &mut crate::Transaction<'_>,
    job_id: impl Into<JobId> + std::fmt::Debug,
    new_node_id: impl Into<WorkerNodeId<'_>> + std::fmt::Debug,
    retry_index: i32,
) -> Result<(), Error> {
    let job_id = job_id.into();
    let new_node_id = new_node_id.into();

    // Update job status to SCHEDULED in jobs_status and assign to worker
    job_status::reschedule(&mut *tx, job_id, &new_node_id).await?;

    // Insert job event record
    job_events::register(&mut *tx, job_id, &new_node_id, JobStatus::Scheduled, None).await?;

    Ok(())
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
    pub desc: JobDescriptorRawOwned,

    /// Job creation timestamp
    pub created_at: DateTime<Utc>,

    /// Job last update timestamp
    pub updated_at: DateTime<Utc>,
}

/// In-tree integration tests
#[cfg(test)]
mod tests {
    mod it_jobs;
}

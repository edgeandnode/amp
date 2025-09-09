//! Job management module for metadata database
//!
//! This module provides functionality for managing distributed job queues with state tracking
//! and coordination between multiple worker nodes.

use sqlx::types::{
    JsonValue,
    chrono::{DateTime, Utc},
};

pub mod events;
mod job_id;
mod job_status;
mod pagination;

pub use self::{
    job_id::{JobId, JobIdFromStrError, JobIdI64ConvError, JobIdU64Error},
    job_status::JobStatus,
    pagination::{list_first_page, list_next_page},
};
use crate::workers::WorkerNodeId;

/// Insert a new job into the queue
///
/// The job will be assigned to the given worker node with the specified status.
pub async fn insert<'c, E>(
    exe: E,
    node_id: &WorkerNodeId,
    descriptor: &str,
    status: JobStatus,
) -> Result<JobId, sqlx::Error>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let query = indoc::indoc! {r#"
        INSERT INTO jobs (node_id, descriptor, status, created_at, updated_at)
        VALUES ($1, $2::jsonb, $3, (timezone('UTC', now())), (timezone('UTC', now())))
        RETURNING id
    "#};
    let res = sqlx::query_scalar(query)
        .bind(node_id)
        .bind(descriptor)
        .bind(status)
        .fetch_one(exe)
        .await?;
    Ok(res)
}

/// Insert a new job into the queue with the default status
///
/// The job will be assigned to the given worker node with the default status (Scheduled).
#[inline]
pub async fn insert_with_default_status<'c, E>(
    exe: E,
    node_id: &WorkerNodeId,
    descriptor: &str,
) -> Result<JobId, sqlx::Error>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    insert(exe, node_id, descriptor, JobStatus::default()).await
}

/// Update the status of a job with multiple possible expected original states
///
/// This function will only update the job status if the job exists and currently has
/// one of the expected original statuses. If the job doesn't exist, returns `UpdateJobStatusError::NotFound`.
/// If the job exists but has a different status than any of the expected ones, returns `UpdateJobStatusError::StateConflict`.
pub async fn update_status_if_any_state<'c, E>(
    exe: E,
    id: &JobId,
    expected_statuses: &[JobStatus],
    new_status: JobStatus,
) -> Result<(), JobStatusUpdateError>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    /// Internal structure to hold the result of the update operation
    #[derive(Debug, sqlx::FromRow)]
    struct UpdateResult {
        updated_id: Option<JobId>,
        original_status: Option<JobStatus>,
    }

    let query = indoc::indoc! {r#"
        WITH target_job AS (
            SELECT id, status
            FROM jobs
            WHERE id = $1
        ),
        target_job_update AS (
            UPDATE jobs
            SET status = $3, updated_at = timezone('UTC', now())
            WHERE id = $1 AND status = ANY($2)
            RETURNING id
        )
        SELECT
            target_job_update.id AS updated_id,
            target_job.status AS original_status
        FROM target_job
        LEFT JOIN target_job_update ON target_job.id = target_job_update.id
    "#};

    let result: Option<UpdateResult> = sqlx::query_as(query)
        .bind(id)
        .bind(expected_statuses)
        .bind(new_status)
        .fetch_optional(exe)
        .await
        .map_err(JobStatusUpdateError::Database)?;

    match result {
        Some(UpdateResult {
            updated_id: Some(_),
            ..
        }) => Ok(()),
        Some(UpdateResult {
            updated_id: None,
            original_status: Some(status),
        }) => Err(JobStatusUpdateError::StateConflict {
            expected: expected_statuses.to_vec(),
            actual: status,
        }),
        _ => Err(JobStatusUpdateError::NotFound),
    }
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
    Database(#[from] sqlx::Error),
}

/// Get a job by its ID
pub async fn get_by_id<'c, E>(exe: E, id: &JobId) -> Result<Option<Job>, sqlx::Error>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT id, node_id, status, descriptor
        FROM jobs
        WHERE id = $1
    "#};
    let res = sqlx::query_as(query).bind(id).fetch_optional(exe).await?;
    Ok(res)
}

/// Get a job by ID with full details including timestamps
pub async fn get_by_id_with_details<'c, E>(
    exe: E,
    id: &JobId,
) -> Result<Option<JobWithDetails>, sqlx::Error>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT
            id,
            node_id,
            status,
            descriptor,
            created_at,
            updated_at
        FROM jobs
        WHERE id = $1
    "#};
    let res = sqlx::query_as(query).bind(id).fetch_optional(exe).await?;
    Ok(res)
}

/// Get jobs for a given worker node with any of the specified statuses
pub async fn get_by_node_id_and_statuses<'c, E, const N: usize>(
    exe: E,
    node_id: &WorkerNodeId,
    statuses: [JobStatus; N],
) -> Result<Vec<Job>, sqlx::Error>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT
            id,
            node_id,
            status,
            descriptor
        FROM jobs
        WHERE node_id = $1 AND status = ANY($2)
        ORDER BY id ASC
    "#};
    let res = sqlx::query_as(query)
        .bind(node_id)
        .bind(statuses)
        .fetch_all(exe)
        .await?;
    Ok(res)
}

/// Delete a job by ID if it matches any of the specified statuses
///
/// This function will only delete the job if it exists and is in one of the specified statuses.
/// Returns true if a job was deleted, false otherwise.
pub async fn delete_by_id_and_statuses<'c, E, const N: usize>(
    exe: E,
    id: &JobId,
    statuses: [JobStatus; N],
) -> Result<bool, sqlx::Error>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let query = indoc::indoc! {r#"
        DELETE FROM jobs 
        WHERE id = $1 AND status = ANY($2)
    "#};

    let result = sqlx::query(query)
        .bind(id)
        .bind(statuses)
        .execute(exe)
        .await?;

    Ok(result.rows_affected() == 1)
}

/// Delete all jobs that match the specified status
///
/// This function deletes all jobs that are in the specified status.
/// Returns the number of jobs that were deleted.
pub async fn delete_by_status<'c, E>(exe: E, status: JobStatus) -> Result<usize, sqlx::Error>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let query = indoc::indoc! {r#"
        DELETE FROM jobs 
        WHERE status = $1
    "#};

    let result = sqlx::query(query).bind(status).execute(exe).await?;

    Ok(result.rows_affected() as usize)
}

/// Delete all jobs that match any of the specified statuses
///
/// This function deletes all jobs that are in one of the specified statuses.
/// Returns the number of jobs that were deleted.
pub async fn delete_by_statuses<'c, E, const N: usize>(
    exe: E,
    statuses: [JobStatus; N],
) -> Result<usize, sqlx::Error>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let query = indoc::indoc! {r#"
        DELETE FROM jobs 
        WHERE status = ANY($1)
    "#};

    let result = sqlx::query(query).bind(statuses).execute(exe).await?;

    Ok(result.rows_affected() as usize)
}

/// Represents a job with its metadata and associated node.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Job {
    /// Unique identifier for the job
    pub id: JobId,

    /// ID of the worker node this job is scheduled for
    pub node_id: WorkerNodeId,

    /// Current status of the job
    pub status: JobStatus,

    /// Job description
    #[sqlx(rename = "descriptor")]
    pub desc: JsonValue,
}

/// Represents a job with its metadata and associated node.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct JobWithDetails {
    /// Unique identifier for the job
    pub id: JobId,

    /// ID of the worker node this job is scheduled for
    pub node_id: WorkerNodeId,

    /// Current status of the job
    pub status: JobStatus,

    /// Job descriptor
    #[sqlx(rename = "descriptor")]
    pub desc: JsonValue,

    /// Job creation timestamp
    pub created_at: DateTime<Utc>,

    /// Job last update timestamp
    pub updated_at: DateTime<Utc>,
}

/// In-tree integration tests
#[cfg(test)]
mod tests {
    mod it_events;
    mod it_jobs;
}

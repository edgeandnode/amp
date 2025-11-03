//! Internal SQL operations for job management

use sqlx::{Executor, Postgres};

use super::{Job, JobId, JobStatus, JobStatusUpdateError};
use crate::{
    datasets::{DatasetName, DatasetVersion},
    workers::WorkerNodeId,
};

/// Insert a new job into the queue
///
/// The job will be assigned to the given worker node with the specified status.
pub async fn insert<'c, E>(
    exe: E,
    node_id: WorkerNodeId<'_>,
    descriptor: &str,
    status: JobStatus,
) -> Result<JobId, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        INSERT INTO jobs (node_id, descriptor, status, created_at, updated_at)
        VALUES ($1, $2::jsonb, $3, (timezone('UTC', now())), (timezone('UTC', now())))
        RETURNING id
    "#};
    let res = sqlx::query_scalar(query)
        .bind(&node_id)
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
    node_id: WorkerNodeId<'_>,
    descriptor: &str,
) -> Result<JobId, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
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
    id: JobId,
    expected_statuses: &[JobStatus],
    new_status: JobStatus,
) -> Result<(), JobStatusUpdateError>
where
    E: Executor<'c, Database = Postgres>,
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

/// Get a job by its ID
pub async fn get_by_id<'c, E>(exe: E, id: JobId) -> Result<Option<Job>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT id, node_id, status, descriptor, created_at, updated_at
        FROM jobs
        WHERE id = $1
    "#};
    let res = sqlx::query_as(query).bind(id).fetch_optional(exe).await?;
    Ok(res)
}

/// Get jobs for a given worker node with any of the specified statuses
pub async fn get_by_node_id_and_statuses<'c, E, const N: usize>(
    exe: E,
    node_id: WorkerNodeId<'_>,
    statuses: [JobStatus; N],
) -> Result<Vec<Job>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
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

/// Get jobs for a given dataset
///
/// Returns all jobs that write to locations belonging to the specified dataset.
/// Jobs are deduplicated as a single job may write to multiple tables within the same dataset.
/// If `version` is `None`, all versions of the dataset are included.
pub async fn get_jobs_by_dataset<'c, E>(
    exe: E,
    dataset: DatasetName<'_>,
    version: Option<DatasetVersion<'_>>,
) -> Result<Vec<Job>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT DISTINCT
            j.id,
            j.node_id,
            j.status,
            j.descriptor,
            j.created_at,
            j.updated_at
        FROM jobs j
        INNER JOIN locations l ON j.id = l.writer
        WHERE l.dataset = $1 AND l.dataset_version = $2
        ORDER BY j.id ASC
    "#};
    let res = sqlx::query_as(query)
        .bind(dataset)
        .bind(version.map(|v| v.to_string()).unwrap_or_default())
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
    id: JobId,
    statuses: [JobStatus; N],
) -> Result<bool, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
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

/// Delete all jobs that match any of the specified statuses
///
/// This function deletes all jobs that are in one of the specified statuses.
/// Returns the number of jobs that were deleted.
pub async fn delete_by_status<'c, E, const N: usize>(
    exe: E,
    statuses: [JobStatus; N],
) -> Result<usize, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        DELETE FROM jobs
        WHERE status = ANY($1)
    "#};

    let result = sqlx::query(query).bind(statuses).execute(exe).await?;

    Ok(result.rows_affected() as usize)
}

/// List the first page of jobs
///
/// Returns a paginated list of jobs ordered by ID in descending order (newest first).
/// This function is used to fetch the initial page when no cursor is available.
pub async fn list_first_page<'c, E>(exe: E, limit: i64) -> Result<Vec<Job>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
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
        ORDER BY id DESC
        LIMIT $1
    "#};

    let res = sqlx::query_as(query).bind(limit).fetch_all(exe).await?;
    Ok(res)
}

/// List subsequent pages of jobs using cursor-based pagination
///
/// Returns a paginated list of jobs with IDs less than the provided cursor,
/// ordered by ID in descending order (newest first). This implements cursor-based
/// pagination for efficient traversal of large job lists.
pub async fn list_next_page<'c, E>(
    exe: E,
    limit: i64,
    last_job_id: JobId,
) -> Result<Vec<Job>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
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
        WHERE id < $2
        ORDER BY id DESC
        LIMIT $1
    "#};

    let res = sqlx::query_as(query)
        .bind(limit)
        .bind(last_job_id)
        .fetch_all(exe)
        .await?;
    Ok(res)
}

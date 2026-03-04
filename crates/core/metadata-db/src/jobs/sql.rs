//! Internal SQL operations for job management

use sqlx::{Executor, Postgres};

use super::{Job, job_descriptor::JobDescriptorRaw, job_id::JobId};
use crate::{
    datasets::{DatasetName, DatasetNamespace},
    job_status::JobStatus,
    manifests::ManifestHash,
    workers::WorkerNodeId,
};

/// Job with calculated retry information
///
/// This struct extends the base Job with retry_index information calculated
/// from SCHEDULED events in job_events. Used by retry scheduling logic.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct JobWithRetryInfo {
    /// Base job information
    #[sqlx(flatten)]
    pub job: Job,

    /// Next retry index to use when scheduling this job
    ///
    /// Calculated as COUNT of SCHEDULED events from job_events table.
    /// Will be 0 if no events exist yet.
    pub next_retry_index: i32,
}

/// Insert a new job into the queue
///
/// Creates the job record with its descriptor and timestamp. The job's status
/// and worker assignment are stored in the `jobs_status` projection table via
/// a separate insert.
pub async fn insert<'c, E>(
    exe: E,
    node_id: &WorkerNodeId<'_>,
    descriptor: &JobDescriptorRaw<'_>,
) -> Result<JobId, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        INSERT INTO jobs (node_id, descriptor, created_at)
        VALUES ($1, $2, (timezone('UTC', now())))
        RETURNING id
    "#};
    let res = sqlx::query_scalar(query)
        .bind(node_id)
        .bind(descriptor)
        .fetch_one(exe)
        .await?;
    Ok(res)
}

/// Get a job by its ID
pub async fn get_by_id<'c, E>(exe: E, id: JobId) -> Result<Option<Job>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT j.id, js.node_id, js.status, j.descriptor, j.created_at, js.updated_at
        FROM jobs j
        INNER JOIN jobs_status js ON j.id = js.job_id
        WHERE j.id = $1
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
            j.id,
            js.node_id,
            js.status,
            j.descriptor,
            j.created_at,
            js.updated_at
        FROM jobs j
        INNER JOIN jobs_status js ON j.id = js.job_id
        WHERE js.node_id = $1 AND js.status = ANY($2)
        ORDER BY j.id ASC
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
    manifest_hash: ManifestHash<'_>,
) -> Result<Vec<Job>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT DISTINCT
            j.id,
            js.node_id,
            js.status,
            j.descriptor,
            j.created_at,
            js.updated_at
        FROM jobs j
        INNER JOIN jobs_status js ON j.id = js.job_id
        INNER JOIN physical_table_revisions ptr ON j.id = ptr.writer
        INNER JOIN physical_tables pt ON pt.active_revision_id = ptr.id
        WHERE pt.manifest_hash = $1
        ORDER BY j.id ASC
    "#};
    let res = sqlx::query_as(query)
        .bind(manifest_hash)
        .fetch_all(exe)
        .await?;
    Ok(res)
}

/// List jobs by dataset reference (namespace, name, and manifest hash)
///
/// Queries the job descriptor JSONB field directly, avoiding joins to physical_tables.
pub async fn list_by_dataset_reference<'c, E>(
    exe: E,
    dataset_namespace: DatasetNamespace<'_>,
    dataset_name: DatasetName<'_>,
    manifest_hash: ManifestHash<'_>,
) -> Result<Vec<Job>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT
            j.id,
            js.node_id,
            js.status,
            j.descriptor,
            j.created_at,
            js.updated_at
        FROM jobs j
        INNER JOIN jobs_status js ON j.id = js.job_id
        WHERE j.descriptor->>'dataset_namespace' = $1
          AND j.descriptor->>'dataset_name' = $2
          AND j.descriptor->>'manifest_hash' = $3
        ORDER BY j.id ASC
    "#};
    let res = sqlx::query_as(query)
        .bind(&dataset_namespace)
        .bind(&dataset_name)
        .bind(&manifest_hash)
        .fetch_all(exe)
        .await?;
    Ok(res)
}

/// Delete a job by ID if it matches any of the specified statuses
///
/// This function will only delete the job if it exists and is in one of the specified statuses.
/// The `jobs_status` row is deleted automatically via ON DELETE CASCADE.
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
        WHERE id = $1 AND id IN (
            SELECT job_id FROM jobs_status WHERE status = ANY($2)
        )
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
/// The `jobs_status` rows are deleted automatically via ON DELETE CASCADE.
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
        WHERE id IN (
            SELECT job_id FROM jobs_status WHERE status = ANY($1)
        )
    "#};

    let result = sqlx::query(query).bind(statuses).execute(exe).await?;

    Ok(result.rows_affected() as usize)
}

/// List the first page of jobs, optionally filtered by status
///
/// Returns a paginated list of jobs ordered by ID in descending order (newest first).
/// This function is used to fetch the initial page when no cursor is available.
/// If `statuses` is provided, only jobs matching those statuses are returned.
pub async fn list_first_page<'c, E>(
    exe: E,
    limit: i64,
    statuses: Option<&[JobStatus]>,
) -> Result<Vec<Job>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    match statuses {
        None => {
            let query = indoc::indoc! {r#"
                SELECT
                    j.id,
                    js.node_id,
                    js.status,
                    j.descriptor,
                    j.created_at,
                    js.updated_at
                FROM jobs j
                INNER JOIN jobs_status js ON j.id = js.job_id
                ORDER BY j.id DESC
                LIMIT $1
            "#};

            sqlx::query_as(query).bind(limit).fetch_all(exe).await
        }
        Some(statuses) => {
            let query = indoc::indoc! {r#"
                SELECT
                    j.id,
                    js.node_id,
                    js.status,
                    j.descriptor,
                    j.created_at,
                    js.updated_at
                FROM jobs j
                INNER JOIN jobs_status js ON j.id = js.job_id
                WHERE js.status = ANY($2)
                ORDER BY j.id DESC
                LIMIT $1
            "#};

            sqlx::query_as(query)
                .bind(limit)
                .bind(statuses)
                .fetch_all(exe)
                .await
        }
    }
}

/// List subsequent pages of jobs using cursor-based pagination, optionally filtered by status
///
/// Returns a paginated list of jobs with IDs less than the provided cursor,
/// ordered by ID in descending order (newest first). This implements cursor-based
/// pagination for efficient traversal of large job lists.
/// If `statuses` is provided, only jobs matching those statuses are returned.
pub async fn list_next_page<'c, E>(
    exe: E,
    limit: i64,
    last_job_id: JobId,
    statuses: Option<&[JobStatus]>,
) -> Result<Vec<Job>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    match statuses {
        None => {
            let query = indoc::indoc! {r#"
                SELECT
                    j.id,
                    js.node_id,
                    js.status,
                    j.descriptor,
                    j.created_at,
                    js.updated_at
                FROM jobs j
                INNER JOIN jobs_status js ON j.id = js.job_id
                WHERE j.id < $2
                ORDER BY j.id DESC
                LIMIT $1
            "#};

            sqlx::query_as(query)
                .bind(limit)
                .bind(last_job_id)
                .fetch_all(exe)
                .await
        }
        Some(statuses) => {
            let query = indoc::indoc! {r#"
                SELECT
                    j.id,
                    js.node_id,
                    js.status,
                    j.descriptor,
                    j.created_at,
                    js.updated_at
                FROM jobs j
                INNER JOIN jobs_status js ON j.id = js.job_id
                WHERE j.id < $2 AND js.status = ANY($3)
                ORDER BY j.id DESC
                LIMIT $1
            "#};

            sqlx::query_as(query)
                .bind(limit)
                .bind(last_job_id)
                .bind(statuses)
                .fetch_all(exe)
                .await
        }
    }
}

/// Get failed jobs that are ready for retry
///
/// Returns failed jobs where enough time has passed since last failure based on
/// exponential backoff. Jobs retry indefinitely with exponentially increasing delays.
///
/// The backoff is calculated as 2^(COUNT(SCHEDULED events)) seconds, where the
/// retry count is derived from SCHEDULED events in job_events. Jobs without events
/// are treated as having retry count 0 (initial attempt).
pub async fn get_failed_jobs_ready_for_retry<'c, E>(
    exe: E,
) -> Result<Vec<JobWithRetryInfo>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT
            j.id,
            js.node_id,
            js.status,
            j.descriptor,
            j.created_at,
            js.updated_at,
            COUNT(je.id)::int4 AS next_retry_index
        FROM jobs j
        INNER JOIN jobs_status js ON j.id = js.job_id
        LEFT JOIN job_events je ON j.id = je.job_id AND je.event_type = $1
        WHERE js.status = $2
        GROUP BY j.id, js.node_id, js.status, j.descriptor, j.created_at, js.updated_at
        HAVING js.updated_at + INTERVAL '1 second' * POW(2, COUNT(je.id))::bigint
            <= timezone('UTC', now())
        ORDER BY j.id ASC
    "#};

    sqlx::query_as(query)
        .bind(JobStatus::Scheduled)
        .bind(JobStatus::Error)
        .fetch_all(exe)
        .await
}

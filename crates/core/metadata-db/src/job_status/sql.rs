//! Internal SQL operations for the `jobs_status` projection table.

use sqlx::{Executor, Postgres};

use crate::{
    datasets::{DatasetName, DatasetNamespace},
    job_events::EventDetail,
    job_status::{JobStatus, JobStatusUpdateError},
    jobs::{Job, JobId},
    manifests::ManifestHash,
    workers::WorkerNodeId,
};

/// Idempotently insert a new job status projection
///
/// Uses `ON CONFLICT DO NOTHING` to match the idempotent behavior of `jobs::sql::insert`.
/// When a job is re-registered with the same idempotency key, the existing status row is
/// preserved unchanged.
pub async fn insert<'c, E>(
    exe: E,
    job_id: JobId,
    node_id: &WorkerNodeId<'_>,
    status: JobStatus,
    detail: Option<EventDetail<'_>>,
) -> Result<(), sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        INSERT INTO jobs_status (job_id, node_id, status, detail, updated_at)
        VALUES ($1, $2, $3, $4, (timezone('UTC', now())))
        ON CONFLICT (job_id) DO NOTHING
    "#};
    sqlx::query(query)
        .bind(job_id)
        .bind(node_id)
        .bind(status)
        .bind(detail)
        .execute(exe)
        .await?;
    Ok(())
}

/// Update the status of a job with multiple possible expected original states
///
/// This function will only update the job status if the job exists in `jobs_status`
/// and currently has one of the expected original statuses.
/// If the job doesn't exist, returns `JobStatusUpdateError::NotFound`.
/// If the job exists but has a different status, returns `JobStatusUpdateError::StateConflict`.
pub async fn update_status_if_any_state<'c, E>(
    exe: E,
    id: JobId,
    expected_statuses: &[JobStatus],
    new_status: JobStatus,
    detail: Option<&EventDetail<'_>>,
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
            SELECT job_id, status
            FROM jobs_status
            WHERE job_id = $1
        ),
        target_job_update AS (
            UPDATE jobs_status
            SET status = $3, detail = $4, updated_at = timezone('UTC', now())
            WHERE job_id = $1 AND status = ANY($2)
            RETURNING job_id
        )
        SELECT
            target_job_update.job_id AS updated_id,
            target_job.status AS original_status
        FROM target_job
        LEFT JOIN target_job_update ON target_job.job_id = target_job_update.job_id
    "#};

    let result: Option<UpdateResult> = sqlx::query_as(query)
        .bind(id)
        .bind(expected_statuses)
        .bind(new_status)
        .bind(detail)
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

/// Reschedule a failed job by updating its status to SCHEDULED in `jobs_status`
///
/// Updates the status and assigns the job to the specified worker node.
pub async fn reschedule<'c, E>(
    exe: E,
    job_id: JobId,
    new_node_id: &WorkerNodeId<'_>,
    detail: &EventDetail<'_>,
) -> Result<(), sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        UPDATE jobs_status
        SET
            status = $3,
            node_id = $2,
            detail = $4,
            updated_at = timezone('UTC', now())
        WHERE job_id = $1
    "#};

    sqlx::query(query)
        .bind(job_id)
        .bind(new_node_id)
        .bind(JobStatus::Scheduled)
        .bind(detail)
        .execute(exe)
        .await?;

    Ok(())
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
            je.job_id as id,
            js.node_id,
            js.status,
            je.detail as descriptor,
            je.created_at,
            js.updated_at
        FROM jobs_status js
        INNER JOIN job_events je ON js.job_id = je.job_id
        WHERE je.detail->>'dataset_namespace' = $1
          AND je.detail->>'dataset_name' = $2
          AND je.detail->>'manifest_hash' = $3
          AND je.event_type = $4
        ORDER BY je.id ASC
    "#};
    let res = sqlx::query_as(query)
        .bind(&dataset_namespace)
        .bind(&dataset_name)
        .bind(&manifest_hash)
        .bind(JobStatus::Scheduled)
        .fetch_all(exe)
        .await?;
    Ok(res)
}

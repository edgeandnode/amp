//! Internal SQL operations for the `job_events` table.

use sqlx::{Executor, Postgres};

use super::{EventDetail, JobEvent};
use crate::{
    jobs::{JobId, JobStatus},
    workers::WorkerNodeId,
};

/// Insert a new event into the job_events log
pub async fn insert<'c, E>(
    exe: E,
    job_id: JobId,
    node_id: &WorkerNodeId<'_>,
    status: JobStatus,
    detail: Option<&EventDetail<'_>>,
) -> Result<(), sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        INSERT INTO job_events (job_id, node_id, event_type, detail)
        VALUES ($1, $2, $3, $4)
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

/// Count the number of scheduling attempts for a job
pub async fn get_attempt_count<'c, E>(exe: E, job_id: JobId) -> Result<i32, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT COUNT(*)::int4
        FROM job_events
        WHERE job_id = $1 AND event_type = $2
    "#};

    sqlx::query_scalar(query)
        .bind(job_id)
        .bind(JobStatus::Scheduled)
        .fetch_one(exe)
        .await
}

/// Get all scheduling attempts for a job
///
/// Each SCHEDULED event in the log represents one attempt.
/// Returns attempts with a derived retry_index (0 = initial, 1+ = retries).
pub async fn get_attempts_for_job<'c, E>(
    exe: E,
    job_id: JobId,
) -> Result<Vec<JobEvent>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT job_id,
               (ROW_NUMBER() OVER (ORDER BY id) - 1)::int4 AS retry_index,
               created_at
        FROM job_events
        WHERE job_id = $1 AND event_type = $2
        ORDER BY id ASC
    "#};

    sqlx::query_as(query)
        .bind(job_id)
        .bind(JobStatus::Scheduled)
        .fetch_all(exe)
        .await
}

//! Internal SQL operations for job attempts

use sqlx::{
    Executor, Postgres,
    types::chrono::{DateTime, Utc},
};

use crate::jobs::JobId;

/// Represents a job attempt with its metadata
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct JobAttempt {
    pub job_id: JobId,
    pub retry_index: i32,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

/// Insert a new job attempt record
pub async fn insert_attempt<'c, E>(
    exe: E,
    job_id: JobId,
    retry_index: i32,
) -> Result<(), sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        INSERT INTO job_attempts (job_id, retry_index)
        VALUES ($1, $2)
    "#};

    sqlx::query(query)
        .bind(job_id)
        .bind(retry_index)
        .execute(exe)
        .await?;

    Ok(())
}

/// Mark an attempt as completed
pub async fn mark_attempt_completed<'c, E>(
    exe: E,
    job_id: JobId,
    retry_index: i32,
) -> Result<(), sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        UPDATE job_attempts
        SET completed_at = timezone('UTC', now())
        WHERE job_id = $1 AND retry_index = $2
    "#};

    sqlx::query(query)
        .bind(job_id)
        .bind(retry_index)
        .execute(exe)
        .await?;

    Ok(())
}

/// Get all attempts for a given job
pub async fn get_attempts_for_job<'c, E>(
    exe: E,
    job_id: JobId,
) -> Result<Vec<JobAttempt>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT job_id, retry_index, created_at, completed_at
        FROM job_attempts
        WHERE job_id = $1
        ORDER BY retry_index ASC
    "#};

    sqlx::query_as(query).bind(job_id).fetch_all(exe).await
}

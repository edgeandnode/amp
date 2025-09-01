//! Pagination functionality for jobs

use sqlx::Executor;

use super::{JobId, JobWithDetails};

/// List the first page of jobs
///
/// Returns a paginated list of jobs ordered by ID in descending order (newest first).
/// This function is used to fetch the initial page when no cursor is available.
pub async fn list_first_page<'c, E>(exe: E, limit: i64) -> Result<Vec<JobWithDetails>, sqlx::Error>
where
    E: Executor<'c, Database = sqlx::Postgres>,
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
) -> Result<Vec<JobWithDetails>, sqlx::Error>
where
    E: Executor<'c, Database = sqlx::Postgres>,
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

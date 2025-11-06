//! Pagination functions for physical table listing

use sqlx::{Executor, Postgres};

use super::{LocationId, PhysicalTable};

/// List the first page of physical tables
///
/// Returns a paginated list of physical tables ordered by ID in descending order (newest first).
/// This function is used to fetch the initial page when no cursor is available.
pub async fn list_first_page<'c, E>(exe: E, limit: i64) -> Result<Vec<PhysicalTable>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT
            id,
            manifest_hash,
            dataset_namespace,
            dataset_name,
            table_name,
            url,
            active,
            writer
        FROM physical_tables
        ORDER BY id DESC
        LIMIT $1
    "#};

    let res = sqlx::query_as(query).bind(limit).fetch_all(exe).await?;
    Ok(res)
}

/// List subsequent pages of physical tables using cursor-based pagination
///
/// Returns a paginated list of physical tables with IDs less than the provided cursor,
/// ordered by ID in descending order (newest first). This implements cursor-based
/// pagination for efficient traversal of large physical table lists.
pub async fn list_next_page<'c, E>(
    exe: E,
    limit: i64,
    last_id: LocationId,
) -> Result<Vec<PhysicalTable>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT
            id,
            manifest_hash,
            dataset_namespace,
            dataset_name,
            table_name,
            url,
            active,
            writer
        FROM physical_tables
        WHERE id < $2
        ORDER BY id DESC
        LIMIT $1
    "#};

    let res = sqlx::query_as(query)
        .bind(limit)
        .bind(last_id)
        .fetch_all(exe)
        .await?;
    Ok(res)
}

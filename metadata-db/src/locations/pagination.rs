//! Pagination functions for location listing

use sqlx::{Executor, Postgres};

use super::{Location, LocationId};

/// List the first page of locations
///
/// Returns a paginated list of locations ordered by ID in descending order (newest first).
/// This function is used to fetch the initial page when no cursor is available.
pub async fn list_locations_first_page<'c, E>(
    exe: E,
    limit: i64,
) -> Result<Vec<Location>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT
            id,
            dataset,
            dataset_version,
            tbl,
            url,
            active
        FROM locations
        ORDER BY id DESC
        LIMIT $1
    "#};

    let res = sqlx::query_as(query).bind(limit).fetch_all(exe).await?;
    Ok(res)
}

/// List subsequent pages of locations using cursor-based pagination
///
/// Returns a paginated list of locations with IDs less than the provided cursor,
/// ordered by ID in descending order (newest first). This implements cursor-based
/// pagination for efficient traversal of large location lists.
pub async fn list_locations_next_page<'c, E>(
    exe: E,
    limit: i64,
    last_location_id: LocationId,
) -> Result<Vec<Location>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT 
            id, 
            dataset, 
            dataset_version, 
            tbl,
            url,
            active
        FROM locations
        WHERE id < $2
        ORDER BY id DESC
        LIMIT $1
    "#};

    let res = sqlx::query_as(query)
        .bind(limit)
        .bind(last_location_id)
        .fetch_all(exe)
        .await?;
    Ok(res)
}

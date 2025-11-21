//! Pagination functions for file metadata listing

use sqlx::{Executor, Postgres};

use super::{FileId, FileMetadata};
use crate::physical_table::LocationId;

/// List the first page of file metadata records for a specific location
///
/// Returns a paginated list of file metadata records for the given location, ordered by ID in descending order (newest first).
/// This function is used to fetch the initial page when no cursor is available.
pub async fn list_first_page<'c, E>(
    exe: E,
    location_id: LocationId,
    limit: i64,
) -> Result<Vec<FileMetadata>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT fm.id,
               fm.location_id,
               fm.file_name,
               l.url,
               fm.object_size,
               fm.object_e_tag,
               fm.object_version
        FROM file_metadata fm
        JOIN physical_tables l ON fm.location_id = l.id
        WHERE fm.location_id = $1
        ORDER BY fm.id DESC
        LIMIT $2
    "#};

    let res = sqlx::query_as(query)
        .bind(location_id)
        .bind(limit)
        .fetch_all(exe)
        .await?;
    Ok(res)
}

/// List subsequent pages of file metadata records using cursor-based pagination for a specific location
///
/// Returns a paginated list of file metadata records for the given location with IDs less than the provided cursor,
/// ordered by ID in descending order (newest first). This implements cursor-based
/// pagination for efficient traversal of large file metadata lists within a location.
pub async fn list_next_page<'c, E>(
    exe: E,
    location_id: LocationId,
    limit: i64,
    last_id: FileId,
) -> Result<Vec<FileMetadata>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT fm.id,
               fm.location_id,
               fm.file_name,
               l.url,
               fm.object_size,
               fm.object_e_tag,
               fm.object_version
        FROM file_metadata fm
        JOIN physical_tables l ON fm.location_id = l.id
        WHERE fm.location_id = $1 AND fm.id < $3
        ORDER BY fm.id DESC
        LIMIT $2
    "#};

    let res = sqlx::query_as(query)
        .bind(location_id)
        .bind(limit)
        .bind(last_id)
        .fetch_all(exe)
        .await?;
    Ok(res)
}

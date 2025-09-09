//! File metadata management module for metadata database
//!
//! This module provides functionality for managing file metadata records that track
//! Parquet files and their associated metadata within locations.

use futures::stream::Stream;
use tracing::instrument;

use crate::locations::LocationId;

pub mod file_id;
pub mod pagination;

pub use file_id::{FileId, FileIdFromStrError, FileIdI64ConvError, FileIdU64Error};
pub type FooterBytes = Vec<u8>;

/// Insert new file metadata record
///
/// Creates a new file metadata entry. Uses ON CONFLICT DO NOTHING for idempotency.
#[instrument(skip(executor, footer))]
pub async fn insert<'e, E>(
    executor: E,
    location_id: LocationId,
    file_name: String,
    object_size: u64,
    object_e_tag: Option<String>,
    object_version: Option<String>,
    parquet_meta: serde_json::Value,
    footer: &FooterBytes,
) -> Result<(), sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let query = indoc::indoc! {r#"
        INSERT INTO file_metadata (location_id, file_name, object_size, object_e_tag, object_version, metadata, footer)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT DO NOTHING
    "#};

    sqlx::query(query)
        .bind(location_id)
        .bind(file_name)
        .bind(object_size as i64)
        .bind(object_e_tag)
        .bind(object_version)
        .bind(parquet_meta)
        .bind(footer)
        .execute(executor)
        .await?;

    Ok(())
}

/// Stream file metadata for a specific location
///
/// Returns file metadata with location information via JOIN.
#[instrument(skip(executor))]
pub fn stream<'e, E>(
    executor: E,
    location_id: LocationId,
) -> impl Stream<Item = Result<FileMetadata, sqlx::Error>> + 'e
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres> + 'e,
{
    let query = indoc::indoc! {r#"
        SELECT fm.id,
               fm.location_id,
               fm.file_name,
               l.url,
               fm.object_size,
               fm.object_e_tag,
               fm.object_version,
               fm.metadata
        FROM file_metadata fm
        JOIN locations l ON fm.location_id = l.id
        WHERE location_id = $1
    "#};

    sqlx::query_as(query).bind(location_id).fetch(executor)
}

/// Represents file metadata with location information
///
/// Contains metadata for a Parquet file along with its associated location details.
#[derive(Debug, sqlx::FromRow)]
pub struct FileMetadata {
    /// file_metadata.id
    pub id: FileId,
    /// file_metadata.location_id
    pub location_id: LocationId,
    /// file_metadata.file_name
    pub file_name: String,
    /// location.url
    pub url: String,
    /// file_metadata.object_size
    pub object_size: Option<i64>,
    /// file_metadata.object_e_tag
    pub object_e_tag: Option<String>,
    /// file_metadata.object_version
    pub object_version: Option<String>,
    /// file_metadata.metadata
    pub metadata: serde_json::Value,
}

/// Get footer bytes for a file by ID
#[instrument(skip(executor))]
pub async fn get_footer_bytes_by_id<'e, E>(
    executor: E,
    id: FileId,
) -> Result<FooterBytes, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let query = "SELECT footer FROM file_metadata WHERE id = $1";

    sqlx::query_scalar(query).bind(id).fetch_one(executor).await
}

/// Delete file metadata record by ID
#[instrument(skip(executor))]
pub async fn delete<'e, E>(executor: E, id: FileId) -> Result<bool, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let query = "DELETE FROM file_metadata WHERE id = $1";

    let result = sqlx::query(query).bind(id).execute(executor).await?;
    Ok(result.rows_affected() > 0)
}

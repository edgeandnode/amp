//! File metadata management module for metadata database
//!
//! This module provides functionality for managing file metadata records that track
//! Parquet files and their associated metadata within locations.

use futures::stream::Stream;
use tracing::instrument;
use url::Url;

use crate::physical_table::LocationId;

pub mod file_id;
pub mod pagination;

pub use file_id::{FileId, FileIdFromStrError, FileIdI64ConvError, FileIdU64Error};
pub type FooterBytes = Vec<u8>;

/// Insert new file metadata record
///
/// Creates a new file metadata entry. Uses ON CONFLICT DO NOTHING for idempotency.
/// Also inserts the footer into the footer_cache table.
/// Also inserts the footer into the footer_cache table.
#[instrument(skip(executor, footer))]
#[expect(clippy::too_many_arguments)]
pub async fn insert<'e, E>(
    executor: E,
    location_id: LocationId,
    url: &Url,
    url: &Url,
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
        WITH inserted AS (
            INSERT INTO file_metadata (location_id, url, file_name, object_size, object_e_tag, object_version, metadata)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT DO NOTHING
            RETURNING id
        )
        INSERT INTO footer_cache (file_id, footer)
        SELECT id, $8 FROM inserted
    "#};

    sqlx::query(query)
        .bind(location_id)
        .bind(url.as_str())
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

/// Get file metadata by ID with detailed location information
///
/// Retrieves a single file metadata record joined with location data.
/// Returns the complete file metadata including location URL needed for object store operations.
/// Returns `None` if the file ID is not found.
#[instrument(skip(executor))]
pub async fn get_by_id_with_details<'e, E>(
    executor: E,
    file_id: FileId,
) -> Result<Option<FileMetadataWithDetails>, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT fm.id,
               fm.location_id,
               fm.file_name,
               fm.url,
               fm.object_size,
               fm.object_e_tag,
               fm.object_version,
               fm.metadata
        FROM file_metadata fm
        WHERE fm.id = $1
    "#};

    sqlx::query_as(query)
        .bind(file_id)
        .fetch_optional(executor)
        .await
}

/// Stream file metadata for a specific location
///
/// Returns file metadata with location information via JOIN.
#[instrument(skip(executor))]
pub fn stream_with_details<'e, E>(
    executor: E,
    location_id: LocationId,
) -> impl Stream<Item = Result<FileMetadataWithDetails, sqlx::Error>> + 'e
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres> + 'e,
{
    let query = indoc::indoc! {r#"
        SELECT fm.id,
               fm.location_id,
               fm.file_name,
               fm.url,
               fm.object_size,
               fm.object_e_tag,
               fm.object_version,
               fm.metadata
        FROM file_metadata fm
        WHERE location_id = $1
    "#};

    sqlx::query_as(query).bind(location_id).fetch(executor)
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
    let query = "SELECT footer FROM footer_cache WHERE file_id = $1";

    sqlx::query_scalar(query).bind(id).fetch_one(executor).await
}

/// Delete file metadata record by ID
///
/// Also deletes the corresponding footer_cache entry.
#[instrument(skip(executor))]
pub async fn delete<'e, E>(executor: E, id: FileId) -> Result<bool, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    // Delete from footer_cache first (no FK constraint), then file_metadata
    let query = indoc::indoc! {r#"
        WITH deleted_footer AS (
            DELETE FROM footer_cache WHERE file_id = $1
        )
        DELETE FROM file_metadata WHERE id = $1
    "#};

    let result = sqlx::query(query).bind(id).execute(executor).await?;
    Ok(result.rows_affected() > 0)
}

/// Lightweight file metadata for listing and pagination operations
///
/// Contains essential file information without heavy fields like parquet metadata JSON.
/// Used for efficient bulk operations like listing files in a location where only basic
/// file properties are needed (ID, name, size, etc.).
///
/// **Use this type when:**
/// - Listing files in bulk (pagination endpoints)
/// - Only basic file info is needed
/// - Performance is critical (avoiding large JSON parsing)
///
/// **Prefer [`FileMetadataWithDetails`] when:**
/// - Complete file information is needed
/// - Performing single file operations
/// - Parquet metadata access is required
#[derive(Debug, sqlx::FromRow)]
pub struct FileMetadata {
    /// Unique identifier for the file
    pub id: FileId,
    /// ID of the location containing this file
    pub location_id: LocationId,
    /// Name/path of the file in the object store
    pub file_name: String,
    /// Storage location URL (parsed during deserialization)
    #[sqlx(try_from = "&'a str")]
    pub url: Url,
    /// Size of the file in bytes
    pub object_size: Option<i64>,
    /// Object store ETag for version tracking
    pub object_e_tag: Option<String>,
    /// Object store version identifier
    pub object_version: Option<String>,
}

/// Complete file metadata with full parquet details for operations requiring all information
///
/// Contains comprehensive file metadata including the heavy parquet metadata JSON field.
/// Used for operations that need complete file information such as object store deletions,
/// detailed file inspection, or when full metadata access is required.
#[derive(Debug, sqlx::FromRow)]
pub struct FileMetadataWithDetails {
    /// Unique identifier for the file
    pub id: FileId,
    /// ID of the location containing this file
    pub location_id: LocationId,
    /// Name/path of the file in the object store
    pub file_name: String,
    /// Storage location URL (parsed during deserialization)
    #[sqlx(try_from = "&'a str")]
    pub url: Url,
    /// Size of the file in bytes
    pub object_size: Option<i64>,
    /// Object store ETag for version tracking
    pub object_e_tag: Option<String>,
    /// Object store version identifier
    pub object_version: Option<String>,
    /// Complete parquet metadata as JSON (includes schema, statistics, etc.)
    pub metadata: serde_json::Value,
}

//! File metadata management module for metadata database
//!
//! This module provides functionality for managing file metadata records that track
//! Parquet files and their associated metadata within locations.

use futures::stream::{Stream, StreamExt};
use url::Url;

use crate::{Error, Executor, physical_table::LocationId};

mod file_id;
mod name;
pub(crate) mod sql;

pub use self::{
    file_id::{FileId, FileIdFromStrError, FileIdI64ConvError, FileIdU64Error},
    name::{Name as FileName, NameOwned as FileNameOwned},
    sql::{FileMetadata, FileMetadataWithDetails},
};
pub type FooterBytes = Vec<u8>;

/// Register a new file in the metadata database
///
/// Creates a new file metadata entry. Uses ON CONFLICT DO NOTHING for idempotency.
/// Also inserts the footer into the footer_cache table.
#[tracing::instrument(skip(exe, footer), err)]
#[expect(clippy::too_many_arguments)]
pub async fn register<'c, E>(
    exe: E,
    location_id: LocationId,
    url: &Url,
    file_name: impl Into<FileName<'_>> + std::fmt::Debug,
    object_size: u64,
    object_e_tag: Option<String>,
    object_version: Option<String>,
    parquet_meta: serde_json::Value,
    footer: &FooterBytes,
) -> Result<(), Error>
where
    E: Executor<'c>,
{
    sql::insert(
        exe,
        location_id,
        url,
        file_name.into(),
        object_size,
        object_e_tag,
        object_version,
        parquet_meta,
        footer,
    )
    .await
    .map_err(Error::Database)
}

/// Get file metadata by ID with detailed location information
///
/// Retrieves a single file metadata record joined with location data.
/// Returns the complete file metadata including location URL needed for object store operations.
/// Returns `None` if the file ID is not found.
#[tracing::instrument(skip(exe), err)]
pub async fn get_by_id_with_details<'c, E>(
    exe: E,
    file_id: FileId,
) -> Result<Option<FileMetadataWithDetails>, Error>
where
    E: Executor<'c>,
{
    sql::get_by_id_with_details(exe, file_id)
        .await
        .map_err(Error::Database)
}

/// Stream file metadata for a specific location
///
/// Returns file metadata with location information via JOIN.
#[tracing::instrument(skip(exe))]
pub fn stream_by_location_id_with_details<'c, E>(
    exe: E,
    location_id: LocationId,
) -> impl Stream<Item = Result<FileMetadataWithDetails, Error>> + 'c
where
    E: Executor<'c> + 'c,
{
    sql::stream_with_details(exe, location_id).map(|r| r.map_err(Error::Database))
}

/// Get footer bytes for a file by ID
#[tracing::instrument(skip(exe), err)]
pub async fn get_footer_bytes<'c, E>(exe: E, id: FileId) -> Result<FooterBytes, Error>
where
    E: Executor<'c>,
{
    sql::get_footer_bytes_by_id(exe, id)
        .await
        .map_err(Error::Database)
}

/// Delete multiple file metadata records by IDs
///
/// Returns the IDs of files that were successfully deleted.
#[tracing::instrument(skip(exe), err)]
pub async fn delete_by_ids<'c, E>(exe: E, ids: &[FileId]) -> Result<Vec<FileId>, Error>
where
    E: Executor<'c>,
{
    sql::delete_by_ids(exe, ids).await.map_err(Error::Database)
}

use metadata_db::{MetadataDb, files::FooterBytes, physical_table_revision::LocationId};
use url::Url;

use crate::{meta::ParquetMeta, retry::RetryableErrorExt};

/// Registers a written Parquet file in the metadata database and notifies listeners of the change.
pub async fn commit_metadata(
    metadata_db: &MetadataDb,
    parquet_meta: ParquetMeta,
    object_meta: object_store::ObjectMeta,
    location_id: LocationId,
    url: &Url,
    footer: FooterBytes,
) -> Result<(), CommitMetadataError> {
    let object_store::ObjectMeta {
        size: object_size,
        e_tag: object_e_tag,
        version: object_version,
        ..
    } = object_meta;

    let file_name = parquet_meta.filename.clone();
    let parquet_meta = serde_json::to_value(parquet_meta)
        .map_err(CommitMetadataError::SerializeParquetMetadata)?;
    metadata_db::files::register(
        metadata_db,
        location_id,
        url,
        file_name,
        object_size,
        object_e_tag,
        object_version,
        parquet_meta,
        &footer,
    )
    .await
    .map_err(CommitMetadataError::Register)?;

    // Notify that the dataset has been changed
    tracing::trace!("notifying location change for location_id: {}", location_id);
    metadata_db::physical_table::send_location_change_notif(metadata_db, location_id)
        .await
        .map_err(CommitMetadataError::Notify)?;

    Ok(())
}

/// Errors that occur when committing file metadata to the database
///
/// This error type is used by `commit_metadata()`.
#[derive(Debug, thiserror::Error)]
pub enum CommitMetadataError {
    /// Failed to register file metadata in the database
    ///
    /// This error occurs when inserting the file metadata record into the
    /// metadata database fails.
    ///
    /// Common causes:
    /// - Database connection lost
    /// - Constraint violation (duplicate file ID)
    /// - Transaction timeout
    #[error("Failed to register file metadata")]
    Register(#[source] metadata_db::Error),

    /// Failed to serialize parquet metadata to JSON
    ///
    /// This error occurs when converting the parquet metadata structure
    /// to a JSON string for storage in the database.
    ///
    /// Common causes:
    /// - Invalid UTF-8 in metadata fields
    /// - Serialization of non-serializable types
    #[error("Failed to serialize parquet metadata")]
    SerializeParquetMetadata(#[source] serde_json::Error),

    /// Failed to notify subscribers of location change
    ///
    /// This error occurs when publishing a notification about the new file
    /// to the database notification channel fails.
    ///
    /// Common causes:
    /// - Database connection lost
    /// - Notification channel unavailable
    /// - Payload too large
    #[error("Failed to notify location change")]
    Notify(#[source] metadata_db::Error),
}

impl RetryableErrorExt for CommitMetadataError {
    /// Returns `true` for database-related errors that may succeed on retry.
    fn is_retryable(&self) -> bool {
        match self {
            Self::Register(err) => err.is_retryable(),
            Self::SerializeParquetMetadata(_) => false,
            Self::Notify(err) => err.is_retryable(),
        }
    }
}

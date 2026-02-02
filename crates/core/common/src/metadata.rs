use std::sync::Arc;

use amp_data_store::{DataStore, PhyTableRevisionFileMetadata, file_name::FileName};
use datafusion::parquet::{
    arrow::{arrow_reader::ArrowReaderOptions, async_reader::AsyncFileReader},
    errors::ParquetError,
    file::metadata::{ParquetMetaData, ParquetMetaDataWriter},
};
use metadata_db::{
    LocationId,
    files::{FileId, FileMetadataWithDetails as FileMetadataRow, FooterBytes},
};
use object_store::{ObjectMeta, path::Path};
use tracing::instrument;

pub mod parquet;
pub mod segments;
mod size;

use self::parquet::{PARQUET_METADATA_KEY, ParquetMeta};
pub use self::size::{
    Generation, Overflow, SegmentSize, get_block_count, le_bytes_to_nonzero_i64_opt,
};

#[derive(Debug, Clone)]
pub struct FileMetadata {
    pub file_id: FileId,
    pub file_name: FileName,
    pub location_id: LocationId,
    pub object_meta: ObjectMeta,
    pub parquet_meta: ParquetMeta,
}

impl FileMetadata {
    /// Create FileMetadata from a database row with a table path prefix.
    ///
    /// The `table_path` should be the relative path of the table within the data store
    /// (e.g., `anvil_rpc/blocks/uuid`). This is used to construct the file's location
    /// relative to the data store, which is needed for the object store to find the file.
    pub fn from_row_with_table_path(
        row: FileMetadataRow,
        table_path: &Path,
    ) -> Result<Self, FromRowError> {
        let FileMetadataRow {
            id: file_id,
            location_id,
            file_name,
            object_size,
            object_e_tag: e_tag,
            object_version: version,
            metadata,
            ..
        } = row;

        // The location is the table path + filename, relative to the data store prefix.
        // The AmpReaderFactory's object_store has the data store prefix, so paths
        // should be like "dataset/table/revision_id/filename.parquet".
        let location = table_path.child(file_name.as_str());

        let parquet_meta: ParquetMeta = serde_json::from_value(metadata).map_err(FromRowError)?;

        let size = object_size.unwrap_or_default() as u64;

        // Extract created_at from ParquetMeta to use as last_modified.
        // Segments are created once and never modified, so created_at is appropriate.
        let last_modified = {
            let this = &parquet_meta.created_at;
            chrono::DateTime::from_timestamp(this.0.as_secs() as i64, this.0.subsec_nanos())
                .unwrap_or_default()
        };

        let object_meta = ObjectMeta {
            location,
            last_modified,
            size,
            e_tag,
            version,
        };

        Ok(Self {
            file_id,
            file_name: file_name.into(),
            location_id,
            object_meta,
            parquet_meta,
        })
    }
}

impl TryFrom<PhyTableRevisionFileMetadata> for FileMetadata {
    type Error = serde_json::Error;

    fn try_from(rev_meta: PhyTableRevisionFileMetadata) -> Result<Self, Self::Error> {
        let parquet_meta: ParquetMeta = serde_json::from_value(rev_meta.parquet_meta_json)?;
        Ok(Self {
            file_id: rev_meta.file_id,
            file_name: rev_meta.file_name,
            location_id: rev_meta.location_id,
            object_meta: rev_meta.object_meta,
            parquet_meta,
        })
    }
}

/// Error when creating FileMetadata from a database row
#[derive(Debug, thiserror::Error)]
#[error("Failed to parse file metadata from database row")]
pub struct FromRowError(#[source] serde_json::Error);

#[instrument(skip(object_meta, store), err)]
pub async fn extract_footer_bytes_from_file(
    store: &DataStore,
    object_meta: &ObjectMeta,
) -> Result<FooterBytes, ParquetError> {
    let parquet_metadata = extract_parquet_metadata_from_file(store, object_meta).await?;
    let mut footer_bytes = Vec::new();

    ParquetMetaDataWriter::new(&mut footer_bytes, &parquet_metadata).finish()?;
    Ok(footer_bytes)
}

#[instrument(skip(object_meta, store), err)]
pub async fn amp_metadata_from_parquet_file(
    store: &DataStore,
    object_meta: &ObjectMeta,
) -> Result<(FileName, ParquetMeta, FooterBytes), AmpMetadataFromParquetError> {
    let parquet_metadata = extract_parquet_metadata_from_file(store, object_meta)
        .await
        .map_err(AmpMetadataFromParquetError::ExtractMetadata)?;

    let file_metadata = parquet_metadata.file_metadata();

    let key_value_metadata = file_metadata.key_value_metadata().ok_or_else(|| {
        AmpMetadataFromParquetError::MissingKeyValueMetadata {
            file_path: object_meta.location.to_string(),
        }
    })?;

    let parquet_meta_key_value_pair = key_value_metadata
        .iter()
        .find(|key_value| key_value.key.as_str() == PARQUET_METADATA_KEY)
        .ok_or_else(|| AmpMetadataFromParquetError::MissingMetadataKey {
            key: PARQUET_METADATA_KEY.to_string(),
            file_path: object_meta.location.to_string(),
        })?;

    let parquet_meta_json = parquet_meta_key_value_pair.value.as_ref().ok_or_else(|| {
        AmpMetadataFromParquetError::EmptyMetadataValue {
            key: PARQUET_METADATA_KEY.to_string(),
            file_path: object_meta.location.to_string(),
        }
    })?;

    let parquet_meta: ParquetMeta = serde_json::from_str(parquet_meta_json).map_err(|err| {
        AmpMetadataFromParquetError::ParseParquetMeta {
            file_path: object_meta.location.to_string(),
            source: err,
        }
    })?;

    // SAFETY: Filenames from object store are trusted - they were created
    // by our constructors and stored in parquet file metadata.
    let file_name = FileName::new_unchecked(object_meta.location.filename().unwrap().to_string());

    let mut footer_bytes = Vec::new();

    ParquetMetaDataWriter::new(&mut footer_bytes, &parquet_metadata)
        .finish()
        .map_err(AmpMetadataFromParquetError::WriteFooter)?;

    Ok((file_name, parquet_meta, footer_bytes))
}

/// Errors that can occur when extracting Amp metadata from Parquet files
///
/// These errors represent failures during the process of reading Parquet file metadata
/// and extracting Amp-specific metadata embedded in the file's key-value metadata.
#[derive(Debug, thiserror::Error)]
pub enum AmpMetadataFromParquetError {
    /// Failed to extract parquet metadata from file
    ///
    /// This occurs when the Parquet library fails to read or parse the file's metadata
    /// section, typically due to:
    /// - Corrupted Parquet file structure
    /// - Invalid or incomplete Parquet footer
    /// - I/O errors reading from object store
    /// - Unsupported Parquet format version
    #[error("Failed to extract parquet metadata from file")]
    ExtractMetadata(#[source] ParquetError),

    /// Failed to write parquet metadata footer
    ///
    /// This occurs when serializing the Parquet metadata footer to bytes fails.
    /// This is typically a rare error that indicates:
    /// - Memory allocation failure
    /// - Internal Parquet library error
    #[error("Failed to write parquet metadata footer")]
    WriteFooter(#[source] ParquetError),

    /// Key value metadata section is missing from Parquet file
    ///
    /// This occurs when the Parquet file does not contain a key-value metadata section.
    /// All Amp-generated Parquet files should have this section, so this error indicates:
    /// - File was not created by Amp
    /// - File was created by an older version of Amp
    /// - File corruption removed the metadata section
    #[error("Key value metadata missing for file: {file_path}")]
    MissingKeyValueMetadata {
        /// The file path where the error occurred
        file_path: String,
    },

    /// Required metadata key not found in Parquet file metadata
    ///
    /// This occurs when the Parquet file's key-value metadata does not contain
    /// the expected Amp metadata key. This indicates:
    /// - File was not created by Amp
    /// - File was created by an incompatible version of Amp
    /// - Metadata was stripped or modified after file creation
    #[error("Missing required metadata key '{key}' in file: {file_path}")]
    MissingMetadataKey {
        /// The metadata key that was not found
        key: String,
        /// The file path where the error occurred
        file_path: String,
    },

    /// Metadata value is empty for the specified key
    ///
    /// This occurs when the metadata key exists but its associated value is null or empty.
    /// This indicates:
    /// - Incomplete metadata write during file creation
    /// - File corruption affecting metadata values
    /// - Bug in file creation logic
    #[error("Empty metadata value for key '{key}' in file: {file_path}")]
    EmptyMetadataValue {
        /// The metadata key with empty value
        key: String,
        /// The file path where the error occurred
        file_path: String,
    },

    /// Failed to deserialize ParquetMeta from JSON metadata
    ///
    /// This occurs when the metadata value cannot be parsed as valid ParquetMeta JSON.
    /// Common causes:
    /// - Incompatible schema version between file creator and reader
    /// - Corrupted JSON in metadata value
    /// - Missing required fields in JSON structure
    /// - Type mismatches in JSON fields
    #[error("Failed to parse ParquetMeta from metadata in file: {file_path}")]
    ParseParquetMeta {
        /// The file path where the error occurred
        file_path: String,
        /// The underlying JSON parsing error
        #[source]
        source: serde_json::Error,
    },
}

async fn extract_parquet_metadata_from_file(
    store: &DataStore,
    object_meta: &ObjectMeta,
) -> Result<Arc<ParquetMetaData>, ParquetError> {
    let mut reader = store
        .create_file_reader_from_path(object_meta.location.clone())
        .with_file_size(object_meta.size)
        .with_preload_column_index(true)
        .with_preload_offset_index(true);

    let options = ArrowReaderOptions::default().with_page_index(true);
    reader.get_metadata(Some(&options)).await
}

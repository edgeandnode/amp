use amp_data_store::{PhyTableRevisionFileMetadata, file_name::FileName};
use metadata_db::{
    files::{FileId, FileMetadataWithDetails as FileMetadataRow},
    physical_table_revision::LocationId,
};
use object_store::{ObjectMeta, path::Path};

use crate::metadata::parquet::ParquetMeta;

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

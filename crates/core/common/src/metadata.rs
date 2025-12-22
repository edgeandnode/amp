use std::sync::Arc;

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

mod file_name;
pub mod parquet;
pub mod segments;
mod size;

use self::parquet::{PARQUET_METADATA_KEY, ParquetMeta};
pub use self::{
    file_name::FileName,
    size::{Generation, Overflow, SegmentSize, get_block_count, le_bytes_to_nonzero_i64_opt},
};
use crate::{BoxError, Store};

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
    ) -> Result<Self, BoxError> {
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

        let parquet_meta: ParquetMeta = serde_json::from_value(metadata)?;

        let size = object_size.unwrap_or_default() as u64;

        let object_meta = ObjectMeta {
            location,
            last_modified: Default::default(),
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

#[instrument(skip(object_meta, store), err)]
pub async fn extract_footer_bytes_from_file(
    store: &Store,
    object_meta: &ObjectMeta,
) -> Result<FooterBytes, ParquetError> {
    let parquet_metadata = extract_parquet_metadata_from_file(store, object_meta).await?;
    let mut footer_bytes = Vec::new();

    ParquetMetaDataWriter::new(&mut footer_bytes, &parquet_metadata).finish()?;
    Ok(footer_bytes)
}

#[instrument(skip(object_meta, store), err)]
pub async fn amp_metadata_from_parquet_file(
    store: &Store,
    object_meta: &ObjectMeta,
) -> Result<(FileName, ParquetMeta, FooterBytes), BoxError> {
    let parquet_metadata = extract_parquet_metadata_from_file(store, object_meta).await?;

    let file_metadata = parquet_metadata.file_metadata();

    let key_value_metadata =
        file_metadata
            .key_value_metadata()
            .ok_or(crate::ArrowError::ParquetError(format!(
                "Unable to fetch Key Value metadata for file {}",
                &object_meta.location
            )))?;

    let parquet_meta_key_value_pair = key_value_metadata
        .iter()
        .find(|key_value| key_value.key.as_str() == PARQUET_METADATA_KEY)
        .ok_or(crate::ArrowError::ParquetError(format!(
            "Missing key: {} in file metadata for file {}",
            PARQUET_METADATA_KEY, &object_meta.location
        )))?;

    let parquet_meta_json =
        parquet_meta_key_value_pair
            .value
            .as_ref()
            .ok_or(crate::ArrowError::ParquetError(format!(
                "Unable to parse ParquetMeta from empty value in metadata for file {}",
                &object_meta.location
            )))?;

    let parquet_meta: ParquetMeta = serde_json::from_str(parquet_meta_json).map_err(|err| {
        crate::ArrowError::ParseError(format!(
            "Unable to parse ParquetMeta from key value metadata for file {}: {}",
            &object_meta.location, err
        ))
    })?;

    // SAFETY: Filenames from object store are trusted - they were created
    // by our constructors and stored in parquet file metadata.
    let file_name = FileName::new_unchecked(object_meta.location.filename().unwrap().to_string());

    let mut footer_bytes = Vec::new();

    ParquetMetaDataWriter::new(&mut footer_bytes, &parquet_metadata).finish()?;

    Ok((file_name, parquet_meta, footer_bytes))
}

async fn extract_parquet_metadata_from_file(
    store: &Store,
    object_meta: &ObjectMeta,
) -> Result<Arc<ParquetMetaData>, ParquetError> {
    let mut reader = store
        .create_revision_file_reader(object_meta.location.clone())
        .with_file_size(object_meta.size)
        .with_preload_column_index(true)
        .with_preload_offset_index(true);

    let options = ArrowReaderOptions::default().with_page_index(true);
    reader.get_metadata(Some(&options)).await
}

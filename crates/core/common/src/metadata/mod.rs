use std::sync::Arc;

use datafusion::parquet::{
    arrow::{
        arrow_reader::ArrowReaderOptions,
        async_reader::{AsyncFileReader, ParquetObjectReader},
    },
    errors::ParquetError,
    file::metadata::{ParquetMetaData, ParquetMetaDataWriter},
};
use metadata_db::{FileId, FileMetadataWithDetails as FileMetadataRow, FooterBytes, LocationId};
use object_store::{ObjectMeta, ObjectStore, path::Path};
use tracing::instrument;

use crate::{
    BoxError,
    metadata::parquet::{PARQUET_METADATA_KEY, ParquetMeta},
};

pub mod parquet;
pub mod segments;
mod size;

pub use size::{Generation, Overflow, SegmentSize, get_block_count, le_bytes_to_nonzero_i64_opt};

#[derive(Debug, Clone)]
pub struct FileMetadata {
    pub file_id: FileId,
    pub file_path: String,
    pub location_id: LocationId,
    pub object_meta: ObjectMeta,
    pub parquet_meta: parquet::ParquetMeta,
}

impl TryFrom<FileMetadataRow> for FileMetadata {
    type Error = BoxError;
    fn try_from(
        FileMetadataRow {
            id: file_id,
            location_id,
            file_path,
            object_size,
            object_e_tag: e_tag,
            object_version: version,
            metadata,
        }: FileMetadataRow,
    ) -> Result<Self, Self::Error> {
        let location = Path::parse(&file_path)?;

        let parquet_meta: parquet::ParquetMeta = serde_json::from_value(metadata)?;

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
            file_path,
            location_id,
            object_meta,
            parquet_meta,
        })
    }
}

#[instrument(skip(object_meta, object_store), err)]
pub async fn extract_footer_bytes_from_file(
    object_meta: &ObjectMeta,
    object_store: Arc<dyn ObjectStore>,
) -> Result<FooterBytes, ParquetError> {
    let parquet_metadata = extract_parquet_metadata_from_file(object_meta, object_store).await?;
    let mut footer_bytes = Vec::new();

    ParquetMetaDataWriter::new(&mut footer_bytes, &parquet_metadata).finish()?;
    Ok(footer_bytes)
}

#[instrument(skip(object_meta, object_store), err)]
pub async fn amp_metadata_from_parquet_file(
    object_meta: &ObjectMeta,
    object_store: Arc<dyn ObjectStore>,
) -> Result<(String, ParquetMeta, FooterBytes), BoxError> {
    let parquet_metadata = extract_parquet_metadata_from_file(object_meta, object_store).await?;

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

    let parquet_meta: parquet::ParquetMeta =
        serde_json::from_str(parquet_meta_json).map_err(|e| {
            crate::ArrowError::ParseError(format!(
                "Unable to parse ParquetMeta from key value metadata for file {}: {}",
                &object_meta.location, e
            ))
        })?;

    let file_path = object_meta.location.as_ref().to_string();

    let mut footer_bytes = Vec::new();

    ParquetMetaDataWriter::new(&mut footer_bytes, &parquet_metadata).finish()?;

    Ok((file_path, parquet_meta, footer_bytes))
}

async fn extract_parquet_metadata_from_file(
    object_meta: &ObjectMeta,
    object_store: Arc<dyn ObjectStore>,
) -> Result<Arc<ParquetMetaData>, ParquetError> {
    let mut reader = ParquetObjectReader::new(object_store.clone(), object_meta.location.clone())
        .with_preload_column_index(true)
        .with_preload_offset_index(true)
        .with_file_size(object_meta.size);

    let options = ArrowReaderOptions::default().with_page_index(true);
    reader.get_metadata(Some(&options)).await
}

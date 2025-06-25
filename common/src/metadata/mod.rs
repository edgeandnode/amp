use std::sync::Arc;

use datafusion::parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use metadata_db::{FileId, FileMetadataRow, LocationId, MetadataHash};
use object_store::{path::Path, ObjectMeta, ObjectStore};
use url::Url;

use crate::BoxError;

pub mod parquet;
pub mod range;

#[derive(Debug, Clone)]
pub struct FileMetadata {
    pub file_id: FileId,
    pub file_name: String,
    pub location_id: LocationId,
    pub object_meta: ObjectMeta,
    pub url: Url,
    pub metadata: ParquetMetaData,
    pub metadata_hash: MetadataHash,
}

impl TryFrom<FileMetadataRow> for FileMetadata {
    type Error = BoxError;
    fn try_from(
        FileMetadataRow {
            id: file_id,
            location_id,
            file_name,
            url,
            object_size,
            object_e_tag: e_tag,
            object_version: version,
            metadata,
            metadata_hash,
            ..
        }: FileMetadataRow,
    ) -> Result<Self, Self::Error> {
        let url = Url::parse(&url)?.join(&file_name)?;
        let location = Path::from_url_path(url.path())?;
        let size = object_size.unwrap_or_default() as u64;

        let object_meta = ObjectMeta {
            location,
            last_modified: Default::default(),
            size,
            e_tag,
            version,
        };

        let metadata = ParquetMetaDataReader::decode_metadata(&metadata)?;

        Ok(Self {
            file_id,
            file_name,
            location_id,
            object_meta,
            url,
            metadata,
            metadata_hash,
        })
    }
}

pub async fn read_metadata_bytes_from_parquet(
    object_meta: &ObjectMeta,
    object_store: Arc<dyn ObjectStore>,
) -> Result<(String, Vec<u8>), BoxError> {
    let mut footer = [0u8; 8];
    let range = object_meta.size - 8..object_meta.size;
    footer.copy_from_slice(&object_store.get_range(&object_meta.location, range).await?);
    let footer = ParquetMetaDataReader::decode_footer_tail(&footer)
        .map_err(|e| BoxError::from(format!("Failed to decode footer: {e}")))?;
    let metadata_length = footer.metadata_length();
    let capacity = metadata_length + 8;
    let range = object_meta.size - capacity as u64..object_meta.size;
    let metadata = object_store
        .get_range(&object_meta.location, range)
        .await?
        .to_vec();

    // Unwrap: We know this is a path with valid file name because we just opened it
    let file_name = object_meta.location.filename().unwrap().to_string();
    Ok((file_name, metadata))
}

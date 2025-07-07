use metadata_db::{FileId, FileMetadataRow, LocationId};
use object_store::{ObjectMeta, path::Path};
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
    pub parquet_meta: parquet::ParquetMeta,
    /// true when the file is associated with canonical chains
    pub canonical: bool,
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
            canonical,
        }: FileMetadataRow,
    ) -> Result<Self, Self::Error> {
        let url = Url::parse(&url)?.join(&file_name)?;
        let location = Path::from_url_path(url.path())?;

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
            file_name,
            location_id,
            object_meta,
            parquet_meta,
            canonical,
        })
    }
}

use std::sync::Arc;

use bytes::Bytes;
use datafusion::parquet::{
    arrow::{
        arrow_reader::ArrowReaderOptions,
        async_reader::{AsyncFileReader, ParquetObjectReader},
    },
    file::metadata::{ParquetMetaData, ParquetMetaDataReader, ParquetMetaDataWriter},
};
use metadata_db::{FileId, FileMetadataRow, LocationId, MetadataHash};
use object_store::{path::Path, ObjectMeta, ObjectStore};
use parquet::ParquetMeta;
use url::Url;

use crate::{BoxError, BoxResult};

pub mod parquet;
pub mod range;

#[derive(Debug, Clone)]
pub struct FileMetadata {
    pub file_id: FileId,
    pub file_name: String,
    pub location_id: LocationId,
    pub object_meta: ObjectMeta,
    pub url: Url,
    pub metadata: Arc<ParquetMetaData>,
    pub metadata_hash: MetadataHash,
}

impl FileMetadata {
    pub fn file_id(&self) -> FileId {
        self.file_id
    }

    pub fn location_id(&self) -> LocationId {
        self.location_id
    }

    pub fn file_name(&self) -> &str {
        &self.file_name
    }

    pub fn object_meta(&self) -> &ObjectMeta {
        &self.object_meta
    }

    pub fn url(&self) -> &Url {
        &self.url
    }

    pub fn metadata(&self) -> Arc<ParquetMetaData> {
        self.metadata.clone()
    }

    pub fn parquet_meta(&self) -> BoxResult<ParquetMeta> {
        ParquetMeta::try_from_parquet_metadata(self.metadata(), self.url(), self.location_id())
    }
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

        let metadata = ParquetMetaDataReader::new()
            .with_page_indexes(true)
            .parse_and_finish(&Bytes::from_iter(metadata))?
            .into();

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
    let metadata = ParquetObjectReader::new(object_store, object_meta.location.clone())
        .with_preload_column_index(true)
        .with_preload_offset_index(true)
        .get_metadata(Some(&ArrowReaderOptions::default().with_page_index(true)))
        .await?;

    let mut buf = Vec::new();
    ParquetMetaDataWriter::new(&mut buf, &metadata).finish()?;

    // Unwrap: We just opened this file, so it must have a valid filename.
    let file_name = object_meta.location.filename().unwrap().to_string();
    Ok((file_name, buf))
}

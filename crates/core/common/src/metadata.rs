use std::sync::Arc;

use datafusion::parquet::{
    arrow::{
        arrow_reader::ArrowReaderOptions,
        async_reader::{AsyncFileReader, ParquetObjectReader},
    },
    errors::ParquetError,
    file::metadata::{ParquetMetaData, ParquetMetaDataWriter},
};
use metadata_db::{
    LocationId,
    files::{FileId, FileMetadataWithDetails as FileMetadataRow, FooterBytes},
};
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
    pub file_name: FileName,
    pub location_id: LocationId,
    pub object_meta: ObjectMeta,
    pub parquet_meta: ParquetMeta,
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
            ..
        }: FileMetadataRow,
    ) -> Result<Self, Self::Error> {
        let url = url.join(&file_name)?;
        let location = Path::from_url_path(url.path())?;

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

    let file_name = object_meta.location.filename().unwrap().to_string();

    let mut footer_bytes = Vec::new();

    ParquetMetaDataWriter::new(&mut footer_bytes, &parquet_metadata).finish()?;

    Ok((file_name, parquet_meta, footer_bytes))
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

/// A validated file name for parquet files.
///
/// File names must be non-empty and not exceed filesystem limits.
/// This type validates at system boundaries and converts to/from the
/// database transport type `metadata_db::files::FileName`.
#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct FileName(String);

impl FileName {
    /// Create a new FileName from a String without validation.
    ///
    /// This constructor trusts that the caller provides a valid filename.
    // TODO: Add a validation function that checks if the filename was corretly created
    pub fn new_unchecked(name: String) -> Self {
        Self(name)
    }

    /// Returns a reference to the inner string value.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the FileName and returns the inner String.
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl AsRef<str> for FileName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl AsRef<FileName> for FileName {
    #[inline(always)]
    fn as_ref(&self) -> &FileName {
        self
    }
}

impl std::ops::Deref for FileName {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq<str> for FileName {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<FileName> for str {
    fn eq(&self, other: &FileName) -> bool {
        *self == other.0
    }
}

impl PartialEq<&str> for FileName {
    fn eq(&self, other: &&str) -> bool {
        self.0 == **other
    }
}

impl PartialEq<String> for FileName {
    fn eq(&self, other: &String) -> bool {
        self.0 == *other
    }
}

impl PartialEq<FileName> for String {
    fn eq(&self, other: &FileName) -> bool {
        *self == other.0
    }
}

impl std::fmt::Display for FileName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<metadata_db::files::FileNameOwned> for FileName {
    fn from(value: metadata_db::files::FileNameOwned) -> Self {
        // Convert to string and wrap - this should always be valid since FileNameOwned
        // comes from the database and should already be validated
        FileName(value.into_inner())
    }
}

impl From<FileName> for metadata_db::files::FileNameOwned {
    fn from(value: FileName) -> Self {
        // SAFETY: FileName maintains invariants through its constructor. It is the constructor's
        // responsibility to ensure invariants hold at creation time.
        metadata_db::files::FileName::from_owned_unchecked(value.0)
    }
}

impl<'a> From<&'a FileName> for metadata_db::files::FileName<'a> {
    fn from(value: &'a FileName) -> Self {
        // SAFETY: FileName maintains invariants through its constructor. It is the constructor's
        // responsibility to ensure invariants hold at creation time.
        metadata_db::files::FileName::from_ref_unchecked(&value.0)
    }
}

impl<'a> PartialEq<metadata_db::files::FileName<'a>> for FileName {
    fn eq(&self, other: &metadata_db::files::FileName<'a>) -> bool {
        self.as_str() == other.as_str()
    }
}

impl<'a> PartialEq<FileName> for metadata_db::files::FileName<'a> {
    fn eq(&self, other: &FileName) -> bool {
        self.as_str() == other.as_str()
    }
}

impl<'a> PartialEq<&metadata_db::files::FileName<'a>> for FileName {
    fn eq(&self, other: &&metadata_db::files::FileName<'a>) -> bool {
        self.as_str() == other.as_str()
    }
}

impl<'a> PartialEq<FileName> for &metadata_db::files::FileName<'a> {
    fn eq(&self, other: &FileName) -> bool {
        self.as_str() == other.as_str()
    }
}

impl<'a> PartialEq<&FileName> for metadata_db::files::FileName<'a> {
    fn eq(&self, other: &&FileName) -> bool {
        self.as_str() == other.as_str()
    }
}

impl<'a> PartialEq<metadata_db::files::FileName<'a>> for &FileName {
    fn eq(&self, other: &metadata_db::files::FileName<'a>) -> bool {
        self.as_str() == other.as_str()
    }
}

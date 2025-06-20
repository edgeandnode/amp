use std::{collections::BTreeMap, sync::Arc};

use datafusion::parquet::file::metadata::{ParquetMetaDataReader, RowGroupMetaDataPtr};
use futures::{StreamExt as _, TryStreamExt as _};
use metadata_db::{FileId, FileMetadataRow, LocationId, MetadataDb};
use object_store::{path::Path, ObjectMeta, ObjectStore};
use parquet::ParquetMeta;
use url::Url;

use crate::{metadata::range::BlockRange, multirange::MultiRange, BoxError, QueryContext};

pub mod parquet;
pub mod range;

#[derive(Debug, Clone)]
pub struct FileMetadata {
    pub file_id: FileId,
    pub file_name: String,
    pub location_id: LocationId,
    pub object_meta: ObjectMeta,
    pub parquet_meta: ParquetMeta,
    pub statistics: Vec<RowGroupMetaDataPtr>,
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

        let arrow_metadata = ParquetMetaDataReader::decode_metadata(&metadata)?;
        let arrow_file_metadata = arrow_metadata.file_metadata();
        let parquet_meta =
            ParquetMeta::try_from_file_metadata(arrow_file_metadata, &url, location_id)?;

        let statistics = arrow_metadata
            .row_groups()
            .into_iter()
            .cloned()
            .map(|rg| Arc::new(rg))
            .collect::<Vec<_>>();

        Ok(Self {
            file_id,
            file_name,
            location_id,
            object_meta,
            parquet_meta,
            statistics,
        })
    }
}

pub async fn ranges_for_table(
    location_id: i64,
    metadata_db: &MetadataDb,
) -> Result<Vec<BlockRange>, BoxError> {
    metadata_db
        .stream_file_metadata(location_id)
        .map(|res| {
            let FileMetadata {
                file_name,
                parquet_meta: parquet::ParquetMeta { mut ranges, .. },
                ..
            } = res?.try_into()?;
            if ranges.len() != 1 {
                return Err(format!("expected exactly 1 range in {file_name}").into());
            }
            Ok(ranges.remove(0))
        })
        .try_collect::<Vec<_>>()
        .await
}

pub async fn block_ranges_by_table(
    ctx: &QueryContext,
) -> Result<BTreeMap<String, Vec<BlockRange>>, BoxError> {
    let mut ranges_by_table = BTreeMap::default();
    for table in ctx.catalog().tables() {
        let ranges = ranges_for_table(table.location_id(), &table.metadata_db).await?;
        ranges_by_table.insert(table.table_name().to_string(), ranges);
    }
    Ok(ranges_by_table)
}

pub async fn multiranges_by_table(
    ctx: &QueryContext,
) -> Result<BTreeMap<String, MultiRange>, BoxError> {
    let ranges = block_ranges_by_table(ctx).await?;
    let multi_ranges = ranges
        .into_iter()
        .map(|(k, v)| {
            let ranges = v.into_iter().map(|r| r.numbers.into_inner()).collect();
            let multirange = MultiRange::from_ranges(ranges)?;
            Ok((k, multirange))
        })
        .collect::<Result<BTreeMap<String, MultiRange>, BoxError>>()?;
    Ok(multi_ranges)
}

pub async fn filenames_for_table(
    metadata_db: &MetadataDb,
    location_id: i64,
) -> Result<Vec<String>, BoxError> {
    let file_names = metadata_db
        .stream_file_metadata(location_id)
        .map(|res| {
            let FileMetadata { file_name, .. } = res?.try_into()?;
            Ok::<_, BoxError>(file_name)
        })
        .try_collect::<Vec<_>>()
        .await?;
    Ok(file_names)
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

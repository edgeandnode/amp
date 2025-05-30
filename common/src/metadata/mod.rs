use std::collections::BTreeMap;

use futures::{StreamExt as _, TryStreamExt as _};
use metadata_db::{FileId, FileMetadataRow, LocationId, MetadataDb};
use object_store::{path::Path, ObjectMeta};
use url::Url;

use crate::{multirange::MultiRange, BoxError, QueryContext};

pub mod parquet;
pub mod range;

#[derive(Debug, Clone)]
pub struct FileMetadata {
    pub file_id: FileId,
    pub file_name: String,
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
            file_name,
            url,
            object_size,
            object_e_tag: e_tag,
            object_version: version,
            metadata,
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
        })
    }
}

pub async fn ranges_for_table(
    location_id: i64,
    metadata_db: &MetadataDb,
) -> Result<Vec<(u64, u64)>, BoxError> {
    metadata_db
        .stream_file_metadata(location_id)
        .map(|res| {
            let FileMetadata {
                parquet_meta:
                    parquet::ParquetMeta {
                        range_start,
                        range_end,
                        ..
                    },
                ..
            } = res?.try_into()?;
            Ok((range_start, range_end))
        })
        .try_collect::<Vec<_>>()
        .await
}

pub async fn block_ranges_by_table(
    ctx: &QueryContext,
) -> Result<BTreeMap<String, MultiRange>, BoxError> {
    let mut multirange_by_table = BTreeMap::default();

    for table in ctx.catalog().tables() {
        let ranges = ranges_for_table(table.location_id(), &table.metadata_db).await?;
        let multi_range = MultiRange::from_ranges(ranges)?;
        multirange_by_table.insert(table.table_name().to_string(), multi_range);
    }

    Ok(multirange_by_table)
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

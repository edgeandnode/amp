use std::collections::BTreeMap;

use futures::{StreamExt as _, TryStreamExt as _};
use metadata_db::MetadataDb;

use crate::{multirange::MultiRange, BoxError, QueryContext};

pub mod parquet;
pub mod range;

pub async fn ranges_for_table(
    location_id: i64,
    metadata_db: &MetadataDb,
) -> Result<Vec<(u64, u64)>, BoxError> {
    metadata_db
        .stream_ranges(location_id)
        .map(|res| {
            let (start, end) = res?;
            Ok((start as u64, end as u64))
        })
        .try_collect::<Vec<_>>()
        .await
}

pub async fn block_ranges_by_table(
    ctx: &QueryContext,
) -> Result<BTreeMap<String, MultiRange>, BoxError> {
    let mut multirange_by_table = BTreeMap::default();

    for table in ctx.catalog().all_tables() {
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
        .stream_file_names(location_id)
        .try_collect::<Vec<_>>()
        .await?;
    Ok(file_names)
}

//! Keeps track of block ranges that have already been scanned. One use of this is an optimization to
//! skip scanned ranges when resuming a write process.
//!
//! These ranges could not be perfectly inferred from tables themselves, because tables can be sparse
//! and not have data for all block numbers. In which case we don't know if a block was never
//! scanned, or if it was scanned and is empty.
//!
//! # Consistency
//! Because there isn't an atomic way of writing multiple files to object storage, we need to be
//! mindful of consistency. The non-atomic write process is:
//! ```ignore
//! write_real_data();
//! write_scanned_ranges();
//! ```
//! Creating the possibility of orphaned data files if the process is interrupted between the two
//! writes or if the first operation succeeds and the second one errors. An orphaned file means a
//! data file for which `__scanned_ranges` does not contain a corresponding range. One way we ensure
//! consistency is by detecting and deleting orphaned files when starting up a write process.
//!
//! See also: scanned-ranges-consistency

use std::collections::BTreeMap;

use futures::{StreamExt, TryStreamExt};
use metadata_db::MetadataDb;
use serde::{Deserialize, Serialize};

use crate::{multirange::MultiRange, BoxError, QueryContext, Timestamp};

pub const METADATA_KEY: &'static str = "nozzle_metadata";

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

pub async fn scanned_ranges_by_table(
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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ScannedRange {
    pub table: String,
    pub range_start: u64,
    pub range_end: u64,
    pub filename: String,
    pub created_at: Timestamp,
}

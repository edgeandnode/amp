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

use chrono::{DateTime, Utc};
use metadata_db::FileMetadataRow;
use object_store::{path::Path, ObjectMeta};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::{multirange::MultiRange, BoxError, QueryContext, Timestamp};

pub const METADATA_KEY: &'static str = "nozzle_metadata";

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ScannedRange {
    pub table: String,
    pub range_start: u64,
    pub range_end: u64,
    pub filename: String,
    pub created_at: Timestamp,
}

#[derive(Debug, Clone)]
pub struct FileMetadata {
    pub file_id: i64,
    pub file_name: String,
    pub location_id: i64,
    pub object_meta: ObjectMeta,
    pub scanned_range: ScannedRange,
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

        let scanned_range: ScannedRange = serde_json::from_value(metadata)?;

        let last_modified: DateTime<Utc> = scanned_range.created_at.try_into()?;

        let size = object_size.unwrap_or_default() as u64;

        let object_meta = ObjectMeta {
            location,
            last_modified,
            size,
            e_tag,
            version,
        };

        Ok(Self {
            file_id,
            file_name,
            location_id,
            object_meta,
            scanned_range,
        })
    }
}

pub async fn scanned_ranges_by_table(
    ctx: &QueryContext,
) -> Result<BTreeMap<String, MultiRange>, BoxError> {
    let mut scanned_ranges = BTreeMap::new();

    for table in ctx.catalog().all_tables() {
        let key = table.table_name().to_string();
        let ranges = table.ranges().await?;
        let value = MultiRange::from_ranges(ranges)?;
        scanned_ranges.insert(key, value);
    }

    Ok(scanned_ranges)
}

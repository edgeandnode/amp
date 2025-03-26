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

use std::{
    collections::BTreeMap,
    sync::{Arc, LazyLock},
};

use crate::{
    multirange::MultiRange, timestamp_type, BoxError,
    QueryContext, Timestamp,
};

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use futures::TryStreamExt;

use metadata_db::MetadataDb;
use serde::{Deserialize, Serialize};

use crate::Table;

pub const TABLE_NAME: &'static str = "__scanned_ranges";

static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| Arc::new(schema()));

pub fn table() -> Table {
    Table {
        name: TABLE_NAME.to_string(),
        schema: SCHEMA.clone(),
        network: None,
    }
}

fn schema() -> Schema {
    let table = Field::new("table", DataType::Utf8, false);
    let start = Field::new("range_start", DataType::UInt64, false);
    let end = Field::new("range_end", DataType::UInt64, false);
    let filename = Field::new("filename", DataType::Utf8, false);
    let created_at = Field::new("created_at", timestamp_type(), false);

    let fields = vec![table, start, end, filename, created_at];
    Schema::new(fields)
}

pub async fn ranges_for_table(
    ctx: &QueryContext,
    table_name: &str,
    metadata_db: Option<&MetadataDb>,
) -> Result<Vec<(u64, u64)>, BoxError> {
    match metadata_db {
        // If MetadataDb is provided, then stream the ranges directly from it (nice)
        Some(metadata_db) => Ok(metadata_db
            .stream_ranges(table_name.into())
            .try_filter_map(|(range_start, range_end)| async move {
                Ok(Some((range_start as u64, range_end as u64)))
            })
            .try_collect::<Vec<_>>()
            .await?),
        // Otherwise read the metadata from all of the parquet files (painful)
        _ => {
            ctx.catalog()
                .all_tables()
                .find(|tbl| tbl.table_name() == table_name)
                // Unwrap: table_name comes from schema.table_names()
                .unwrap()
                .ranges()
                .await
        }
    }
}

pub async fn scanned_ranges_by_table(
    ctx: &QueryContext,
    metadata_db: Option<&MetadataDb>,
) -> Result<BTreeMap<String, MultiRange>, BoxError> {
    let mut multirange_by_table = BTreeMap::default();

    for table in ctx.catalog().all_tables() {
        let table_name = table.table_name().to_string();
        let ranges = ranges_for_table(ctx, &table_name, metadata_db).await?;
        let multi_range = MultiRange::from_ranges(ranges)?;
        multirange_by_table.insert(table_name, multi_range);
    }

    Ok(multirange_by_table)
}

pub async fn filenames_for_table(
    ctx: &QueryContext,
    table_name: &str,
    metadata_db: Option<&MetadataDb>,
) -> Result<Vec<String>, BoxError> {
    match metadata_db {
        // If MetadataDb is provided, then stream the file names directly from it (nice)
        Some(metadata_db) => {
            let tbl = table_name.to_string();
            let file_names = metadata_db
                .stream_file_names(tbl)
                .try_collect::<Vec<_>>()
                .await?;
            Ok(file_names)
        }
        // Otherwise read the metadata from all of the parquet files (painful)
        _ => Ok(ctx
            .catalog()
            .all_tables()
            .find(|tbl| tbl.table_name() == table_name)
            .unwrap()
            .parquet_files(true)
            .await?
            .into_keys()
            .collect()),
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ScannedRange {
    pub table: String,
    pub range_start: u64,
    pub range_end: u64,
    pub filename: String,
    pub created_at: Timestamp,
}

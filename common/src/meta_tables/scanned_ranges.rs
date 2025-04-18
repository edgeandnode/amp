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

use std::{collections::BTreeMap, sync::Arc};

use crate::{multirange::MultiRange, BoxError, QueryContext, Timestamp};

use datafusion::parquet::file::metadata::ParquetMetaData;
use futures::TryStreamExt;

use metadata_db::{MetadataDb, TableId};
use object_store::ObjectMeta;
use serde::{Deserialize, Serialize};

pub const METADATA_KEY: &'static str = "nozzle_metadata";

pub type NozzleMetadata = (ScannedRange, ObjectMeta, usize, i64);

pub async fn ranges_for_table(
    ctx: &QueryContext,
    tbl: TableId<'_>,
) -> Result<Vec<(u64, u64)>, BoxError> {
    let catalog = ctx.catalog();
    let table_name = tbl.table.to_string();

    let table = catalog
        .all_tables()
        .await?
        .into_iter()
        .find(|table| table.table_id() == tbl)
        .ok_or(datafusion::error::DataFusionError::Plan(format!(
            "Table with ID {} not found in catalog",
            table_name
        )))?;

    let ranges = table.ranges()?.await?;

    Ok(ranges)
}

pub async fn scanned_ranges_by_table(
    ctx: &QueryContext,
) -> Result<BTreeMap<String, MultiRange>, BoxError> {
    let mut multirange_by_table = BTreeMap::default();

    for table in ctx.catalog().all_tables().await? {
        let tbl = table.table_id();

        let ranges = ranges_for_table(ctx, tbl).await?;
        let multi_range = MultiRange::from_ranges(ranges)?;
        multirange_by_table.insert(tbl.table.to_string(), multi_range);
    }

    Ok(multirange_by_table)
}

pub async fn filenames_for_table<T: AsRef<MetadataDb>>(
    ctx: &QueryContext,
    metadata_db: Option<T>,
    tbl: TableId<'_>,
) -> Result<Vec<String>, BoxError> {
    match metadata_db {
        // If MetadataDb is provided, then stream the file names directly from it (nice)
        Some(metadata_db) => {
            let file_names = metadata_db
                .as_ref()
                .stream_file_names(tbl)
                .try_collect::<Vec<_>>()
                .await?;
            Ok(file_names)
        }
        // Otherwise read the metadata from all of the parquet files (painful)
        _ => Ok(ctx
            .catalog()
            .all_tables()
            .await?
            .into_iter()
            .find(|physical_table| physical_table.table_id() == tbl)
            // TODO: method to select a table from the catalog by TableId, returning a Result<PhysicalTable>
            // to avoid this whole iteration + combinator + unwrap pattern.
            // Unwrap: the caller has already confirmed the existence of the physical table with this TableId
            .unwrap()
            .parquet_files()?
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

impl TryFrom<(Arc<ParquetMetaData>, &ObjectMeta)> for ScannedRange {
    type Error = datafusion::error::DataFusionError;

    fn try_from(
        (parquet_metadata, object_meta): (Arc<ParquetMetaData>, &ObjectMeta),
    ) -> Result<Self, Self::Error> {
        let file_metadata = parquet_metadata.file_metadata();

        let kv_metadata =
            file_metadata
                .key_value_metadata()
                .ok_or(crate::ArrowError::ParquetError(format!(
                    "No key value metadata found in parquet file: {}",
                    object_meta.location
                )))?;

        let scanned_range_key_value_pair = kv_metadata
            .iter()
            .find(|key_value| key_value.key.as_str() == METADATA_KEY)
            .ok_or(crate::ArrowError::ParquetError(format!(
                "Missing key: {} in file metadata for file {}",
                METADATA_KEY, object_meta.location
            )))?;

        let scanned_range = scanned_range_key_value_pair
            .value
            .as_ref()
            .ok_or(crate::ArrowError::ParseError(format!(
                "Unable to parse ScannedRange from key value metadata for file {}",
                object_meta.location
            )))
            .map(|scanned_range_json| serde_json::from_str::<ScannedRange>(scanned_range_json))?
            .map_err(|e| Self::Error::External(Box::new(e)))?;

        Ok(scanned_range)
    }
}

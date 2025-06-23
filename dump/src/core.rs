use std::{
    collections::{BTreeMap, BTreeSet},
    ops::RangeInclusive,
    str::FromStr,
    sync::Arc,
};

use common::{
    BlockNum, BoxError,
    catalog::physical::{Catalog, PhysicalTable},
    config::Config,
    query_context::{Error as QueryError, QueryContext},
    store::Store as DataStore,
};
use dataset_store::{DatasetKind, DatasetStore};
use futures::TryStreamExt as _;
use metadata_db::{LocationId, MetadataDb};
use object_store::ObjectMeta;

mod block_ranges;
mod raw_dataset;
mod sql_dataset;
mod tasks;

use crate::parquet_writer::ParquetWriterProperties;

/// Dumps a set of tables. All tables must belong to the same dataset.
pub async fn dump_tables(
    ctx: Ctx,
    tables: &[Arc<PhysicalTable>],
    n_jobs: u16,
    partition_size: u64,
    input_batch_size_blocks: u64,
    parquet_opts: &ParquetWriterProperties,
    range: (i64, Option<i64>),
) -> Result<(), BoxError> {
    let mut kinds = BTreeSet::new();
    for t in tables {
        kinds.insert(DatasetKind::from_str(&t.dataset().kind)?);
    }

    if kinds.iter().any(|k| k.is_raw()) {
        if !kinds.iter().all(|k| k.is_raw()) {
            return Err("Cannot mix raw and non-raw datasets in a same dump".into());
        }
        dump_raw_tables(ctx, tables, n_jobs, partition_size, parquet_opts, range).await
    } else {
        dump_user_tables(
            ctx,
            tables,
            input_batch_size_blocks,
            n_jobs,
            parquet_opts,
            range,
        )
        .await
    }
}

/// Dumps a set of raw dataset tables. All tables must belong to the same dataset.
pub async fn dump_raw_tables(
    ctx: Ctx,
    tables: &[Arc<PhysicalTable>],
    n_jobs: u16,
    partition_size: u64,
    parquet_opts: &ParquetWriterProperties,
    range: (i64, Option<i64>),
) -> Result<(), BoxError> {
    if tables.is_empty() {
        return Ok(());
    }

    // Check that all tables belong to the same dataset.
    let dataset = {
        let ds = tables[0].table().dataset();
        for table in tables {
            if table.dataset().name != ds.name {
                return Err(format!("Table {} is not in {}", table.table_ref(), ds.name).into());
            }
        }
        ds
    };

    let catalog = Catalog::new(tables.to_vec(), vec![]);
    let env = ctx.config.make_query_env()?;
    let query_ctx = Arc::new(QueryContext::for_catalog(catalog, env.clone())?);

    // Ensure consistency before starting the dump procedure.
    for table in tables {
        consistency_check(table).await?;
    }

    // Query the block ranges, we might already have some ranges if this is not the first dump run
    // for this dataset.
    let mut synced_ranges_by_table: BTreeMap<String, Option<RangeInclusive<BlockNum>>> =
        Default::default();
    for table in tables {
        synced_ranges_by_table.insert(table.table_name().to_string(), table.synced_range().await?);
    }

    for (table_name, range) in &synced_ranges_by_table {
        let Some(range) = range else {
            continue;
        };
        tracing::info!(
            "table `{}` has scanned block range [{}-{}]",
            table_name,
            range.start(),
            range.end(),
        );
    }

    let kind = DatasetKind::from_str(&dataset.kind)?;
    match kind {
        DatasetKind::EvmRpc | DatasetKind::Firehose | DatasetKind::Substreams => {
            raw_dataset::dump(
                ctx,
                n_jobs,
                query_ctx,
                &dataset.name,
                synced_ranges_by_table,
                partition_size,
                parquet_opts,
                range,
            )
            .await?;
        }
        DatasetKind::Sql | DatasetKind::Manifest => {
            return Err(format!(
                "Attempted to dump dataset `{}` of kind `{}` as raw dataset",
                dataset.name, kind,
            )
            .into());
        }
    }

    tracing::info!("dump of dataset {} completed successfully", dataset.name);

    Ok(())
}

pub async fn dump_user_tables(
    ctx: Ctx,
    tables: &[Arc<PhysicalTable>],
    input_batch_size_blocks: u64,
    n_jobs: u16,
    parquet_opts: &ParquetWriterProperties,
    range: (i64, Option<i64>),
) -> Result<(), BoxError> {
    if n_jobs > 1 {
        tracing::warn!("n_jobs > 1 has no effect for SQL datasets");
    }

    let env = ctx.config.make_query_env()?;

    for table in tables {
        consistency_check(table).await?;

        let dataset = table.table().dataset();
        let kind = DatasetKind::from_str(&dataset.kind)?;

        let dataset = match kind {
            DatasetKind::Sql => ctx.dataset_store.load_sql_dataset(&dataset.name).await?,
            DatasetKind::Manifest => {
                ctx.dataset_store
                    .load_manifest_dataset(&dataset.name)
                    .await?
            }
            _ => {
                return Err(format!(
                    "Unsupported dataset kind {:?} for table {}",
                    kind,
                    table.table_ref()
                )
                .into());
            }
        };

        sql_dataset::dump_table(
            ctx.clone(),
            dataset,
            &env,
            table.clone(),
            parquet_opts,
            input_batch_size_blocks,
            range,
        )
        .await?;

        tracing::info!("dump of `{}` completed successfully", table.table_name());
    }

    Ok(())
}

/// Dataset dump context
#[derive(Clone)]
pub struct Ctx {
    pub config: Arc<Config>,
    pub metadata_db: Arc<MetadataDb>,
    pub dataset_store: Arc<DatasetStore>,
    pub data_store: Arc<DataStore>,
}

/// This will check and fix consistency issues when possible. When fixing is not possible, it will
/// return a `CorruptedDataset` error.
///
/// ## List of checks
///
/// Check: All files in the data store are accounted for in the metadata DB.
/// On fail: Fix by deleting orphaned files to restore consistency.
///
/// Check: All files in the table exist in the data store.
/// On fail: Return a `CorruptedDataset` error.
///
/// Check: metadata entries do not contain overlapping ranges.
/// On fail: Return a `CorruptedDataset` error.
async fn consistency_check(table: &PhysicalTable) -> Result<(), ConsistencyCheckError> {
    // See also: metadata-consistency

    let location_id = table.location_id();

    // Check that bock ranges do not contain overlapping ranges.
    let ranges: Vec<(BlockNum, BlockNum)> = table
        .files()
        .await
        .map_err(|err| ConsistencyCheckError::CorruptedTable(location_id, err))?
        .into_iter()
        .map(|m| m.parquet_meta.ranges[0].numbers.clone().into_inner())
        .collect();
    for window in ranges.windows(2) {
        let ((a, b), (c, d)) = (window[0], window[1]);
        if !((a <= b) && (b <= c) && (c <= d)) {
            return Err(ConsistencyCheckError::CorruptedTable(
                location_id,
                format!("overlapping block ranges: [{a}-{b}] and [{c}-{d}]").into(),
            ));
        }
    }

    let registered_files: BTreeSet<String> = table
        .files()
        .await
        .map_err(|err| ConsistencyCheckError::CorruptedTable(location_id, err))?
        .into_iter()
        .map(|m| m.file_name)
        .collect();

    let store = table.object_store();
    let path = table.path();

    let stored_files: BTreeMap<String, ObjectMeta> = store
        .list(Some(table.path()))
        .try_collect::<Vec<ObjectMeta>>()
        .await
        .map_err(|err| ConsistencyCheckError::CorruptedTable(location_id, err.into()))?
        .into_iter()
        .filter_map(|object| Some((object.location.filename()?.to_string(), object)))
        .collect();

    for (filename, object_meta) in &stored_files {
        if !registered_files.contains(filename) {
            // This file was written by a dump job but it is not present in the metadata DB,
            // so it is an orphaned file. Delete it.
            tracing::warn!("Deleting orphaned file: {}", object_meta.location);
            store.delete(&object_meta.location).await?;
        }
    }

    // Check for files in the metadata DB that do not exist in the store.
    for filename in registered_files {
        if !stored_files.contains_key(&filename) {
            let err = format!(
                "file `{path}/{filename}` is registered in metadata DB but is not in the data store"
            )
            .into();
            return Err(ConsistencyCheckError::CorruptedTable(location_id, err));
        }
    }

    Ok(())
}

/// Error type for consistency checks
#[derive(Debug, thiserror::Error)]
#[error("consistency check error: {0}")]
enum ConsistencyCheckError {
    #[error("internal query error: {0}")]
    QueryError(#[from] QueryError),

    #[error("object store error: {0}")]
    ObjectStoreError(#[from] object_store::Error),

    #[error("table {0} is corrupted: {1}")]
    CorruptedTable(LocationId, BoxError),
}

pub fn missing_block_ranges(
    synced: RangeInclusive<BlockNum>,
    desired: RangeInclusive<BlockNum>,
) -> Vec<RangeInclusive<BlockNum>> {
    // no overlap
    if (synced.end() < desired.start()) || (synced.start() > desired.end()) {
        return vec![desired];
    }
    // desired is subset of synced
    if (synced.start() <= desired.start()) && (synced.end() >= desired.end()) {
        return vec![];
    }
    // partial overlap
    let mut result = Vec::new();
    if desired.start() < synced.start() {
        result.push(*desired.start()..=(*synced.start() - 1));
    }
    if desired.end() > synced.end() {
        result.push((*synced.end() + 1)..=*desired.end());
    }
    result
}

#[cfg(test)]
mod test {
    #[test]
    fn missing_block_ranges() {
        // no overlap, desired before synced
        assert_eq!(super::missing_block_ranges(10..=20, 0..=5), vec![0..=5]);
        // no overlap, desired after synced
        assert_eq!(super::missing_block_ranges(0..=5, 10..=20), vec![10..=20]);
        // desired is subset of synced
        assert_eq!(super::missing_block_ranges(0..=10, 2..=8), vec![]);
        // desired is same as synced
        assert_eq!(super::missing_block_ranges(0..=10, 0..=10), vec![]);
        // synced starts before desired, ends with desired
        assert_eq!(super::missing_block_ranges(0..=10, 0..=10), vec![]);
        // synced starts with desired, ends after desired
        assert_eq!(super::missing_block_ranges(0..=10, 0..=10), vec![]);
        // partial overlap, desired starts before synced
        assert_eq!(super::missing_block_ranges(5..=10, 0..=7), vec![0..=4]);
        // partial overlap, desired ends after synced
        assert_eq!(super::missing_block_ranges(0..=5, 3..=10), vec![6..=10]);
        // partial overlap, desired surrounds synced
        assert_eq!(
            super::missing_block_ranges(5..=10, 0..=15),
            vec![0..=4, 11..=15]
        );
        // desired starts same as synced, ends after synced
        assert_eq!(super::missing_block_ranges(0..=5, 0..=10), vec![6..=10]);
        // desired starts before synced, ends same as synced
        assert_eq!(super::missing_block_ranges(5..=10, 0..=10), vec![0..=4]);
        // adjacent ranges (desired just before synced)
        assert_eq!(super::missing_block_ranges(5..=10, 0..=4), vec![0..=4]);
        // adjacent ranges (desired just after synced)
        assert_eq!(super::missing_block_ranges(0..=5, 6..=10), vec![6..=10]);
        // single block ranges
        assert_eq!(super::missing_block_ranges(0..=0, 0..=0), vec![]);
        assert_eq!(super::missing_block_ranges(0..=0, 1..=1), vec![1..=1]);
        assert_eq!(super::missing_block_ranges(1..=1, 0..=0), vec![0..=0]);
        assert_eq!(super::missing_block_ranges(0..=2, 0..=3), vec![3..=3]);
        assert_eq!(super::missing_block_ranges(1..=3, 0..=3), vec![0..=0]);
        assert_eq!(super::missing_block_ranges(0..=2, 0..=3), vec![3..=3]);
    }
}

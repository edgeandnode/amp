pub mod job;
mod job_partition;
mod metrics; // unused for now
mod parquet_writer;
pub mod worker;

use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use common::{
    catalog::physical::{Catalog, PhysicalDataset, PhysicalTable},
    config::Config,
    multirange::MultiRange,
    parquet,
    parquet::basic::ZstdLevel,
    query_context::{Error as CoreError, QueryContext},
    BlockNum, BlockStreamer as _, BoxError,
};
use datafusion::execution::runtime_env::RuntimeEnv;
use dataset_store::{
    sql_datasets::{is_incremental, max_end_block, SqlDataset},
    DatasetKind, DatasetStore,
};
use futures::{future::try_join_all, TryFutureExt as _, TryStreamExt};
use job_partition::JobPartition;
use object_store::ObjectMeta;
use parquet::{basic::Compression, file::properties::WriterProperties as ParquetWriterProperties};
use parquet_writer::{insert_scanned_range, ParquetFileWriter};
use thiserror::Error;
use tracing::{info, instrument, warn};

pub async fn dump_dataset(
    dataset: &PhysicalDataset,
    dataset_store: &Arc<DatasetStore>,
    config: &Config,
    n_jobs: u16,
    partition_size: u64,
    input_batch_size_blocks: u64,
    parquet_opts: &ParquetWriterProperties,
    start: i64,
    end_block: Option<i64>,
) -> Result<(), BoxError> {
    use common::meta_tables::scanned_ranges::scanned_ranges_by_table;

    let catalog = Catalog::new(vec![dataset.clone()]);
    let env = Arc::new(config.make_runtime_env()?);
    let ctx = Arc::new(QueryContext::for_catalog(catalog, env.clone())?);

    // Ensure consistency before starting the dump procedure.
    consistency_check(dataset).await?;

    // Query the scanned ranges, we might already have some ranges if this is not the first dump run
    // for this dataset.
    let scanned_ranges_by_table = scanned_ranges_by_table(&ctx).await?;
    for (table_name, multirange) in &scanned_ranges_by_table {
        if multirange.total_len() == 0 {
            continue;
        }

        info!(
            "table `{}` has scanned {} blocks in the ranges: {}",
            table_name,
            multirange.total_len(),
            multirange,
        );
    }

    let kind = DatasetKind::from_str(dataset.kind())?;
    match kind {
        DatasetKind::EvmRpc | DatasetKind::Firehose | DatasetKind::Substreams => {
            dump_raw_dataset(
                n_jobs,
                ctx,
                &dataset.name(),
                dataset_store,
                scanned_ranges_by_table,
                partition_size,
                parquet_opts,
                start,
                end_block,
            )
            .await?;
        }
        DatasetKind::Sql | DatasetKind::Manifest => {
            if n_jobs > 1 {
                info!("n_jobs > 1 has no effect for SQL datasets");
            }

            let dataset = match kind {
                DatasetKind::Sql => dataset_store.load_sql_dataset(dataset.name()).await?,
                DatasetKind::Manifest => {
                    dataset_store.load_manifest_dataset(dataset.name()).await?
                }
                _ => unreachable!(),
            };

            dump_sql_dataset(
                ctx,
                dataset,
                config.data_store.clone(),
                dataset_store,
                &env,
                scanned_ranges_by_table,
                parquet_opts,
                start,
                end_block,
                input_batch_size_blocks,
            )
            .await?;
        }
    }

    info!("dump of dataset {} completed successfully", dataset.name());

    Ok(())
}

async fn dump_raw_dataset(
    n_jobs: u16,
    ctx: Arc<QueryContext>,
    dataset_name: &str,
    dataset_store: &DatasetStore,
    scanned_ranges_by_table: BTreeMap<String, MultiRange>,
    partition_size: u64,
    parquet_opts: &ParquetWriterProperties,
    start: i64,
    end: Option<i64>,
) -> Result<(), BoxError> {
    let mut client = dataset_store.load_client(dataset_name).await?;

    let (start, end) = match (start, end) {
        (start, Some(end)) if start >= 0 && end >= 0 => (start as BlockNum, end as BlockNum),
        _ => {
            let latest_block = client.latest_block(true).await?;
            resolve_relative_block_range(start, end, latest_block)?
        }
    };

    // This is the intersection of the `__scanned_ranges` for all tables. That is, a range is only
    // considered scanned if it is scanned for all tables.
    let scanned_ranges = {
        let mut scanned_ranges = scanned_ranges_by_table.clone().into_iter().map(|(_, r)| r);
        let first = scanned_ranges.next().ok_or("no tables")?;
        let intersection = scanned_ranges.fold(first, |acc, r| acc.intersection(&r));
        intersection
    };

    // Find the ranges of blocks that have not been scanned yet for at least one table.
    let ranges = scanned_ranges.complement(start, end);
    info!("dumping dataset {dataset_name} for ranges {ranges}");

    if ranges.total_len() == 0 {
        info!("no blocks to dump for {dataset_name}");
        return Ok(());
    }

    // Split them across the target number of jobs as to balance the number of blocks per job.
    let multiranges = ranges.split_and_partition(n_jobs as u64, 2000);

    let jobs = multiranges.into_iter().enumerate().map(|(i, multirange)| {
        Arc::new(JobPartition {
            dataset_ctx: ctx.clone(),
            metadata_db: dataset_store.metadata_db.clone(),
            block_streamer: client.clone(),
            multirange,
            id: i as u32,
            partition_size,
            parquet_opts: parquet_opts.clone(),
            scanned_ranges_by_table: scanned_ranges_by_table.clone(),
        })
    });

    // Spawn the jobs so they run in parallel, terminating early if any job fails.
    let mut join_handles = vec![];
    for job in jobs {
        let handle = tokio::spawn(job.run());

        // Stagger the start of each job by 1 second in an attempt to avoid client rate limits.
        tokio::time::sleep(Duration::from_secs(1)).await;

        join_handles.push(async { handle.err_into().await.and_then(|x| x) });
    }

    try_join_all(join_handles).await?;

    Ok(())
}

#[instrument(skip_all, err, fields(dataset = %dataset.name()))]
async fn dump_sql_dataset(
    dst_ctx: Arc<QueryContext>,
    dataset: SqlDataset,
    data_store: Arc<common::Store>,
    dataset_store: &Arc<DatasetStore>,
    env: &Arc<RuntimeEnv>,
    scanned_ranges_by_table: BTreeMap<String, MultiRange>,
    parquet_opts: &ParquetWriterProperties,
    start: i64,
    end: Option<i64>,
    input_batch_size_blocks: u64,
) -> Result<(), BoxError> {
    let physical_dataset = &dst_ctx.catalog().datasets()[0].clone();
    let mut join_handles = vec![];
    let dataset_name = dataset.name().to_string();

    for (table, query) in dataset.queries {
        let dataset_store = dataset_store.clone();
        let env = env.clone();
        let data_store = data_store.clone();
        let physical_dataset = physical_dataset.clone();
        let scanned_ranges_by_table = scanned_ranges_by_table.clone();
        let parquet_opts = parquet_opts.clone();
        let dataset_name = dataset_name.clone();

        let handle = tokio::spawn(async move {
            let (start, end) = match (start, end) {
                (start, Some(end)) if start >= 0 && end >= 0 => {
                    (start as BlockNum, end as BlockNum)
                }
                _ => {
                    match max_end_block(&query, dataset_store.clone(), env.clone()).await? {
                        Some(max_end_block) => {
                            resolve_relative_block_range(start, end, max_end_block)?
                        }
                        None => {
                            // If the dependencies have synced nothing, we have nothing to do.
                            warn!("no blocks to dump for {table}, dependencies are empty");
                            return Ok::<(), BoxError>(());
                        }
                    }
                }
            };

            let physical_table = {
                let mut tables = physical_dataset.tables();
                tables.find(|t| t.table_name() == table).unwrap()
            };

            let src_ctx = dataset_store
                .clone()
                .ctx_for_sql(&query, env.clone())
                .await?;
            let plan = src_ctx.plan_sql(query.clone()).await?;
            let is_incr = is_incremental(&plan)?;

            if is_incr {
                let ranges_to_scan = scanned_ranges_by_table[&table].complement(start, end);
                for (start, end) in ranges_to_scan.ranges {
                    let mut start = start;
                    while start <= end {
                        let batch_end = std::cmp::min(start + input_batch_size_blocks - 1, end);
                        info!(
                            "dumping {} between blocks {start} and {batch_end}",
                            physical_table.table_ref()
                        );

                        dump_sql_query(
                            &dataset_store,
                            &query,
                            &env,
                            start,
                            batch_end,
                            physical_table,
                            &parquet_opts,
                        )
                        .await?;
                        start = batch_end + 1;
                    }
                }
            } else {
                let physical_table = PhysicalTable::next_revision(
                    physical_table.table(),
                    &data_store,
                    &dataset_name,
                    dataset_store.metadata_db.clone(),
                )
                .await?;
                info!(
                    "dumping entire {} to {}",
                    physical_table.table_ref(),
                    physical_table.url()
                );
                dump_sql_query(
                    &dataset_store,
                    &query,
                    &env,
                    start,
                    end,
                    &physical_table,
                    &parquet_opts,
                )
                .await?;
            }

            Ok::<(), BoxError>(())
        });

        join_handles.push(async { handle.err_into().await.and_then(|x| x) });
    }

    try_join_all(join_handles).await?;

    Ok(())
}

#[instrument(skip_all, err)]
async fn dump_sql_query(
    dataset_store: &Arc<DatasetStore>,
    query: &datafusion::sql::parser::Statement,
    env: &Arc<RuntimeEnv>,
    start: BlockNum,
    end: BlockNum,
    physical_table: &common::catalog::physical::PhysicalTable,
    parquet_opts: &ParquetWriterProperties,
) -> Result<(), BoxError> {
    use dataset_store::sql_datasets::execute_query_for_range;

    let store = dataset_store.clone();
    let mut stream = execute_query_for_range(query.clone(), store, env.clone(), start, end).await?;
    let mut writer = ParquetFileWriter::new(physical_table.clone(), parquet_opts.clone(), start)?;

    while let Some(batch) = stream.try_next().await? {
        writer.write(&batch).await?;
    }
    let (scanned_range, object_meta) = writer.close(end).await?;
    insert_scanned_range(
        scanned_range,
        object_meta,
        dataset_store.metadata_db.clone(),
        physical_table.location_id(),
    )
    .await
}

pub fn default_partition_size() -> u64 {
    4096 * 1024 * 1024 // 4 GB
}

pub fn default_input_batch_size_blocks() -> u64 {
    100_000
}

pub fn default_parquet_opts() -> ParquetWriterProperties {
    parquet_opts(Compression::ZSTD(ZstdLevel::try_new(1).unwrap()), true)
}

pub fn parquet_opts(compression: Compression, bloom_filters: bool) -> ParquetWriterProperties {
    // We have not done our own benchmarking, but the default 1_000_000 value for this adds about a
    // megabyte of storage per column, per row group. This analysis by InfluxData suggests that
    // smaller NDV values may be equally effective:
    // https://www.influxdata.com/blog/using-parquets-bloom-filters/
    let bloom_filter_ndv = 10_000;

    // For DataFusion defaults, see `ParquetOptions` here:
    // https://github.com/apache/arrow-datafusion/blob/main/datafusion/common/src/config.rs
    //
    // Note: We could set `sorting_columns` for columns like `block_num` and `ordinal`. However,
    // Datafusion doesn't actually read that metadata info anywhere and just reiles on the
    // `file_sort_order` set on the reader configuration.
    ParquetWriterProperties::builder()
        .set_compression(compression)
        .set_bloom_filter_ndv(bloom_filter_ndv)
        .set_bloom_filter_enabled(bloom_filters)
        .build()
}

#[derive(Error, Debug)]
#[error("consistency check error: {0}")]
enum ConsistencyCheckError {
    #[error("internal query error: {0}")]
    QueryError(#[from] CoreError),

    #[error("object store error: {0}")]
    ObjectStoreError(#[from] object_store::Error),

    #[error("dataset {0} is corrupted: {1}")]
    CorruptedDataset(String, BoxError),
}

/// This will check and fix consistency issues when possible. When fixing is not possible, it will
/// return a `CorruptedDataset` error.
///
/// ## List of checks
///
/// Check: All files in the data store are accounted for in `__scanned_ranges`.
/// On fail: Fix by deleting orphaned files to restore consistency.
///
/// Check: All files in `__scanned_ranges` exist in the data store.
/// On fail: Return a `CorruptedDataset` error.
///
/// Check: `__scanned_ranges` does not contain overlapping ranges.
/// On fail: Return a `CorruptedDataset` error.
async fn consistency_check(
    physical_dataset: &PhysicalDataset,
) -> Result<(), ConsistencyCheckError> {
    // See also: scanned-ranges-consistency

    use ConsistencyCheckError::CorruptedDataset;

    for table in physical_dataset.tables() {
        let dataset_name = table.catalog_schema().to_string();
        let tbl = table.table_id();
        // Check that `__scanned_ranges` does not contain overlapping ranges.
        {
            let ranges = table.ranges().await.map_err(|err| {
                ConsistencyCheckError::CorruptedDataset(
                    table.catalog_schema().to_string(),
                    err.into(),
                )
            })?;
            if let Err(e) = MultiRange::from_ranges(ranges) {
                return Err(CorruptedDataset(dataset_name, e.into()));
            }
        }

        let registered_files = {
            let f = table.file_names().await.map_err(|err| {
                ConsistencyCheckError::CorruptedDataset(tbl.dataset.to_string(), err.into())
            })?;
            BTreeSet::from_iter(f.into_iter())
        };

        let store = table.object_store();
        let path = table.path();

        // Collect all stored files whose filename matches `is_dump_file`.
        let stored_files: BTreeMap<String, ObjectMeta> =
            table.parquet_files().await.map_err(|e| {
                ConsistencyCheckError::CorruptedDataset(table.table_id().dataset.to_string(), e)
            })?;

        for (filename, object_meta) in &stored_files {
            if !registered_files.contains(filename) {
                // This file was written by a dump job but it is not present in `__scanned_ranges`,
                // so it is an orphaned file. Delete it.
                warn!("Deleting orphaned file: {}", object_meta.location);
                store.delete(&object_meta.location).await?;
            }
        }

        // Check for files in `__scanned_ranges` that do not exist in the store.
        for filename in registered_files {
            if !stored_files.contains_key(&filename) {
                let err =
                    format!("file `{path}/{filename}` is registered in `scanned_ranges` but is not in the data store")
                        .into();
                return Err(ConsistencyCheckError::CorruptedDataset(dataset_name, err));
            }
        }
    }
    Ok(())
}

fn resolve_relative_block_range(
    start: i64,
    end: Option<i64>,
    latest_block: BlockNum,
) -> Result<(BlockNum, BlockNum), BoxError> {
    if start >= 0 {
        if let Some(end) = end {
            if end > 0 {
                return Ok((start as BlockNum, end as BlockNum));
            }
        }
    }

    let start_block = if start >= 0 {
        start
    } else {
        latest_block as i64 + start // Using + because start is negative
    };

    if start_block < 0 {
        return Err(format!("start block {start_block} is invalid").into());
    }

    let end_block = match end {
        // Absolute block number
        Some(e) if e > 0 => e as BlockNum,

        // Relative to latest block
        Some(e) => {
            let end = latest_block as i64 + e; // Using + because e is negative
            if end < start_block {
                return Err(format!(
                    "end_block {end} must be greater than or equal to start_block {start_block}"
                )
                .into());
            }
            end as BlockNum
        }

        // Default to latest block
        None => latest_block,
    };

    Ok((start_block as BlockNum, end_block))
}

#[cfg(test)]
mod tests;

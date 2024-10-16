mod job;
mod metrics; // unused for now
mod parquet_writer;

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use common::catalog::physical::Catalog;
use common::catalog::physical::PhysicalDataset;
use common::config::Config;
use common::meta_tables::scanned_ranges;
use common::multirange::MultiRange;
use common::parquet;
use common::query_context::Error as CoreError;
use common::query_context::QueryContext;
use common::BlockNum;
use common::BlockStreamer as _;
use common::BoxError;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::sql::TableReference;
use dataset_store::sql_datasets::max_end_block;
use dataset_store::DatasetKind;
use dataset_store::DatasetStore;
use futures::future::try_join_all;
use futures::TryFutureExt as _;
use futures::TryStreamExt;
use job::Job;
use log::info;
use log::warn;
use object_store::path::Path;
use object_store::ObjectMeta;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties as ParquetWriterProperties;
use parquet_writer::ParquetFileWriter;
use thiserror::Error;

pub async fn dump_dataset(
    dataset_name: &str,
    dataset_store: &Arc<DatasetStore>,
    config: &Config,
    env: &Arc<RuntimeEnv>,
    n_jobs: u16,
    partition_size: u64,
    parquet_opts: &ParquetWriterProperties,
    start: u64,
    end_block: Option<u64>,
) -> Result<(), BoxError> {
    use common::meta_tables::scanned_ranges::scanned_ranges_by_table;

    let dataset = dataset_store.load_dataset(&dataset_name).await?;
    let catalog = Catalog::for_dataset(&dataset, config.data_store.clone())?;
    let physical_dataset = catalog.datasets()[0].clone();
    let ctx = Arc::new(QueryContext::for_catalog(catalog, env.clone())?);

    // Ensure consistency before starting the dump procedure.
    consistency_check(&physical_dataset, &ctx).await?;

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

    match DatasetKind::from_str(&dataset.kind)? {
        DatasetKind::EvmRpc | DatasetKind::Firehose | DatasetKind::Substreams => {
            run_block_stream_jobs(
                n_jobs,
                ctx,
                dataset_name,
                dataset_store,
                scanned_ranges_by_table,
                partition_size,
                parquet_opts,
                start,
                end_block,
            )
            .await?;
        }
        DatasetKind::Sql => {
            if n_jobs > 1 {
                warn!("n_jobs > 1 has no effect for SQL datasets");
            }

            dump_sql_dataset(
                ctx,
                &dataset_name,
                dataset_store,
                env,
                scanned_ranges_by_table,
                parquet_opts,
                start,
                end_block,
            )
            .await?;
        }
    }

    info!("dump of dataset {dataset_name} completed successfully");

    Ok(())
}

async fn run_block_stream_jobs(
    n_jobs: u16,
    ctx: Arc<QueryContext>,
    dataset_name: &str,
    dataset_store: &DatasetStore,
    scanned_ranges_by_table: BTreeMap<String, MultiRange>,
    partition_size: u64,
    parquet_opts: &ParquetWriterProperties,
    start: BlockNum,
    end: Option<BlockNum>,
) -> Result<(), BoxError> {
    let mut client = dataset_store.load_client(dataset_name).await?;

    let end = match end {
        Some(end) => end,
        None => client.recent_final_block_num().await?,
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
        Arc::new(Job {
            dataset_ctx: ctx.clone(),
            block_streamer: client.clone(),
            multirange,
            job_id: i as u32,
            partition_size,
            parquet_opts: parquet_opts.clone(),
            scanned_ranges_by_table: scanned_ranges_by_table.clone(),
        })
    });

    // Spawn the jobs so they run in parallel, terminating early if any job fails.
    let mut join_handles = vec![];
    for job in jobs {
        let handle = tokio::spawn(job::run(job));

        // Stagger the start of each job by 1 second in an attempt to avoid client rate limits.
        tokio::time::sleep(Duration::from_secs(1)).await;

        join_handles.push(async { handle.err_into().await.and_then(|x| x) });
    }

    try_join_all(join_handles).await?;

    Ok(())
}

async fn dump_sql_dataset(
    ctx: Arc<QueryContext>,
    dataset_name: &str,
    dataset_store: &Arc<DatasetStore>,
    env: &Arc<RuntimeEnv>,
    scanned_ranges_by_table: BTreeMap<String, MultiRange>,
    parquet_opts: &ParquetWriterProperties,
    start: BlockNum,
    end: Option<BlockNum>,
) -> Result<(), BoxError> {
    use dataset_store::sql_datasets::execute_query_for_range;

    let dataset = dataset_store.load_sql_dataset(dataset_name).await?;
    let physical_dataset = &ctx.catalog().datasets()[0].clone();
    let data_store = physical_dataset.data_store();

    for (table, query) in dataset.queries {
        let end = match end {
            Some(end) => end,
            None => {
                match max_end_block(&query, dataset_store.clone(), env.clone()).await? {
                    Some(end) => end,
                    None => {
                        // If the dependencies have synced nothing, we have nothing to do.
                        continue;
                    }
                }
            }
        };

        let physical_table = {
            let mut tables = physical_dataset.tables();
            tables.find(|t| t.table_name() == table).unwrap()
        };
        let ranges_to_scan = scanned_ranges_by_table[&table].complement(start, end);

        for (start, end) in ranges_to_scan.ranges {
            info!(
                "dumping {} between blocks {start} and {end}",
                physical_table.table_ref()
            );

            let store = dataset_store.clone();
            let mut stream =
                execute_query_for_range(query.clone(), store, env.clone(), Some(start), end)
                    .await?;
            let mut writer = ParquetFileWriter::new(
                &data_store,
                physical_table.clone(),
                parquet_opts.clone(),
                start,
            )?;
            while let Some(batch) = stream.try_next().await? {
                writer.write(&batch).await?;
            }
            let scanned_range = writer.close(end).await?.as_record_batch();
            let table_ref = TableReference::partial(dataset_name, scanned_ranges::TABLE_NAME);
            ctx.meta_insert_into(table_ref, scanned_range).await?;
        }
    }

    Ok(())
}

pub fn parquet_opts(compression: Compression, bloom_filters: bool) -> ParquetWriterProperties {
    // For DataFusion defaults, see `ParquetOptions` here:
    // https://github.com/apache/arrow-datafusion/blob/main/datafusion/common/src/config.rs
    //
    // Note: We could set `sorting_columns` for columns like `block_num` and `ordinal`. However,
    // Datafusion doesn't actually read that metadata info anywhere and just reiles on the
    // `file_sort_order` set on the reader configuration.
    ParquetWriterProperties::builder()
        .set_compression(compression)
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
    ctx: &QueryContext,
) -> Result<(), ConsistencyCheckError> {
    // See also: scanned-ranges-consistency

    use scanned_ranges::{filenames_for_table, ranges_for_table};
    use ConsistencyCheckError::CorruptedDataset;

    for table in physical_dataset.tables() {
        let dataset_name = table.catalog_schema().to_string();

        // Check that `__scanned_ranges` does not contain overlapping ranges.
        {
            let ranges = ranges_for_table(ctx, table.catalog_schema(), table.table_name()).await?;
            if let Err(e) = MultiRange::from_ranges(ranges) {
                return Err(CorruptedDataset(dataset_name, e.into()));
            }
        }

        let registered_files = {
            let f = filenames_for_table(&ctx, table.catalog_schema(), table.table_name()).await?;
            BTreeSet::from_iter(f.into_iter())
        };

        let store = physical_dataset.data_store().prefixed_store();

        // Unwrap: The table path is syntatically valid.
        let path = Path::parse(table.path()).unwrap();

        // Check that this is a file written by a dump job, with name in the format:
        // "<block_num>.parquet".
        let is_dump_file = |filename: &str| {
            filename.ends_with(".parquet")
                && filename.trim_end_matches(".parquet").parse::<u64>().is_ok()
        };

        // Collect all stored files whose filename matches `is_dump_file`.
        let stored_files: BTreeMap<String, ObjectMeta> = store
            .list(Some(&path))
            .try_collect::<Vec<ObjectMeta>>()
            .await?
            .into_iter()
            // Unwrap: A full object path always has a filename.
            .map(|f| (f.location.filename().unwrap().to_string(), f))
            .filter(|(filename, _)| is_dump_file(filename))
            .collect();

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
                    format!("file `{path}` is registered in `__scanned_ranges` but is not in the data store")
                        .into();
                return Err(ConsistencyCheckError::CorruptedDataset(dataset_name, err));
            }
        }
    }
    Ok(())
}

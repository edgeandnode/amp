mod job;
mod metrics; // unused for now
mod parquet_writer;

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use common::arrow::array::AsArray as _;
use common::arrow::datatypes::UInt64Type;
use common::catalog::physical::Catalog;
use common::catalog::physical::PhysicalDataset;
use common::config::Config;
use common::meta_tables::scanned_ranges;
use common::meta_tables::scanned_ranges::filenames_for_table;
use common::multirange::MultiRange;
use common::parquet;
use common::query_context::Error as CoreError;
use common::query_context::QueryContext;
use common::tracing;
use common::BlockNum;
use common::BlockStreamer as _;
use common::BoxError;
use common::BLOCK_NUM;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::sql::TableReference;
use dataset_store::sql_datasets::max_end_block;
use dataset_store::DatasetKind;
use dataset_store::DatasetStore;
use futures::future::try_join_all;
use futures::StreamExt as _;
use futures::TryFutureExt as _;
use futures::TryStreamExt;
use job::Job;
use log::info;
use log::warn;
use object_store::path::Path;
use object_store::ObjectMeta;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties as ParquetWriterProperties;
use parquet_writer::ParquetWriter;
use thiserror::Error;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

/// A tool for dumping firehose or substreams data to parquet files.
#[derive(Parser, Debug)]
#[command(name = "firehose-dump")]
struct Args {
    /// Path to a config file. See README for details on the format.
    #[arg(long, env = "NOZZLE_CONFIG")]
    config: String,

    /// The name of the dataset to dump. This will be looked up in the dataset definiton directory.
    /// Will also be used as a subdirectory in the output path, `<data_dir>/<dataset>`.
    #[arg(long, env = "DUMP_DATASET")]
    dataset: String,

    /// The block number to start from, inclusive. If ommited, defaults to `0`. Note that `dump` is
    /// smart about keeping track of what blocks have already been dumped, so you only need to set
    /// this if you really don't want the data before this block.
    #[arg(long, short, default_value = "0", env = "DUMP_START_BLOCK")]
    start: u64,

    /// The block number to end at, inclusive. If starts with "+" then relative to `start`.
    #[arg(long, short, env = "DUMP_END_BLOCK")]
    end_block: Option<String>,

    /// How many parallel extractor jobs to run. Defaults to 1. Each job will be responsible for an
    /// equal number of blocks. Example: If start = 0, end = 10_000_000 and n_jobs = 10, then each
    /// job will be responsible for a contiguous section of 1 million blocks.
    #[arg(long, short = 'j', default_value = "1", env = "DUMP_N_JOBS")]
    n_jobs: u16,

    /// The size of each partition in MB. Once the size is reached, a new part file is created. This
    /// is based on the estimated in-memory size of the data. The actual on-disk file size will vary,
    /// but will correlate with this value. Defaults to 4 GB.
    #[arg(long, default_value = "4096", env = "DUMP_PARTITION_SIZE_MB")]
    partition_size_mb: u64,

    /// Whether to disable compression when writing parquet files. Defaults to false.
    #[arg(long, env = "DUMP_DISABLE_COMPRESSION")]
    disable_compression: bool,
}

#[tokio::main]
async fn main() {
    match main_inner().await {
        Ok(()) => {}
        Err(e) => {
            // Manually print the error so we can control the format.
            eprintln!("Exiting with error: {e}");
            std::process::exit(1);
        }
    }
}

async fn main_inner() -> Result<(), BoxError> {
    tracing::register_logger();

    let args = Args::parse();
    let Args {
        config: config_path,
        start,
        end_block,
        n_jobs,
        partition_size_mb,
        disable_compression,
        dataset: dataset_name,
    } = args;

    let config = Arc::new(Config::load(config_path)?);
    let dataset_store = DatasetStore::new(config.clone());
    let partition_size = partition_size_mb * 1024 * 1024;
    let compression = if disable_compression {
        parquet::basic::Compression::UNCOMPRESSED
    } else {
        Compression::ZSTD(ZstdLevel::try_new(1).unwrap())
    };
    let parquet_opts = parquet_opts(compression);

    let end_block = end_block.map(|e| resolve_end_block(start, e)).transpose()?;

    let dataset = dataset_store.load_dataset(&dataset_name).await?;

    let env = Arc::new(config.make_runtime_env()?);
    let catalog = Catalog::for_dataset(&dataset, config.data_store.clone())?;
    let physical_dataset = catalog.datasets()[0].clone();
    let ctx = Arc::new(QueryContext::for_catalog(catalog, env.clone())?);

    // Ensure consistency before starting the dump procedure.
    delete_orphaned_files(&physical_dataset, &ctx).await?;

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
        DatasetKind::Firehose | DatasetKind::Substreams => {
            run_block_stream_jobs(
                n_jobs,
                ctx,
                &dataset_name,
                &dataset_store,
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
                return Err("n_jobs > 1 is not supported for SQL datasets".into());
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
    parquet_opts: ParquetWriterProperties,
    start: BlockNum,
    end: Option<BlockNum>,
) -> Result<(), BoxError> {
    let mut client = dataset_store.load_client(dataset_name).await?;

    let end = match end {
        Some(end) => end,
        None => client.recent_final_block_num().await?,
    };

    info!("dumping dataset {dataset_name} from {start} to {end}");

    // This is the intersection of the `__scanned_ranges` for all tables. That is, a range is only
    // considered scanned if it is scanned for all tables.
    let scanned_ranges = {
        let mut scanned_ranges = scanned_ranges_by_table.into_iter().map(|(_, r)| r);
        let first = scanned_ranges.next().ok_or("no tables")?;
        let intersection = scanned_ranges.fold(first, |acc, r| acc.intersection(&r));
        intersection
    };

    // Find the ranges of blocks that have not been scanned yet for at least one table.
    let ranges = scanned_ranges.complement(start, end);

    // Split them across the target number of jobs as to balance the number of blocks per job.
    let multiranges = ranges.split_and_partition(n_jobs as u64, 1000);
    let existing_blocks = existing_blocks(&ctx).await?;

    let jobs = multiranges.into_iter().enumerate().map(|(i, multirange)| {
        Arc::new(Job {
            dataset_ctx: ctx.clone(),
            block_streamer: client.clone(),
            multirange,
            job_id: i as u32,
            partition_size,
            parquet_opts: parquet_opts.clone(),
            existing_blocks: existing_blocks.clone(),
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
    dataset_store: Arc<DatasetStore>,
    env: Arc<RuntimeEnv>,
    scanned_ranges_by_table: BTreeMap<String, MultiRange>,
    parquet_opts: ParquetWriterProperties,
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
            let mut writer = ParquetWriter::new(
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

// if end_block starts with "+" then it is a relative block number
// otherwise, it's an absolute block number and should be after start_block
fn resolve_end_block(start_block: u64, end_block: String) -> Result<u64, BoxError> {
    let end_block = if end_block.starts_with('+') {
        let relative_block = end_block
            .trim_start_matches('+')
            .parse::<u64>()
            .map_err(|e| format!("invalid relative end block: {e}"))?;
        start_block + relative_block
    } else {
        end_block
            .parse::<u64>()
            .map_err(|e| format!("invalid end block: {e}"))?
    };
    if end_block < start_block {
        return Err("end_block must be greater than or equal to start_block".into());
    }
    if end_block == 0 {
        return Err("end_block must be greater than 0".into());
    }
    Ok(end_block)
}

fn parquet_opts(compression: Compression) -> ParquetWriterProperties {
    // For DataFusion defaults, see `ParquetOptions` here:
    // https://github.com/apache/arrow-datafusion/blob/main/datafusion/common/src/config.rs
    //
    // Note: We could set `sorting_columns` for columns like `block_num` and `ordinal`. However,
    // Datafusion doesn't actually read that metadata info anywhere and just reiles on the
    // `file_sort_order` set on the reader configuration.
    ParquetWriterProperties::builder()
        .set_compression(compression)
        .set_bloom_filter_enabled(true)
        .build()
}

/// Blocks that already exist in the dataset. This is used to ensure no duplicate data is written.
async fn existing_blocks(ctx: &QueryContext) -> Result<BTreeMap<String, MultiRange>, BoxError> {
    let mut existing_blocks: BTreeMap<String, MultiRange> = BTreeMap::new();
    for table in ctx.catalog().all_tables() {
        let mut multirange = MultiRange::default();
        let mut record_stream = ctx
            .execute_sql(&format!(
                "select distinct({BLOCK_NUM}) from {} order by block_num",
                table.table_ref()
            ))
            .await?;
        while let Some(batch) = record_stream.next().await {
            let batch = batch?;
            let block_nums = batch.column(0).as_primitive::<UInt64Type>().values();
            MultiRange::from_values(block_nums.as_ref()).and_then(|r| multirange.append(r))?;
        }
        existing_blocks.insert(table.table_name().to_string(), multirange);
    }

    Ok(existing_blocks)
}

// This is the intersection of the `__scanned_ranges` for all tables. That is, a range is only
// considered scanned if it is scanned for all tables.
async fn scanned_ranges_by_table(
    ctx: &QueryContext,
) -> Result<BTreeMap<String, MultiRange>, BoxError> {
    use common::meta_tables::scanned_ranges::ranges_for_table;

    let mut multirange_by_table = BTreeMap::default();

    for table in ctx.catalog().all_tables() {
        let table_name = table.table_name().to_string();
        let ranges = ranges_for_table(ctx, table.catalog_schema(), &table_name).await?;
        let multi_range = MultiRange::from_ranges(ranges)?;
        multirange_by_table.insert(table_name, multi_range);
    }

    Ok(multirange_by_table)
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

// As a consistency check, ensure that all previously written are accounted for in
// `__scanned_ranges`. If they are not, delete them as that is an inconsistent state.
//
// See also: scanned-ranges-consistency
async fn delete_orphaned_files(
    physical_dataset: &PhysicalDataset,
    ctx: &QueryContext,
) -> Result<(), ConsistencyCheckError> {
    for table in physical_dataset.tables() {
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
                let dataset_name = table.catalog_schema().to_string();
                let err = format!(
                    "file in __scanned_ranges does not exist in store: {}",
                    filename
                )
                .into();
                return Err(ConsistencyCheckError::CorruptedDataset(dataset_name, err));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_block_range() {
        let test_cases = vec![
            (10, "20", Ok(20)),
            (0, "1", Ok(1)),
            (
                18446744073709551614,
                "18446744073709551615",
                Ok(18_446_744_073_709_551_615u64),
            ),
            (10, "+5", Ok(15)),
            (100, "90", Err(BoxError::from(""))),
            (0, "0", Err(BoxError::from(""))),
            (0, "0x", Err(BoxError::from(""))),
            (0, "xxx", Err(BoxError::from(""))),
            (100, "+1000x", Err(BoxError::from(""))),
            (100, "+1x", Err(BoxError::from(""))),
        ];

        for (start_block, end_block, expected) in test_cases {
            match resolve_end_block(start_block, end_block.into()) {
                Ok(result) => assert_eq!(expected.unwrap(), result),
                Err(_) => assert_eq!(expected.is_err(), true),
            }
        }
    }
}

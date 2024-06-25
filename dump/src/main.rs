mod job;
mod metrics; // unused for now
mod parquet_writer;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use common::arrow::array::AsArray as _;
use common::arrow::datatypes::UInt64Type;
use common::catalog::physical::Catalog;
use common::config::Config;
use common::multirange::MultiRange;
use common::parquet;
use common::query_context::QueryContext;
use common::tracing;
use common::BoxError;
use common::BLOCK_NUM;
use datafusion::sql::TableReference;
use dataset_store::load_client;
use dataset_store::load_dataset;
use futures::future::try_join_all;
use futures::StreamExt as _;
use futures::TryFutureExt as _;
use job::Job;
use log::info;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties as ParquetWriterProperties;

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
    start: String,

    /// The block number to end at, inclusive. If starts with "+" then relative to `start`.
    #[arg(long, short, env = "DUMP_END_BLOCK")]
    end_block: String,

    /// How many parallel extractor jobs to run. Defaults to 1. Each job will be responsible for an
    /// equal number of blocks. Example: If start = 0, end = 10_000_000 and n_jobs = 10, then each
    /// job will be responsible for a contiguous section of 1 million blocks.
    #[arg(long, short = 'j', default_value = "1", env = "DUMP_N_JOBS")]
    n_jobs: u8,

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
async fn main() -> Result<(), BoxError> {
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

    let cfg = Config::load(config_path)?;
    let partition_size = partition_size_mb * 1024 * 1024;
    let compression = if disable_compression {
        parquet::basic::Compression::UNCOMPRESSED
    } else {
        Compression::ZSTD(ZstdLevel::try_new(1).unwrap())
    };

    let (start, end_block) = resolve_block_range(start, end_block)?;

    let dataset = load_dataset(&dataset_name, &cfg.dataset_defs_store).await?;
    let client = load_client(&dataset_name, &cfg.dataset_defs_store, &cfg.providers_store).await?;

    let env = Arc::new(cfg.to_runtime_env()?);
    let catalog = Catalog::for_dataset(&dataset, cfg.data_store)?;
    let ctx = Arc::new(QueryContext::for_catalog(catalog, env).await?);

    let existing_blocks = existing_blocks(&ctx).await?;
    for (table_name, multirange) in &existing_blocks {
        info!(
            "Existing blocks for table `{}`: {}",
            table_name,
            multirange.total_len()
        );
    }

    // Find the ranges of blocks that have not been scanned yet, in case we are resuming the dump
    // process, and split them across jobs as to balance the number of blocks per job.
    let ranges_to_scan = {
        let scanned_ranges = scanned_ranges(&ctx).await?;
        let missing_ranges = scanned_ranges.complement(start, end_block);
        missing_ranges.split_and_partition(n_jobs as u64, 1000)
    };

    let jobs = ranges_to_scan
        .into_iter()
        .enumerate()
        .map(|(i, multirange)| {
            Arc::new(Job {
                dataset_ctx: ctx.clone(),
                block_streamer: client.clone(),
                multirange,
                job_id: i as u32,
                partition_size,
                parquet_opts: parquet_opts(compression),
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

    info!("All {} jobs completed successfully", n_jobs);

    Ok(())
}

// start block is always a number
// if end_block starts with "+" then it is a relative block number
// otherwise, it's an absolute block number and should be after start_block
fn resolve_block_range(start_block: String, end_block: String) -> Result<(u64, u64), BoxError> {
    if start_block.starts_with('+') {
        return Err("start_block must be an absolute block number".into());
    }
    let start_block = start_block
        .parse::<u64>()
        .map_err(|e| format!("invalid start block: {e}"))?;
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
    Ok((start_block, end_block))
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
async fn scanned_ranges(ctx: &QueryContext) -> Result<MultiRange, BoxError> {
    use common::meta_tables::scanned_ranges::TABLE_NAME as __SCANNED_RANGES;

    let mut multirange_by_table: BTreeMap<String, MultiRange> = BTreeMap::default();

    for table in ctx.catalog().all_tables() {
        let table_name = table.table_name().to_string();
        let scanned_ranges_ref = TableReference::partial(table.catalog_schema(), __SCANNED_RANGES);
        let batch = ctx
            .meta_execute_sql(&format!(
                "select range_start, range_end from {scanned_ranges_ref} where table = '{table_name}' order by range_start, range_end",
            ))
            .await?;
        let start_blocks: &[u64] = batch
            .column(0)
            .as_primitive::<UInt64Type>()
            .values()
            .as_ref();
        let end_blocks: &[u64] = batch
            .column(1)
            .as_primitive::<UInt64Type>()
            .values()
            .as_ref();
        let ranges = start_blocks.iter().zip(end_blocks).map(|(s, e)| (*s, *e));
        let multi_range = MultiRange::from_ranges(ranges.collect());
        multirange_by_table.insert(table_name, multi_range);
    }

    let mut scanned_ranges = multirange_by_table.into_iter().map(|(_, r)| r);
    let first = scanned_ranges.next().ok_or("no tables")?;
    let intersection = scanned_ranges.fold(first, |acc, r| acc.intersection(&r));
    Ok(intersection)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_block_range() {
        let test_cases = vec![
            ("10", "20", Ok((10, 20))),
            ("0", "1", Ok((0, 1))),
            (
                "18446744073709551614",
                "18446744073709551615",
                Ok((18_446_744_073_709_551_614u64, 18_446_744_073_709_551_615u64)),
            ),
            ("10", "+5", Ok((10, 15))),
            ("100", "90", Err(BoxError::from(""))),
            ("0", "0", Err(BoxError::from(""))),
            ("0", "0x", Err(BoxError::from(""))),
            ("0", "xxx", Err(BoxError::from(""))),
            ("xxx", "123", Err(BoxError::from(""))),
            ("100", "+1000x", Err(BoxError::from(""))),
            ("100", "+1x", Err(BoxError::from(""))),
            ("123x", "+5", Err(BoxError::from(""))),
            ("+10", "1000", Err(BoxError::from(""))),
            ("-10", "100", Err(BoxError::from(""))),
            ("-10", "+50", Err(BoxError::from(""))),
        ];

        for (start_block, end_block, expected) in test_cases {
            match resolve_block_range(start_block.into(), end_block.into()) {
                Ok(result) => assert_eq!(expected.unwrap(), result),
                Err(_) => assert_eq!(expected.is_err(), true),
            }
        }
    }
}

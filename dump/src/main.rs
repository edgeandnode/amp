mod client;
mod job;
mod parquet_writer;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use crate::client::BlockStreamerClient;
use anyhow::Context as _;
use clap::Parser;
use common::arrow::array::AsArray as _;
use common::arrow::datatypes::UInt64Type;
use common::dataset_context::DatasetContext;
use common::multirange::MultiRange;
use common::parquet;
use common::BLOCK_NUM;
use fs_err as fs;
use futures::future::try_join_all;
use futures::StreamExt as _;
use futures::TryFutureExt as _;
use job::Job;
use log::info;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties as ParquetWriterProperties;

/// A tool for dumping a range of firehose blocks to a protobufs json file and/or for converting them
/// to parquet tables.
#[derive(Parser, Debug)]
#[command(name = "firehose-dump")]
struct Args {
    /// Path to a provider config file. Example config:
    ///
    /// ```toml
    /// url = "http://localhost:8080"
    /// token = "secret"
    /// ```
    #[arg(long, short, env = "FIREHOSE_PROVIDER")]
    config: String,

    /// The block number to start from, inclusive. If ommited, defaults to `0`. Note that `dump` is
    /// smart about keeping track of what blocks have already been dumped, so you only need to set
    /// this if you really don't want the data before this block.
    #[arg(long, short, default_value = "0", env = "DUMP_START_BLOCK")]
    start: u64,

    /// The block number to end at, inclusive.
    #[arg(long, short, env = "DUMP_END_BLOCK")]
    end_block: u64,

    /// How many parallel extractor jobs to run. Defaults to 1. Each job will be responsible for an
    /// equal number of blocks. Example: If start = 0, end = 10_000_000 and n_jobs = 10, then each
    /// job will be responsible for a contiguous section of 1 million blocks.
    #[arg(long, short = 'j', default_value = "1", env = "DUMP_N_JOBS")]
    n_jobs: u8,

    /// The output location and path. Both local and object storage are supported.
    ///
    /// - For local storage, this is the path to a directory.
    ///
    /// - For GCS, this expected to be gs://<bucket>.
    ///   GCS Authorization can be configured through one of the following environment variables:
    ///     * GOOGLE_SERVICE_ACCOUNT_PATH: location of service account file, or
    ///     * GOOGLE_SERVICE_ACCOUNT_KEY: JSON serialized service account key.
    ///   It will otherwise fallback to using Appication Default Credentials.
    ///
    /// - For S3, this expected to be s3://<bucket>.
    ///   S3 session can be configured through the following environment variables:
    ///     * AWS_ACCESS_KEY_ID: access key ID
    ///     * AWS_SECRET_ACCESS_KEY: secret access key
    ///     * AWS_DEFAULT_REGION: AWS region
    ///     * AWS_ENDPOINT: endpoint
    ///     * AWS_SESSION_TOKEN: session token
    ///     * AWS_ALLOW_HTTP: allow HTTP
    #[arg(long, env = "DUMP_TO")]
    to: String,

    /// The size of each partition in MB. Once the size is reached, a new part file is created. This
    /// is based on the estimated in-memory size of the data. The actual on-disk file size will vary,
    /// but will correlate with this value. Defaults to 2 GB.
    #[arg(long, default_value = "2048", env = "DUMP_PARTITION_SIZE_MB")]
    partition_size_mb: u64,

    /// Whether to disable compression when writing parquet files. Defaults to false.
    #[arg(long, env = "DUMP_DISABLE_COMPRESSION")]
    disable_compression: bool,

    // Substreams package manifest URL if streaming substreams data
    #[arg(long, env = "DUMP_SUBSTREAMS_MANIFEST")]
    manifest: Option<String>,

    // Substreams output module name
    #[arg(long, env = "DUMP_SUBSTREAMS_MODULE")]
    module: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let Args {
        config,
        start,
        end_block,
        to,
        n_jobs,
        partition_size_mb,
        disable_compression,
        manifest,
        module,
    } = args;
    let partition_size = partition_size_mb * 1024 * 1024;
    let compression = if disable_compression {
        parquet::basic::Compression::UNCOMPRESSED
    } else {
        Compression::ZSTD(ZstdLevel::try_new(1).unwrap())
    };

    if end_block == 0 {
        return Err(anyhow::anyhow!(
            "The end block number must be greater than 0"
        ));
    }

    let client = {
        let config = fs::read_to_string(&config)?;
        let provider = toml::from_str(&config)?;
        if manifest.is_none() {
            BlockStreamerClient::FirehoseClient(
                firehose_datasets::client::Client::new(provider).await?,
            )
        } else {
            let manifest = manifest.context("missing manifest")?;
            let module = module.context("missing output module")?;
            let client =
                substreams_datasets::client::Client::new(provider, manifest, module).await?;
            BlockStreamerClient::SubstreamsClient(client)
        }
    };

    let dataset = match client {
        BlockStreamerClient::FirehoseClient(_) => {
            firehose_datasets::evm::dataset("mainnet".to_string())
        }
        BlockStreamerClient::SubstreamsClient(ref client) => {
            substreams_datasets::dataset("mainnet".to_string(), client.tables().clone())
        }
    };

    let ctx = Arc::new(DatasetContext::new(dataset, to).await?);
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
        missing_ranges.split_and_partition(n_jobs as u64, 100)
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
    try_join_all(jobs.into_iter().map(|job| {
        let handle = tokio::spawn(job::run(job));

        // Stagger the start of each job by 1 second in an attempt to avoid client rate limits.
        std::thread::sleep(Duration::from_secs(1));

        async { handle.err_into().await.and_then(|x| x) }
    }))
    .await?;

    Ok(())
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
        .build()
}

/// Blocks that already exist in the dataset. This is used to ensure no duplicate data is written.
async fn existing_blocks(
    ctx: &DatasetContext,
) -> Result<BTreeMap<String, MultiRange>, anyhow::Error> {
    let mut existing_blocks: BTreeMap<String, MultiRange> = BTreeMap::new();
    for table in ctx.tables() {
        let table_name = table.name.clone();
        let mut multirange = MultiRange::default();
        let mut record_stream = ctx
            .execute_sql(&format!(
                "select distinct({BLOCK_NUM}) from {} order by block_num",
                table_name
            ))
            .await?;
        while let Some(batch) = record_stream.next().await {
            let batch = batch?;
            let block_nums = batch.column(0).as_primitive::<UInt64Type>().values();
            MultiRange::from_values(block_nums.as_ref()).and_then(|r| multirange.append(r))?;
        }
        existing_blocks.insert(table_name, multirange);
    }

    Ok(existing_blocks)
}

// This is the intersection of the `__scanned_ranges` for all tables. That is, a range is only
// considered scanned if it is scanned for all tables.
async fn scanned_ranges(ctx: &DatasetContext) -> Result<MultiRange, anyhow::Error> {
    use common::meta_tables::scanned_ranges::TABLE_NAME as __SCANNED_RANGES;

    let mut multirange_by_table: BTreeMap<String, MultiRange> = BTreeMap::default();

    for table in ctx.tables() {
        let table_name = table.name.clone();
        let batch = ctx
            .meta_execute_sql(&format!(
                "select range_start, range_end from {__SCANNED_RANGES} where table = '{table_name}' order by range_start, range_end",
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
    let first = scanned_ranges.next().context("no tables")?;
    let intersection = scanned_ranges.fold(first, |acc, r| acc.intersection(&r));
    Ok(intersection)
}

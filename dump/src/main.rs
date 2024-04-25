mod job;
mod parquet_writer;

use clap::Parser;
use common::dataset_context::DatasetContext;
use common::parquet;
use firehose_datasets::client::Client;
use fs_err as fs;
use futures::future::join_all;
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

    /// The block number to start from, inclusive. If ommited, the default behaviour is to:
    /// - Start from genesis if no blocks are present in the target tables.
    /// - Start from the last block in the target tables, picking up where it left off.
    #[arg(long, short, env = "DUMP_START_BLOCK")]
    start: Option<u64>,

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
    /// - S3 support TODO.
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
        Client::new(provider).await?
    };

    let dataset = firehose_datasets::evm::dataset("mainnet".to_string());
    let ctx = DatasetContext::new(dataset.clone(), to).await?;
    let block_stats = ctx.block_stats().await?;
    for (table_name, multirange) in &block_stats.existing_blocks {
        info!(
            "Existing blocks for table `{}`: {}",
            table_name,
            multirange.total_len()
        );
    }
    let start = start.unwrap_or(block_stats.min_latest_block);
    info!("Starting from block {}", start);

    let store = ctx.object_store()?;
    let jobs = {
        let mut jobs = vec![];
        let total_blocks = end_block - start + 1;
        let blocks_per_job = total_blocks.div_ceil(n_jobs as u64);
        let mut from = start;
        while from <= end_block {
            let to = (from + blocks_per_job).min(end_block);
            jobs.push(Job {
                dataset: dataset.clone(),
                block_streamer: client.clone(),
                start: from,
                end: to,
                job_id: jobs.len() as u8,
                store: store.clone(),
                partition_size,
                parquet_opts: parquet_opts(compression),
                existing_blocks: block_stats.existing_blocks.clone(),
            });
            from = to + 1;
        }
        jobs
    };

    for res in join_all(jobs.into_iter().map(job::run_job)).await {
        res?;
    }

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

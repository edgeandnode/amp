mod job;
mod parquet_writer;

use anyhow::Context as _;
use clap::Parser;
use datafusion::execution::context::SessionContext;
use datafusion::parquet;
use firehose_datasources::client::Client;
use fs_err as fs;
use futures::future::join_all;
use job::Job;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::ObjectStore;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties as ParquetWriterProperties;
use std::sync::Arc;
use url::Url;
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
    #[arg(long, short, env = "DUMP_FIREHOSE_PROVIDER")]
    config: String,

    /// The block number to start from, inclusive.
    start: u64,

    /// The block number to end at, inclusive.
    end: u64,

    /// How many parallel extractor jobs to run. Defaults to 1. Each job will be responsible for an
    /// equal number of blocks. Example: If start = 0, end = 10_000_000 and n_jobs = 10, then each
    /// job will be responsible for a contiguous section of 1 million blocks.
    #[arg(long, short = 'j', default_value = "1")]
    n_jobs: u8,

    /// The output location and path. Both local and object storage are supported.
    /// - For local storage, this is the path to a directory.
    /// - For GCS, this expected to be gs://<bucket>.
    ///   GCS Authorization can be configured through one of the following environment variables:
    ///     * GOOGLE_SERVICE_ACCOUNT_PATH: location of service account file
    ///     * GOOGLE_SERVICE_ACCOUNT_KEY: JSON serialized service account key It will otherwise
    ///   fallback to using Appication Default Credentials.
    ///     * https://cloud.google.com/docs/authentication/application-default-credentials
    /// - S3 support TODO.
    #[arg(long, short)]
    out: String,

    /// The size of each partition in MB. Once the size is reached, a new part file is created. This
    /// is based on the estimated in-memory size of the data. The actual on-disk file size will vary,
    /// but will correlate with this value. Defaults to 1 GB.
    #[arg(long, default_value = "1024")]
    partition_size_mb: u64,

    /// Whether to disable compression when writing parquet files. Defaults to false.
    #[arg(long)]
    disable_compression: bool,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();
    let Args {
        config,
        start,
        end,
        mut out,
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

    if end == 0 {
        return Err(anyhow::anyhow!(
            "The end block number must be greater than 0"
        ));
    }

    let client = {
        let config = fs::read_to_string(&config)?;
        let provider = toml::from_str(&config)?;
        Client::new(provider).await?
    };

    // Make sure `out` has a trailing slash.
    if !out.ends_with('/') {
        out.push('/');
    }

    let (base_url, store): (_, Arc<dyn ObjectStore>) = if out.starts_with("gs://") {
        let bucket = {
            let segment = out.trim_start_matches("gs://").split('/').next();
            segment.context("invalid GCS url")?
        };

        let store = Arc::new(
            GoogleCloudStorageBuilder::from_env()
                .with_bucket_name(bucket)
                .build()?,
        );
        (Url::parse(&out)?, store)
    } else {
        let store = Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&out)?);
        let path = fs::canonicalize(&out)?;
        let url = Url::from_directory_path(path).unwrap();
        (url, store)
    };

    let dataset = firehose_datasources::evm::dataset("mainnet".to_string());

    let ctx = SessionContext::new();
    ctx.runtime_env()
        .register_object_store(&base_url, store.clone());

    let jobs = {
        let mut jobs = vec![];
        let total_block = end - start + 1;
        let blocks_per_job = total_block.div_ceil(n_jobs as u64);
        let mut from = start;
        while from <= end {
            let to = (from + blocks_per_job).min(end);
            jobs.push(Job {
                client: client.clone(),
                start: from,
                end: to,
                job_id: jobs.len() as u8,
                store: store.clone(),
                partition_size,
                parquet_opts: parquet_opts(compression),
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

struct MultiRange {
    // The ranges are inclusive on both ends.
    ranges: Vec<(u64, u64)>,
}

impl MultiRange {
    fn contains(&self, value: u64) -> bool {
        self.ranges
            .iter()
            .any(|(start, end)| *start <= value && value <= *end)
    }
}

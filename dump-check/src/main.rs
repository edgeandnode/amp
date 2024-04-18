mod job;

use clap::Parser;
use common::dataset_context::DatasetContext;
use firehose_datasets::client::Client;
use futures::future::join_all;
use job::Job;
use std::{fs, sync::Arc};

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

    /// The block number to start from, inclusive.
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
    /// - S3 support TODO.
    #[arg(long, env = "DUMP_TO")]
    to: String,

    /// Number of blocks to validate per query
    #[arg(long, short, default_value = "1000", env = "DUMP_BATCH_SIZE")]
    batch_size: u64,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();
    let Args {
        config,
        start,
        end_block,
        to,
        batch_size,
        n_jobs,
    } = args;

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

    let ctx = Arc::new(DatasetContext::new(dataset.clone(), to).await?);
    let total_blocks = end_block - start + 1;
    let jobs = {
        let mut jobs = vec![];
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
                batch_size,
                ctx: ctx.clone(),
            });
            from = to + 1;
        }
        jobs
    };

    for res in join_all(jobs.into_iter().map(job::run_job)).await {
        res?;
    }

    println!("Validated successfully {total_blocks} blocks");

    Ok(())
}

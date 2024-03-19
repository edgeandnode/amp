mod job;

use clap::Parser;
use firehose_datasources::client::Client;
use fs_err as fs;
use futures::future::join_all;
use job::Job;
use std::sync::Arc;

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

    /// The path to write the output files to.
    #[arg(long, short)]
    out: String,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();
    let Args {
        config,
        start,
        end,
        out,
        n_jobs,
    } = args;

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

    let store = Arc::new(object_store::local::LocalFileSystem::new_with_prefix(out)?);

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

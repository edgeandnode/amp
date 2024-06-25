mod job;
mod metrics;
mod ui;

use clap::Parser;
use common::{catalog::physical::Catalog, config::Config, query_context::QueryContext, BoxError};
use dataset_store::{load_client, load_dataset};
use futures::future::try_join_all;
use job::Job;
use std::sync::Arc;

use crate::metrics::MetricsRegistry;

/// Checks the output of `dump` against a provider.
#[derive(Parser, Debug)]
#[command(name = "firehose-dump-check")]
struct Args {
    /// Path to a config file. See README for details on the format.
    #[arg(long, env = "NOZZLE_CONFIG")]
    config: String,

    /// The name of the dataset to dump. This will be looked up in the dataset directory.
    /// Will also be used as a subdirectory in the output path, `<data_dir>/<dataset>`.
    #[arg(long, env = "DUMP_DATASET")]
    dataset: String,

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

    /// Number of blocks to validate per query
    #[arg(long, short, default_value = "1000", env = "DUMP_BATCH_SIZE")]
    batch_size: u64,
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let Args {
        config: config_path,
        dataset: dataset_name,
        start,
        end_block,
        batch_size,
        n_jobs,
    } = args;

    let cfg = Config::load(config_path)?;

    if end_block == 0 {
        return Err("The end block number must be greater than 0".into());
    }

    if start > end_block {
        return Err("The start block number must be less than the end block number".into());
    }

    let metrics = Arc::new(MetricsRegistry::new());

    prometheus_exporter::start("0.0.0.0:9102".parse().expect("failed to parse binding"))
        .expect("failed to start prometheus exporter");

    let dataset = load_dataset(&dataset_name, &cfg.dataset_defs_store).await?;
    let client = load_client(&dataset_name, &cfg.dataset_defs_store, &cfg.providers_store).await?;

    let env = Arc::new((cfg.to_runtime_env())?);
    let catalog = Catalog::for_dataset(&dataset, cfg.data_store)?;
    let ctx = Arc::new(QueryContext::for_catalog(catalog, env).await?);
    let total_blocks = end_block - start + 1;
    let ui_handle = tokio::spawn(ui::ui(total_blocks, metrics.clone()));

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
                metrics: metrics.clone(),
            });
            from = to + 1;
        }
        jobs
    };

    // early return if any job errors out
    try_join_all(jobs.into_iter().map(job::run_job)).await?;

    ui_handle.await?;

    println!("Validated successfully {total_blocks} blocks");

    Ok(())
}

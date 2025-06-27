mod ui;

use std::sync::Arc;

use clap::Parser;
use common::{
    BoxError,
    config::{Addrs, Config},
};
use dataset_store::DatasetStore;
use metadata_db::MetadataDb;

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

    let config = Arc::new(Config::load(config_path, true, None, Addrs::default(), false).await?);
    let metadata_db: Arc<MetadataDb> = MetadataDb::connect(&config.metadata_db_url).await?.into();
    let dataset_store = DatasetStore::new(config.clone(), metadata_db.clone());

    if end_block == 0 {
        return Err("The end block number must be greater than 0".into());
    }

    if start > end_block {
        return Err("The start block number must be less than the end block number".into());
    }

    prometheus_exporter::start("0.0.0.0:9102".parse().expect("failed to parse binding"))
        .expect("failed to start prometheus exporter");

    let total_blocks = end_block - start + 1;
    let ui_handle = tokio::spawn(ui::ui(total_blocks));

    let env = config.make_query_env()?;

    dump_check::dump_check(
        &dataset_name,
        &dataset_store,
        metadata_db,
        &env,
        batch_size,
        n_jobs,
        start,
        end_block,
    )
    .await?;

    ui_handle.await?;

    Ok(())
}

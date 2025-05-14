mod dump_cmd;

use std::sync::Arc;

use clap::Parser as _;
use common::{
    config::{Addrs, Config},
    tracing_helpers, BoxError,
};
use dump::worker::Worker;
use tokio::{signal, sync::broadcast};
use tracing::{error, info};

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[derive(Debug, clap::Parser)]
struct Args {
    #[arg(long, env = "NOZZLE_CONFIG")]
    config: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, clap::Subcommand)]
enum Command {
    Dump {
        /// The name of the dataset to dump. This will be looked up in the dataset definition directory.
        /// Will also be used as a subdirectory in the output path, `<data_dir>/<dataset>`.
        ///
        /// Also accepts a comma-separated list of datasets, which will be dumped in the provided order.
        #[arg(long, required = true, env = "DUMP_DATASET", value_delimiter = ',')]
        dataset: Vec<String>,

        /// If set to true, only the listed datasets will be dumped in the order they are listed.
        /// By default dump listed datasets and their dependencies, ordered such that each dataset
        /// will be dumped after all datasets they depend on.
        #[arg(long, env = "DUMP_IGNORE_DEPS")]
        ignore_deps: bool,

        /// The block number to start from, inclusive. If omitted, defaults to `0`. Note that `dump` is
        /// smart about keeping track of what blocks have already been dumped, so you only need to set
        /// this if you really don't want the data before this block. If starts with "-" then relative
        /// to the latest block for the dataset.
        #[arg(long, short, default_value = "0", env = "DUMP_START_BLOCK")]
        start: i64,

        /// The block number to end at, inclusive. If starts with "+" then relative to `start`. If
        /// omitted, defaults to a recent block. If starts with "-" then relative to the latest block
        /// for this dataset.
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

        /// The maximum number of blocks to be dumped per SQL query at once. Defaults to 100_000.
        #[arg(long, default_value = "100000", env = "DUMP_INPUT_BATCH_SIZE_BLOCKS")]
        input_batch_size_blocks: u64,

        /// Whether to disable compression when writing parquet files. Defaults to false.
        #[arg(long, env = "DUMP_DISABLE_COMPRESSION")]
        disable_compression: bool,

        /// How often to run the dump job in minutes. By default will run once and exit.
        #[arg(long, env = "DUMP_RUN_EVERY_MINS")]
        run_every_mins: Option<u64>,
    },
    Server {
        /// Disable admin API
        #[arg(long, env = "SERVER_NO_ADMIN")]
        no_admin: bool,
    },
    Worker {
        /// The node id of the worker.
        #[arg(long, env = "NOZZLE_NODE_ID")]
        node_id: String,
    },
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
    tracing_helpers::register_logger();

    // Log version info
    info!(
        "built on {}, git describe {}",
        env!("VERGEN_BUILD_DATE"),
        env!("VERGEN_GIT_DESCRIBE"),
    );

    let args = Args::parse();
    let config = Arc::new(Config::load(args.config, true, None, Addrs::default()).await?);
    let metadata_db = config.metadata_db().await?.into();

    match args.command {
        Command::Dump {
            start,
            end_block,
            n_jobs,
            partition_size_mb,
            input_batch_size_blocks,
            disable_compression,
            dataset: datasets,
            ignore_deps,
            run_every_mins,
        } => {
            dump_cmd::dump(
                config,
                metadata_db,
                datasets,
                ignore_deps,
                start,
                end_block,
                n_jobs,
                partition_size_mb,
                input_batch_size_blocks,
                disable_compression,
                run_every_mins,
            )
            .await
        }
        Command::Server { no_admin } => {
            let (_, server) =
                nozzle::server::run(config, metadata_db, no_admin, ctrl_c_shutdown()).await?;
            server.await
        }
        Command::Worker { node_id } => {
            let worker = Worker::new(config.clone(), metadata_db.as_ref().clone(), node_id);
            worker.run().await.map_err(Into::into)
        }
    }
}

/// Graceful shutdown as recommended by axum:
/// https://github.com/tokio-rs/axum/blob/9c9cbb5c5f72452825388d63db4f1e36c0d9b3aa/examples/graceful-shutdown/src/main.rs
fn ctrl_c_shutdown() -> broadcast::Receiver<()> {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    let (tx, rx) = broadcast::channel(1);
    tokio::spawn(async move {
        tokio::select! {
            _ = ctrl_c => {},
            _ = terminate => {},
        }
        info!("gracefully shutting down");
        if let Err(e) = tx.send(()) {
            error!("failed to send shutdown signal: {e}");
        }
    });
    rx
}

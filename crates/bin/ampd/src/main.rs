use std::{path::PathBuf, sync::Arc};

use ampd::{dev_cmd, dump_cmd, gen_manifest_cmd, migrate_cmd, restore_cmd, server_cmd, worker_cmd};
use common::{BoxError, config::Config};
use dataset_store::DatasetKind;
use dump::EndBlock;

#[cfg(feature = "snmalloc")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[derive(Debug, clap::Parser)]
#[command(version = env!("VERGEN_GIT_DESCRIBE"))]
struct Args {
    /// The configuration file to use. This file defines where to look for dataset definitions and
    /// providers, along with many other configuration options.
    ///
    /// This argument is optional for the `generate-manifest` command.
    #[arg(long, env = "AMP_CONFIG")]
    config: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Clone, clap::Subcommand)]
enum Command {
    Dump {
        /// The name or path of the dataset to dump. This will be looked up in the dataset definition directory.
        /// Will also be used as a subdirectory in the output path, `<data_dir>/<dataset>`.
        ///
        /// Also accepts a comma-separated list of datasets, which will be dumped in the provided order.
        #[arg(long, required = true, env = "DUMP_DATASET", value_delimiter = ',')]
        dataset: Vec<String>,

        /// If set to true, only the listed datasets will be dumped in the order they are listed.
        /// By default, dump listed datasets and their dependencies, ordered such that each dataset
        /// will be dumped after all datasets they depend on.
        #[arg(long, env = "DUMP_IGNORE_DEPS")]
        ignore_deps: bool,

        /// The block number to end at, inclusive.
        ///
        /// Accepts:
        /// - Positive integer (e.g., "1000000"): Stop at this specific block number
        /// - "latest": Stop at the latest available block
        /// - Negative integer (e.g., "-100"): Stop 100 blocks before latest
        /// - Omitted: Continuous dumping (only supported for single dataset)
        ///
        /// When combined with --run-every-mins, omitting this defaults to "latest".
        #[arg(long, short, env = "DUMP_END_BLOCK")]
        end_block: Option<EndBlock>,

        /// How many parallel extractor jobs to run. Defaults to 1. Each job will be responsible for an
        /// equal number of blocks. Example: If start = 0, end = 10_000_000 and n_jobs = 10, then each
        /// job will be responsible for a contiguous section of 1 million blocks.
        #[arg(long, short = 'j', default_value = "1", env = "DUMP_N_JOBS")]
        n_jobs: u16,

        /// The size of each partition in MB. Once the size is reached, a new part file is created. This
        /// is based on the estimated in-memory size of the data. The actual on-disk file size will vary,
        /// but will correlate with this value. Defaults to 4 GB.
        #[arg(long, env = "DUMP_PARTITION_SIZE_MB")]
        partition_size_mb: Option<u64>,

        /// How often to run the dump job in minutes. By default will run once and exit.
        #[arg(long, env = "DUMP_RUN_EVERY_MINS")]
        run_every_mins: Option<u64>,

        /// The location of the dump. If not specified, the dump will be written to the default location in AMP_DATA_DIR.
        #[arg(long)]
        location: Option<String>,

        /// Overwrite existing location and dump to a new, fresh directory
        #[arg(long, env = "DUMP_FRESH")]
        fresh: bool,
    },
    Dev {
        /// Enable Arrow Flight RPC Server.
        #[arg(long, env = "FLIGHT_SERVER")]
        flight_server: bool,
        /// Enable JSON Lines Server.
        #[arg(long, env = "JSONL_SERVER")]
        jsonl_server: bool,
        /// Enable Admin API Server.
        #[arg(long, env = "ADMIN_SERVER")]
        admin_server: bool,
    },
    Server {
        /// Enable Arrow Flight RPC Server.
        #[arg(long, env = "FLIGHT_SERVER")]
        flight_server: bool,
        /// Enable JSON Lines Server.
        #[arg(long, env = "JSONL_SERVER")]
        jsonl_server: bool,
    },
    Worker {
        /// The node id of the worker.
        #[arg(long, env = "AMP_NODE_ID")]
        node_id: String,
    },
    Controller,
    GenerateManifest {
        /// The name of the network.
        #[arg(long, required = true, env = "GM_NETWORK")]
        network: String,

        /// Kind of the dataset.
        #[arg(long, required = true, env = "GM_KIND")]
        kind: String,

        /// The name of the dataset.
        #[arg(long, required = true, env = "GM_NAME")]
        name: String,

        /// Output file or directory. If it's a directory, the generated file name will
        /// match the `kind` parameter.
        ///
        /// If not specified, the manifest will be printed to stdout.
        #[arg(short, long, env = "GM_OUT")]
        out: Option<PathBuf>,

        /// The starting block number for the dataset. Defaults to 0.
        #[arg(long, env = "GM_START_BLOCK")]
        start_block: Option<u64>,

        /// Only include finalized block data.
        #[arg(long, env = "GM_FINALIZED_BLOCKS_ONLY")]
        finalized_blocks_only: bool,
    },
    /// Run migrations on the metadata database
    Migrate,
    /// Restore dataset snapshots from storage
    Restore {
        /// The name or names of the datasets to restore (comma-separated).
        #[arg(long, required = true, value_delimiter = ',')]
        dataset: Vec<String>,
    },
}

#[tokio::main]
async fn main() {
    if let Err(err) = main_inner().await {
        // Manually print the error so we can control the format.
        eprintln!("Exiting with error: {err}");
        std::process::exit(1);
    }
}

async fn main_inner() -> Result<(), BoxError> {
    let Args {
        config: config_path,
        command,
    } = clap::Parser::parse();

    // Log version info
    tracing::info!(
        "built on {}, git describe {}",
        env!("VERGEN_BUILD_DATE"),
        env!("VERGEN_GIT_DESCRIBE"),
    );

    match command {
        Command::Dev {
            mut flight_server,
            mut jsonl_server,
            mut admin_server,
        } => {
            // If neither of the flags are set, enable all servers
            if !flight_server && !jsonl_server && !admin_server {
                flight_server = true;
                jsonl_server = true;
                admin_server = true;
            }

            let config = load_config(config_path.as_ref(), true).await?;
            let metadata_db = config.metadata_db().await?;

            let (tracing_provider, metrics_provider, metrics_meter) =
                monitoring::init(config.opentelemetry.as_ref())?;

            let result = dev_cmd::run(
                config,
                metadata_db,
                flight_server,
                jsonl_server,
                admin_server,
                metrics_meter,
            )
            .await;

            monitoring::deinit(metrics_provider, tracing_provider)?;

            result?;
            Ok(())
        }
        Command::Dump {
            end_block,
            n_jobs,
            partition_size_mb,
            dataset: datasets,
            ignore_deps,
            run_every_mins,
            location,
            fresh,
        } => {
            let config = load_config(config_path.as_ref(), false).await?;
            let metadata_db = config.metadata_db().await?;

            let (tracing_provider, metrics_provider, metrics_meter) =
                monitoring::init(config.opentelemetry.as_ref())?;

            let result = dump_cmd::run(
                config,
                metadata_db,
                datasets,
                ignore_deps,
                end_block,
                n_jobs,
                partition_size_mb,
                run_every_mins,
                location,
                fresh,
                metrics_meter.as_ref(),
            )
            .await;

            monitoring::deinit(metrics_provider, tracing_provider)?;

            result?;
            Ok(())
        }
        Command::Server {
            mut flight_server,
            mut jsonl_server,
        } => {
            // If neither of the flags are set, enable both servers
            if !flight_server && !jsonl_server {
                flight_server = true;
                jsonl_server = true;
            }

            let config = load_config(config_path.as_ref(), false).await?;
            let metadata_db = config.metadata_db().await?;

            let (tracing_provider, metrics_provider, metrics_meter) =
                monitoring::init(config.opentelemetry.as_ref())?;

            let result = server_cmd::run(
                config,
                metadata_db,
                flight_server,
                jsonl_server,
                metrics_meter.as_ref(),
            )
            .await;

            monitoring::deinit(metrics_provider, tracing_provider)?;

            result?;
            Ok(())
        }
        Command::Worker { node_id } => {
            let node_id = node_id.parse()?;

            let config = load_config(config_path.as_ref(), false).await?;
            let metadata_db = config.metadata_db().await?;

            let (tracing_provider, metrics_provider, metrics_meter) =
                monitoring::init(config.opentelemetry.as_ref())?;

            let result = worker_cmd::run(
                config,
                metadata_db,
                node_id,
                metrics_meter
                    .clone()
                    .expect("metrics_meter should always be initialized"),
            )
            .await;

            monitoring::deinit(metrics_provider, tracing_provider)?;

            result?;
            Ok(())
        }
        Command::Controller => {
            let config = load_config(config_path.as_ref(), false).await?;

            let (tracing_provider, metrics_provider, metrics_meter) =
                monitoring::init(config.opentelemetry.as_ref())?;

            let config = Arc::new(config);
            let (addr, server) =
                controller::serve(config.addrs.admin_api_addr, config, metrics_meter.as_ref())
                    .await?;

            tracing::info!("Controller Admin API running at {}", addr);
            let result = server.await;

            monitoring::deinit(metrics_provider, tracing_provider)?;

            result?;
            Ok(())
        }
        Command::GenerateManifest {
            network,
            kind,
            name,
            out,
            start_block,
            finalized_blocks_only,
        } => {
            let name = name.parse()?;
            let kind = kind.parse::<DatasetKind>()?;

            let (tracing_provider, metrics_provider, _) = monitoring::init(None)?;

            let result = if let Some(mut out) = out {
                if out.is_dir() {
                    out.push(format!("{}.json", &kind));
                }

                let mut out = std::fs::File::create(out)?;
                gen_manifest_cmd::run(
                    name,
                    kind,
                    network,
                    start_block,
                    finalized_blocks_only,
                    &mut out,
                )
                .await
            } else {
                let mut out = std::io::stdout();
                gen_manifest_cmd::run(
                    name,
                    kind,
                    network,
                    start_block,
                    finalized_blocks_only,
                    &mut out,
                )
                .await
            };

            monitoring::deinit(metrics_provider, tracing_provider)?;

            result?;
            Ok(())
        }
        Command::Migrate => {
            let config = load_config(config_path.as_ref(), false).await?;

            let (tracing_provider, metrics_provider, _metrics_meter) =
                monitoring::init(config.opentelemetry.as_ref())?;

            let result = migrate_cmd::run(config).await;

            monitoring::deinit(metrics_provider, tracing_provider)?;

            result?;
            Ok(())
        }
        Command::Restore { dataset: datasets } => {
            let config = load_config(config_path.as_ref(), false).await?;
            let metadata_db = config.metadata_db().await?;

            let (tracing_provider, metrics_provider, _metrics_meter) =
                monitoring::init(config.opentelemetry.as_ref())?;

            let result = restore_cmd::run(config.into(), metadata_db, datasets).await;

            monitoring::deinit(metrics_provider, tracing_provider)?;

            result?;
            Ok(())
        }
    }
}

async fn load_config(
    config_path: Option<&String>,
    allow_temp_db: bool,
) -> Result<Config, BoxError> {
    let Some(config) = config_path else {
        return Err("--config parameter is mandatory".into());
    };
    let config = Config::load(config, true, None, allow_temp_db).await?;
    Ok(config)
}

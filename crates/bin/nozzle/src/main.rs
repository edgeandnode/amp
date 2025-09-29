use std::{path::PathBuf, sync::Arc};

use clap::Parser as _;
use common::{BoxError, config::Config, manifest::derived::Manifest};
use dataset_store::DatasetStore;
use dump::worker::Worker;
use metadata_db::MetadataDb;
use nozzle::dump_cmd;

#[cfg(feature = "snmalloc")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[derive(Debug, clap::Parser)]
struct Args {
    /// The configuration file to use. This file defines where to look for dataset definitions and
    /// providers, along with many other configuration options.
    ///
    /// This argument is optional for the `generate-manifest` command.
    #[arg(long, env = "NOZZLE_CONFIG")]
    config: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, clap::Subcommand, Clone)]
enum Command {
    Dump {
        /// The name or path of the dataset to dump. This will be looked up in the dataset definition directory.
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

        /// The block number to end at, inclusive. If omitted, defaults to a recent block. If
        /// starts with "-" then relative to the latest block for this dataset.
        #[arg(long, short, env = "DUMP_END_BLOCK")]
        end_block: Option<i64>,

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

        /// How often to run the dump job in minutes. By default will run once and exit.
        #[arg(long, env = "DUMP_RUN_EVERY_MINS")]
        run_every_mins: Option<u64>,

        /// The location of the dump. If not specified, the dump will be written to the default location in NOZZLE_DATA_DIR.
        #[arg(long)]
        location: Option<String>,

        /// Overwrite existing location and dump to a new, fresh directory
        #[arg(long, env = "DUMP_FRESH")]
        fresh: bool,

        /// Only dump finalized block data. This only applies to raw datasets.
        #[arg(long, env = "DUMP_ONLY_FINALIZED_BLOCKS")]
        only_finalized_blocks: bool,
    },
    Server {
        /// Run in dev mode, which starts a worker in the same process.
        #[arg(long, env = "SERVER_DEV")]
        dev: bool,
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
    Worker {
        /// The node id of the worker.
        #[arg(long, env = "NOZZLE_NODE_ID")]
        node_id: String,
    },
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

        /// Substreams package manifest URL, required for DatasetKind::Substreams.
        #[arg(long, env = "GM_SS_MANIFEST_URL")]
        manifest: Option<String>,

        /// Substreams output module name, required for DatasetKind::Substreams.
        #[arg(long, env = "GM_SS_MODULE")]
        module: Option<String>,
    },
    /// Run migrations on the metadata database
    Migrate,
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
    let Args { config, command } = Args::parse();
    let config_path = config;

    let allow_temp_db = matches!(command, Command::Server { dev, .. } if dev);
    let (config, metadata_db) =
        load_config_and_metadata_db(config_path.as_ref(), allow_temp_db).await?;

    let (telemetry_tracing_provider, telemetry_metrics_provider, telemetry_metrics_meter) =
        monitoring::init(config.opentelemetry.as_ref())?;

    let metrics_registry = telemetry_metrics_meter
        .as_ref()
        .map(|meter| Arc::new(dump::metrics::MetricsRegistry::new(meter)));

    // Log version info
    tracing::info!(
        "built on {}, git describe {}",
        env!("VERGEN_BUILD_DATE"),
        env!("VERGEN_GIT_DESCRIBE"),
    );

    let cmd_result = match command {
        Command::Dump {
            end_block,
            n_jobs,
            partition_size_mb,
            dataset: datasets,
            ignore_deps,
            run_every_mins,
            location,
            fresh,
            only_finalized_blocks,
        } => {
            if let Some(ref opentelemetry) = config.opentelemetry {
                dump_cmd::validate_export_interval(opentelemetry.metrics_export_interval);
            }
            let config = Arc::new(config);

            let mut datasets_to_dump = Vec::new();
            let dataset_store = DatasetStore::new(config.clone(), metadata_db.clone());

            for dataset in datasets {
                if dataset.ends_with(".json") {
                    tracing::info!("Registering manifest: {}", dataset);
                    let manifest = std::fs::read_to_string(&dataset)?;
                    let manifest: Manifest = serde_json::from_str(&manifest)?;
                    dataset_store
                        .register_manifest(&manifest.name, &manifest.version, &manifest)
                        .await
                        .map_err(|err| -> BoxError { err.to_string().into() })?;
                    datasets_to_dump.push(manifest.to_identifier());
                } else {
                    datasets_to_dump.push(dataset);
                }
            }

            dump_cmd::dump(
                config,
                metadata_db,
                datasets_to_dump,
                ignore_deps,
                end_block,
                n_jobs,
                partition_size_mb,
                run_every_mins,
                None,
                location,
                fresh,
                metrics_registry,
                only_finalized_blocks,
            )
            .await?;
            Ok(())
        }
        Command::Server {
            dev,
            mut flight_server,
            mut jsonl_server,
            mut admin_server,
        } => {
            if !flight_server && !jsonl_server && !admin_server {
                flight_server = true;
                jsonl_server = true;
                admin_server = true;
            }

            let (_, server) = nozzle::server::run(
                config.into(),
                metadata_db,
                dev,
                flight_server,
                jsonl_server,
                admin_server,
                metrics_registry,
            )
            .await?;
            server.await
        }
        Command::Worker { node_id } => {
            let worker = Worker::new(
                config.into(),
                metadata_db,
                node_id.parse()?,
                metrics_registry,
            );
            worker.run().await.map_err(Into::into)
        }
        Command::GenerateManifest {
            network,
            kind,
            name,
            out,
            manifest,
            module,
        } => {
            if let Some(mut out) = out {
                if out.is_dir() {
                    out.push(format!("{}.json", &kind));
                }

                let mut out = std::fs::File::create(out)?;
                generate_manifest::run(network, kind, name, manifest, module, &mut out).await
            } else {
                let mut stdout = std::io::stdout();
                generate_manifest::run(network, kind, name, manifest, module, &mut stdout).await
            }
        }
        Command::Migrate => {
            let config_path = config_path.ok_or("--config parameter is mandatory")?;
            let config = Arc::new(Config::load(config_path, true, None, false).await?);

            let url = config
                .metadata_db
                .url
                .as_ref()
                .ok_or("metadata_db.url is required for migrate command")?;

            tracing::info!("Running migrations on metadata database...");
            let _metadata_db =
                MetadataDb::connect_with_config(url, config.metadata_db.pool_size, true).await?;
            tracing::info!("Migrations completed successfully");

            Ok(())
        }
    };

    cmd_result.and_then(move |_| {
        monitoring::deinit(telemetry_metrics_provider, telemetry_tracing_provider)?;
        Ok(())
    })?;

    Ok(())
}

async fn load_config_and_metadata_db(
    config_path: Option<&String>,
    allow_temp_db: bool,
) -> Result<(Config, MetadataDb), BoxError> {
    let Some(config) = config_path else {
        return Err("--config parameter is mandatory".into());
    };

    let config = Config::load(config, true, None, allow_temp_db).await?;
    let metadata_db = config.metadata_db().await?.into();
    Ok((config, metadata_db))
}

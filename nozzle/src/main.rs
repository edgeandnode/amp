mod dump;

use std::{net::SocketAddr, sync::Arc};

use arrow_flight::flight_service_server::FlightServiceServer;
use clap::Parser as _;
use common::{config::Config, tracing, BoxError};
use futures::{StreamExt as _, TryStreamExt as _};
use log::info;
use metadata_db::MetadataDb;
use server::service::Service;
use tonic::transport::Server;

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
        /// The name of the dataset to dump. This will be looked up in the dataset definiton directory.
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

        /// The block number to start from, inclusive. If ommited, defaults to `0`. Note that `dump` is
        /// smart about keeping track of what blocks have already been dumped, so you only need to set
        /// this if you really don't want the data before this block.
        #[arg(long, short, default_value = "0", env = "DUMP_START_BLOCK")]
        start: u64,

        /// The block number to end at, inclusive. If starts with "+" then relative to `start`. If
        /// ommited, defaults to a recent block.
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

        /// Whether to disable compression when writing parquet files. Defaults to false.
        #[arg(long, env = "DUMP_DISABLE_COMPRESSION")]
        disable_compression: bool,

        /// How often to run the dump job in minutes. By default will run once and exit.
        #[arg(long, env = "DUMP_RUN_EVERY_MINS")]
        run_every_mins: Option<u64>,
    },
    Server,
    AdminApi,
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
    tracing::register_logger();
    let args = Args::parse();

    let config = Arc::new(
        Config::load(args.config, true, None).map_err(|e| format!("failed to load config: {e}"))?,
    );
    let metadata_db = match &config.metadata_db_url {
        Some(url) => Some(MetadataDb::connect(url).await?),
        None => None,
    };

    match args.command {
        Command::Dump {
            start,
            end_block,
            n_jobs,
            partition_size_mb,
            disable_compression,
            dataset: datasets,
            ignore_deps,
            run_every_mins,
        } => {
            dump::dump(
                config,
                metadata_db,
                datasets,
                ignore_deps,
                start,
                end_block,
                n_jobs,
                partition_size_mb,
                disable_compression,
                run_every_mins,
            )
            .await
        }
        Command::Server => {
            info!("memory limit is {} MB", config.max_mem_mb);
            info!(
                "spill to disk allowed: {}",
                !config.spill_location.is_empty()
            );

            let service = Service::new(config, metadata_db)?;

            let flight_addr: SocketAddr = ([0, 0, 0, 0], 1602).into();
            let flight_server = Server::builder()
                .add_service(FlightServiceServer::new(service.clone()))
                .serve(flight_addr);
            info!("Serving Arrow Flight RPC at {}", flight_addr);

            let jsonl_addr: SocketAddr = ([0, 0, 0, 0], 1603).into();
            let jsonl_server = run_jsonl_server(service, jsonl_addr);
            info!("Serving JSON lines at {}", jsonl_addr);

            tokio::select! {
                result = flight_server => result?,
                result = jsonl_server => result?,
            };
            Err("server shutdown unexpectedly, it should run forever".into())
        }
        Command::AdminApi => {
            let admin_api_addr: SocketAddr = ([0, 0, 0, 0], 1610).into();
            info!("Admin API running at {}", admin_api_addr);

            let registry_service_addr: SocketAddr = ([0, 0, 0, 0], 1611).into();
            info!("Registry service running at {}", registry_service_addr);

            let admin_api = admin_api::serve(admin_api_addr, config.clone());

            let metadata_db = if let Some(url) = &config.metadata_db_url {
                Some(MetadataDb::connect(url).await?)
            } else {
                None
            };
            let registry_service =
                registry_service::serve(registry_service_addr, config, metadata_db);

            // Run the admin api and registry service concurrently
            tokio::select! {
                _ = admin_api => {}
                _ = registry_service => {}
            }

            Err("admin api shutdown unexpectedly, it should run forever".into())
        }
    }
}

async fn run_jsonl_server(service: Service, addr: SocketAddr) -> Result<(), BoxError> {
    let app = axum::Router::new()
        .route(
            "/",
            axum::routing::post(handle_jsonl_request).with_state(service),
        )
        .layer(
            tower_http::compression::CompressionLayer::new()
                .br(true)
                .gzip(true),
        );
    http_common::serve_at(addr, app).await?;
    Ok(())
}

async fn handle_jsonl_request(
    axum::extract::State(service): axum::extract::State<Service>,
    request: String,
) -> axum::response::Response {
    fn error_payload(message: impl std::fmt::Display) -> String {
        format!(r#"{{"error": "{}"}}"#, message)
    }
    let stream = match service.execute_query(&request).await {
        Ok(stream) => stream,
        Err(err) => return axum::response::Response::new(error_payload(err.message()).into()),
    };
    let stream = stream
        .map(|result| -> Result<String, BoxError> {
            let batch = result.map_err(error_payload)?;
            let mut buf: Vec<u8> = Default::default();
            let mut writer = arrow_json::writer::LineDelimitedWriter::new(&mut buf);
            writer.write(&batch)?;
            Ok(String::from_utf8(buf).unwrap())
        })
        .map_err(error_payload);
    axum::response::Response::builder()
        .header("content-type", "application/x-ndjson")
        .body(axum::body::Body::from_stream(stream))
        .unwrap()
}

use std::sync::Arc;

use ampd::{dev_cmd, migrate_cmd, restore_cmd, server_cmd, worker_cmd};
use common::{BoxError, config::Config};
use datasets_common::reference::Reference;

#[cfg(feature = "snmalloc")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[derive(Debug, clap::Parser)]
#[command(version = env!("VERGEN_GIT_DESCRIBE"))]
struct Args {
    /// The configuration file to use. This file defines where to look for dataset definitions and
    /// providers, along with many other configuration options.
    #[arg(long, env = "AMP_CONFIG")]
    config: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Clone, clap::Subcommand)]
enum Command {
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
    /// Run migrations on the metadata database
    Migrate,
    /// Restore dataset snapshots from storage
    Restore {
        /// The name or names of the datasets to restore (comma-separated).
        #[arg(long, required = true, value_delimiter = ',')]
        dataset: Vec<Reference>,
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
    // Initialize tokio-console subscriber if feature is enabled
    #[cfg(feature = "console-subscriber")]
    {
        console_subscriber::init();
        tracing::info!("tokio-console subscriber initialized");
    }

    let Args {
        config: config_path,
        command,
    } = clap::Parser::parse();

    // Log version info
    tracing::info!(
        "version {}, commit {} ({}), built on {}",
        env!("VERGEN_GIT_DESCRIBE"),
        env!("VERGEN_GIT_SHA"),
        env!("VERGEN_GIT_COMMIT_TIMESTAMP"),
        env!("VERGEN_BUILD_DATE"),
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

            let result = worker_cmd::run(config, metadata_db, node_id, metrics_meter).await;

            monitoring::deinit(metrics_provider, tracing_provider)?;

            result?;
            Ok(())
        }
        Command::Controller => {
            let config = load_config(config_path.as_ref(), false).await?;

            let (tracing_provider, metrics_provider, metrics_meter) =
                monitoring::init(config.opentelemetry.as_ref())?;

            let admin_api_addr = config.addrs.admin_api_addr;
            let (addr, server) =
                controller::service::new(Arc::new(config), metrics_meter.as_ref(), admin_api_addr)
                    .await?;

            tracing::info!("Controller Admin API running at {}", addr);
            let result = server.await;

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

    // Gather build info from environment variables set by vergen
    let build_info = common::config::BuildInfo {
        version: env!("VERGEN_GIT_DESCRIBE").to_string(),
        commit_sha: env!("VERGEN_GIT_SHA").to_string(),
        commit_timestamp: env!("VERGEN_GIT_COMMIT_TIMESTAMP").to_string(),
        build_date: env!("VERGEN_BUILD_DATE").to_string(),
    };

    let config = Config::load(config, true, None, allow_temp_db, build_info).await?;
    Ok(config)
}

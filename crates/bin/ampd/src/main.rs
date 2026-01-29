use amp_config::Config;

mod controller_cmd;
mod migrate_cmd;
mod server_cmd;
mod solo_cmd;
mod worker_cmd;

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
    /// Run Amp in local development mode with all services
    #[command(alias = "dev")]
    Solo {
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
    /// Run query server (Arrow Flight, JSON Lines)
    Server {
        /// Enable Arrow Flight RPC Server.
        #[arg(long, env = "FLIGHT_SERVER")]
        flight_server: bool,
        /// Enable JSON Lines Server.
        #[arg(long, env = "JSONL_SERVER")]
        jsonl_server: bool,
    },
    /// Run a distributed worker node
    Worker {
        /// The node id of the worker.
        #[arg(long, env = "AMP_NODE_ID")]
        node_id: String,
    },
    /// Run the controller with Admin API
    Controller,
    /// Run migrations on the metadata database
    Migrate,
}

#[tokio::main]
async fn main() {
    if let Err(err) = main_inner().await {
        // Manually print the error so we can control the format.
        let err = error_with_causes(&err);
        eprintln!("Exiting with error: {err}");
        std::process::exit(1);
    }
}

async fn main_inner() -> Result<(), Error> {
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
        Command::Solo {
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

            let config = load_config(config_path.as_ref(), true)
                .await
                .map_err(Error::LoadConfig)?;

            let (_providers, meter) =
                monitoring::init(config.opentelemetry.as_ref()).map_err(Error::Monitoring)?;

            solo_cmd::run(config, meter, flight_server, jsonl_server, admin_server)
                .await
                .map_err(Error::Solo)?;
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

            let config = load_config(config_path.as_ref(), false)
                .await
                .map_err(Error::LoadConfig)?;
            let addrs = config.addrs.clone();

            let (_providers, meter) =
                monitoring::init(config.opentelemetry.as_ref()).map_err(Error::Monitoring)?;

            server_cmd::run(config, meter, &addrs, flight_server, jsonl_server)
                .await
                .map_err(Error::Server)?;
            Ok(())
        }
        Command::Worker { node_id } => {
            let node_id = node_id.parse().map_err(Error::ParseNodeId)?;

            let config = load_config(config_path.as_ref(), false)
                .await
                .map_err(Error::LoadConfig)?;

            let (_providers, meter) =
                monitoring::init(config.opentelemetry.as_ref()).map_err(Error::Monitoring)?;

            worker_cmd::run(config, meter, node_id)
                .await
                .map_err(Error::Worker)?;
            Ok(())
        }
        Command::Controller => {
            let config = load_config(config_path.as_ref(), false)
                .await
                .map_err(Error::LoadConfig)?;
            let admin_api_addr = config.addrs.admin_api_addr;

            let (_providers, meter) =
                monitoring::init(config.opentelemetry.as_ref()).map_err(Error::Monitoring)?;

            controller_cmd::run(config, meter, admin_api_addr)
                .await
                .map_err(Error::Controller)?;
            Ok(())
        }
        Command::Migrate => {
            let config = load_config(config_path.as_ref(), false)
                .await
                .map_err(Error::LoadConfig)?;

            let (_providers, _meter) =
                monitoring::init(config.opentelemetry.as_ref()).map_err(Error::Monitoring)?;

            migrate_cmd::run(config).await.map_err(Error::Migrate)?;
            Ok(())
        }
    }
}

/// Top-level error type for the `ampd` binary.
///
/// Each variant wraps a command-specific error, providing a unified error type
/// for the main entry point while preserving the full error chain.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to load configuration.
    #[error("Failed to load config: {0}")]
    LoadConfig(#[source] LoadConfigError),

    /// Failed to initialize monitoring/telemetry.
    #[error("Failed to initialize monitoring: {0}")]
    Monitoring(#[source] monitoring::telemetry::ExporterBuildError),

    /// Failed to parse worker node ID.
    #[error("Invalid worker node ID: {0}")]
    ParseNodeId(#[source] worker::node_id::InvalidIdError),

    /// Solo command failed.
    #[error("Solo command failed: {0}")]
    Solo(#[source] solo_cmd::Error),

    /// Server command failed.
    #[error("Server command failed: {0}")]
    Server(#[source] server_cmd::Error),

    /// Worker command failed.
    #[error("Worker command failed: {0}")]
    Worker(#[source] worker_cmd::Error),

    /// Controller command failed.
    #[error("Controller command failed: {0}")]
    Controller(#[source] controller_cmd::Error),

    /// Migrate command failed.
    #[error("Migrate command failed: {0}")]
    Migrate(#[source] migrate_cmd::Error),
}

async fn load_config(
    config_path: Option<&String>,
    allow_temp_db: bool,
) -> Result<Config, LoadConfigError> {
    let Some(config) = config_path else {
        return Err(LoadConfigError::MissingConfigPath);
    };

    // Gather build info from environment variables set by vergen
    let build_info = amp_config::BuildInfo {
        version: env!("VERGEN_GIT_DESCRIBE").to_string(),
        commit_sha: env!("VERGEN_GIT_SHA").to_string(),
        commit_timestamp: env!("VERGEN_GIT_COMMIT_TIMESTAMP").to_string(),
        build_date: env!("VERGEN_BUILD_DATE").to_string(),
    };

    let config = Config::load(config, true, None, allow_temp_db, build_info)
        .await
        .map_err(LoadConfigError::Config)?;
    Ok(config)
}

/// Error type for [`load_config`](crate::load_config).
#[derive(Debug, thiserror::Error)]
#[allow(clippy::large_enum_variant)]
pub enum LoadConfigError {
    /// Config path argument is required but was not provided.
    #[error("--config parameter is mandatory")]
    MissingConfigPath,

    /// Failed to load configuration from file.
    #[error("Failed to load config: {0}")]
    Config(#[source] amp_config::ConfigError),
}

/// Builds an error chain string from an error and its sources.
fn error_with_causes(err: &dyn std::error::Error) -> String {
    let mut error_chain = Vec::new();
    let mut current = err;
    while let Some(source) = current.source() {
        error_chain.push(source.to_string());
        current = source;
    }

    if error_chain.is_empty() {
        err.to_string()
    } else {
        format!("{} | Caused by: {}", err, error_chain.join(" -> "))
    }
}

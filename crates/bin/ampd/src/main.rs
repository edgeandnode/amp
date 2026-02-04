use std::{env, path::PathBuf};

use amp_config::{DEFAULT_AMP_DIR_NAME, DEFAULT_CONFIG_FILENAME};

mod build_info;
mod config;
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
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Clone, clap::Subcommand)]
enum Command {
    /// Run Amp in local development mode with all services
    #[command(alias = "dev")]
    Solo {
        /// Directory for Amp daemon state and data. Defaults to `<cwd>/.amp/`.
        /// If `--config` is not provided, searches for `config.toml` within this directory.
        #[arg(long, env = "AMP_DIR")]
        amp_dir: Option<PathBuf>,
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
            amp_dir,
            mut flight_server,
            mut jsonl_server,
            mut admin_server,
        } => {
            if !flight_server && !jsonl_server && !admin_server {
                flight_server = true;
                jsonl_server = true;
                admin_server = true;
            }

            // Resolve the amp state directory, creating it if needed.
            let amp_dir = match amp_dir {
                Some(dir) => dir,
                None => env::current_dir()
                    .map_err(Error::CurrentDir)?
                    .join(DEFAULT_AMP_DIR_NAME),
            };
            amp_config::ensure_amp_dir_root(&amp_dir).map_err(Error::AmpDirRoot)?;
            amp_config::ensure_amp_base_directories(&amp_dir).map_err(Error::AmpBaseDirs)?;

            // Use explicit --config if provided, otherwise look for config.toml in amp_dir.
            let config_path = config_path.or_else(|| {
                let path = amp_dir.join(DEFAULT_CONFIG_FILENAME);
                path.exists().then_some(path)
            });
            if let Some(path) = &config_path
                && !path.is_file()
            {
                return Err(Error::ConfigNotAFile(path.clone()));
            }

            solo_cmd::run(
                amp_dir,
                config_path,
                flight_server,
                jsonl_server,
                admin_server,
            )
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

            let config_path = config_path.as_ref().ok_or(Error::MissingConfigPath)?;
            let config = config::load(config_path).map_err(Error::LoadConfig)?;
            let metadata_db_config = amp_config::metadb::load(config_path, None)
                .ok_or(Error::MissingMetadataDbConfig)?;
            let addrs = config.server_addrs.clone();

            let (_providers, meter) =
                monitoring::init(config.opentelemetry.as_ref()).map_err(Error::Monitoring)?;

            server_cmd::run(
                config,
                &metadata_db_config,
                meter,
                &addrs,
                flight_server,
                jsonl_server,
            )
            .await
            .map_err(Error::Server)?;
            Ok(())
        }
        Command::Worker { node_id } => {
            let node_id = node_id.parse().map_err(Error::ParseNodeId)?;

            let config_path = config_path.as_ref().ok_or(Error::MissingConfigPath)?;
            let config = config::load(config_path).map_err(Error::LoadConfig)?;
            let metadata_db_config = amp_config::metadb::load(config_path, None)
                .ok_or(Error::MissingMetadataDbConfig)?;

            let (_providers, meter) =
                monitoring::init(config.opentelemetry.as_ref()).map_err(Error::Monitoring)?;

            let build_info = build_info::load();
            worker_cmd::run(build_info, config, &metadata_db_config, meter, node_id)
                .await
                .map_err(Error::Worker)?;
            Ok(())
        }
        Command::Controller => {
            let config_path = config_path.as_ref().ok_or(Error::MissingConfigPath)?;
            let config = config::load(config_path).map_err(Error::LoadConfig)?;
            let metadata_db_config = amp_config::metadb::load(config_path, None)
                .ok_or(Error::MissingMetadataDbConfig)?;
            let admin_api_addr = config.controller_addrs.admin_api_addr;

            let (_providers, meter) =
                monitoring::init(config.opentelemetry.as_ref()).map_err(Error::Monitoring)?;

            let build_info = build_info::load();
            controller_cmd::run(
                build_info,
                config,
                &metadata_db_config,
                meter,
                admin_api_addr,
            )
            .await
            .map_err(Error::Controller)?;
            Ok(())
        }
        Command::Migrate => {
            let config_path = config_path.as_ref().ok_or(Error::MissingConfigPath)?;
            let config = config::load(config_path).map_err(Error::LoadConfig)?;
            let metadata_db_config = amp_config::metadb::load(config_path, None)
                .ok_or(Error::MissingMetadataDbConfig)?;

            let (_providers, _meter) =
                monitoring::init(config.opentelemetry.as_ref()).map_err(Error::Monitoring)?;

            migrate_cmd::run(&metadata_db_config)
                .await
                .map_err(Error::Migrate)?;
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
    /// Failed to determine current working directory.
    #[error("Failed to determine current directory: {0}")]
    CurrentDir(#[source] std::io::Error),

    /// Failed to create amp directory root.
    #[error("Failed to create amp directory root: {0}")]
    AmpDirRoot(#[source] amp_config::EnsureAmpDirRootError),

    /// Failed to create amp base directories.
    #[error("Failed to create amp base directories: {0}")]
    AmpBaseDirs(#[source] amp_config::EnsureAmpBaseDirectoriesError),

    /// Config path exists but is not a file.
    #[error("config path is not a file: {0}")]
    ConfigNotAFile(PathBuf),

    /// Config path argument is required but was not provided.
    #[error("--config parameter is mandatory")]
    MissingConfigPath,

    /// Failed to load configuration.
    #[error("Failed to load config: {0}")]
    LoadConfig(#[source] config::LoadError),

    /// Missing required metadata database configuration.
    #[error(
        "Missing required metadata database configuration. Provide metadata_db.url via TOML config file or AMP_CONFIG_METADATA_DB__URL environment variable"
    )]
    MissingMetadataDbConfig,

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

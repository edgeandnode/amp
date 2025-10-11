mod batch_utils;
mod config;
mod conn;
mod manifest;
mod sql_validator;
mod stream_manager;
mod stream_task;
mod sync_engine;
mod version_polling;

use amp_client::SqlClient;
use clap::Parser as _;
use common::BoxError;
use conn::{DEFAULT_POOL_SIZE, DbConnPool};
use datasets_common::{name::Name, version::Version};
use tracing::{error, info};

use crate::{
    config::AmpsyncConfig,
    stream_manager::{shutdown_streams_gracefully, spawn_stream_tasks},
    sync_engine::AmpsyncDbEngine,
    version_polling::version_poll_task,
};

#[cfg(feature = "snmalloc")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[derive(Clone, Debug, clap::Subcommand)]
enum Command {
    Sync {
        /// The name of the dataset to sync into the configured postgres database.
        #[arg(long, required = true, env = "DATASET_NAME")]
        dataset_name: Name,

        /// (Optional) The specific dataset version to pull the schema for.
        /// If provided, the version is validated to be found from the admin-api;
        /// if not found, an error is thrown.
        ///
        /// If not provided:
        /// - the latest version from the dataset versions endpoint schema is pulled.
        /// - the versions endpoint is polled from the admin-api and any newly found versions are fetched
        #[arg(long, required = false, env = "DATASET_VERSION")]
        dataset_version: Option<Version>,

        /// Address of the amp arrow flight url.
        /// Used to stream the arrow queries to fetch data and insert into the configured database.
        ///
        /// # default
        /// http://localhost:1602
        #[arg(long, default_value = "http://localhost:1602", env = "AMP_FLIGHT_ADDR")]
        amp_flight_addr: String,

        /// Address of the amp admin-api.
        /// Used to fetch the dataset versions and schema information.
        ///
        /// # default
        /// http://localhost:1610
        #[arg(
            long,
            default_value = "http://localhost:1610",
            env = "AMP_ADMIN_API_ADDR"
        )]
        amp_admin_api_addr: String,

        /// If no DATASET_VERSION value is provided, this is the time, in seconds, that we poll
        /// the admin-api to fetch dataset versions.
        #[arg(long, default_value = "5", env = "VERSION_POLL_INTERVAL_SECS")]
        version_polling_interval_secs: u64,

        /// Postgres database url.
        /// Either this value, or the database_user/host combo is required.
        /// Provides the URL of the database to sync data in to.
        ///
        /// # Format
        /// `postgres{ql}://{user}{:password}@{host}:{port}/{database_name}
        #[arg(long, env = "DATABASE_URL")]
        database_url: Option<String>,

        /// Postgres database user.
        /// Either this AND the database_host and database_name are required.
        /// OR, the database_url is required.
        #[arg(long, short = 'u', env = "DATABASE_USER")]
        database_user: Option<String>,

        /// (Optional) Postgres database user password.
        #[arg(long, short = 'p', env = "DATABASE_PASSWORD")]
        database_password: Option<String>,

        /// Postgres database host.
        /// Either this AND the database_user and database_name are required.
        /// OR, the database_url is required.
        #[arg(long, short = 'h', env = "DATABASE_HOST")]
        database_host: Option<String>,

        /// Postgres database port.
        ///
        /// # default
        /// 5432
        #[arg(long, env = "DATABASE_PORT", default_value = "5432")]
        database_port: u16,

        /// Postgres database to sync data into.
        /// Either this AND the database_user and database_host are required.
        /// OR, the database_url is required.
        #[arg(long, env = "DATABASE_NAME")]
        database_name: Option<String>,
    },
}

#[derive(Debug, clap::Parser)]
#[command(version = "0.1.0")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[tokio::main]
async fn main() {
    match ampsync_runner().await {
        Ok(()) => {}
        Err(e) => {
            eprintln!("Exiting with error: {e}");
            std::process::exit(1);
        }
    }
}

/// Main ampsync orchestrator.
///
/// Coordinates:
/// - Configuration loading
/// - Database and Nozzle client connections
/// - Stream task spawning and management
/// - Version polling (when enabled)
/// - Graceful shutdown on signals
///
/// Supports version polling: when DATASET_VERSION is not specified, polls for new versions
/// and gracefully reloads when a new version is detected.
async fn ampsync_runner() -> Result<(), BoxError> {
    monitoring::logging::init();

    let Args { command } = Args::parse();

    match command {
        Command::Sync {
            dataset_name,
            dataset_version,
            amp_admin_api_addr,
            amp_flight_addr,
            version_polling_interval_secs,
            database_url,
            database_host,
            database_port,
            database_user,
            database_password,
            database_name,
        } => {
            let mut config = AmpsyncConfig::from_cmd(
                dataset_name,
                dataset_version,
                amp_admin_api_addr,
                amp_flight_addr,
                version_polling_interval_secs,
                database_url,
                database_host,
                database_port,
                database_user,
                database_password,
                database_name,
            )
            .await?;
            let version_polling_enabled = config.dataset_version.is_none();
            if version_polling_enabled {
                info!("Starting ampsync with version polling enabled");
            } else {
                info!(
                    dataset_version = %config.dataset_version.as_ref().unwrap(),
                    "Starting ampsync with fixed version (version polling disabled)"
                );
            }

            info!(
                dataset_name = %config.dataset_name,
                dataset_version = %config.manifest.version,
                "manifest_loaded"
            );

            // Connect to target database (reused across reloads)
            let db_pool = DbConnPool::connect(&config.database_url, DEFAULT_POOL_SIZE).await?;

            // Connect to Nozzle server (reused across reloads)
            let sql_client = SqlClient::new(&config.amp_flight_addr).await?;
            info!(
                flight_addr = %config.amp_flight_addr,
                "nozzle_server_connected"
            );

            let ampsync_db_engine = AmpsyncDbEngine::new(&db_pool);

            // Initialize checkpoint tracking table
            ampsync_db_engine.init_checkpoint_table().await?;
            info!("checkpoint_tracking_initialized");

            // Set up version polling (only if DATASET_VERSION not specified)
            let (version_change_tx, mut version_change_rx) =
                tokio::sync::mpsc::channel::<Version>(1);
            let version_poll_handle = if version_polling_enabled {
                let admin_api_addr = config.amp_admin_api_addr.clone();
                let dataset_name = config.dataset_name.clone();
                let poll_interval = config.version_poll_interval_secs;
                let current_version = config.manifest.version.clone();

                info!(
                    poll_interval_secs = poll_interval,
                    "version_polling_initialized"
                );

                Some(tokio::spawn(async move {
                    version_poll_task(
                        admin_api_addr,
                        dataset_name,
                        current_version,
                        poll_interval,
                        version_change_tx,
                    )
                    .await
                }))
            } else {
                info!("version_polling_disabled");
                None
            };

            // Setup signal handlers for graceful shutdown
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
            let mut sigint =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;

            // Main reload loop
            loop {
                info!(
                    dataset_name = %config.manifest.name,
                    dataset_version = %config.manifest.version,
                    table_count = config.manifest.tables.len(),
                    "starting_dataset_sync"
                );

                // Spawn stream processing tasks for current configuration
                let shutdown_token = tokio_util::sync::CancellationToken::new();
                let task_handles = spawn_stream_tasks(
                    &config,
                    &sql_client,
                    &ampsync_db_engine,
                    shutdown_token.clone(),
                )
                .await?;

                // Wait for version change, or shutdown signal
                tokio::select! {
                    // New version detected - reload configuration
                    Some(new_version) = version_change_rx.recv() => {
                        info!(
                            old_version = %config.manifest.version,
                            new_version = %new_version,
                            "version_change_detected"
                        );

                        // Gracefully stop all streams
                        shutdown_token.cancel();
                        shutdown_streams_gracefully(task_handles).await;

                        // Fetch new manifest with the detected version
                        match manifest::fetch_manifest(&config.amp_admin_api_addr, &config.dataset_name, Some(&new_version)).await {
                            Ok(new_manifest) => {
                                info!(
                                    dataset_name = %new_manifest.name,
                                    dataset_version = %new_manifest.version,
                                    table_count = new_manifest.tables.len(),
                                    "version_reload_success"
                                );
                                config.manifest = std::sync::Arc::new(new_manifest);
                                // Loop continues with new manifest
                            }
                            Err(e) => {
                                error!(
                                    error = %e,
                                    dataset_name = %config.dataset_name,
                                    new_version = %new_version,
                                    "version_reload_failed"
                                );
                                error!("Keeping previous configuration active");
                                // Loop continues with old config (safe fallback)
                            }
                        }
                    }

                    _ = sigterm.recv() => {
                        info!(signal = "SIGTERM", "shutdown_signal_received");
                        shutdown_token.cancel();
                        shutdown_streams_gracefully(task_handles).await;
                        if let Some(handle) = version_poll_handle {
                            handle.abort();
                        }
                        break;
                    }

                    _ = sigint.recv() => {
                        info!(signal = "SIGINT", "shutdown_signal_received");
                        shutdown_token.cancel();
                        shutdown_streams_gracefully(task_handles).await;
                        if let Some(handle) = version_poll_handle {
                            handle.abort();
                        }
                        break;
                    }
                }
            }
        }
    }

    info!("shutdown_complete");
    Ok(())
}

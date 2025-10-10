mod batch_utils;
mod config;
mod conn;
mod manifest;
mod sql_validator;
mod stream_manager;
mod stream_task;
mod sync_engine;
mod version_polling;

use common::BoxError;
use conn::{DEFAULT_POOL_SIZE, DbConnPool};
use nozzle_client::SqlClient;
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
    // Initialize logging
    monitoring::logging::init();

    let mut config = AmpsyncConfig::from_env().await?;

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
        tokio::sync::mpsc::channel::<datasets_common::version::Version>(1);
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
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;

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

            // SIGTERM received - graceful shutdown
            _ = sigterm.recv() => {
                info!(signal = "SIGTERM", "shutdown_signal_received");
                shutdown_token.cancel();
                shutdown_streams_gracefully(task_handles).await;
                if let Some(handle) = version_poll_handle {
                    handle.abort();
                }
                break;
            }

            // SIGINT received - graceful shutdown
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

    info!("shutdown_complete");
    Ok(())
}

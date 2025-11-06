use std::{future::Future, pin::Pin, sync::Arc};

use common::{
    BoxError,
    config::{BuildInfo, Config},
};
use figment::providers::Serialized;
use tempdb::KEEP_TEMP_DIRS;

pub async fn run(
    config_path: &str,
    build_info: BuildInfo,
    flight_server: bool,
    jsonl_server: bool,
    admin_server: bool,
    temp_db: bool,
) -> Result<(), Error> {
    // Conditionally create temp-db for dev command
    let (tempdb_handle, tempdb_fut): (_, Pin<Box<dyn Future<Output = _> + Send>>) = if temp_db {
        let (tempdb_handle, fut) = tempdb::service::new(*KEEP_TEMP_DIRS);
        tracing::info!("Using temporary database for dev environment");
        (Some(tempdb_handle), Box::pin(fut))
    } else {
        (None, Box::pin(std::future::pending()))
    };

    // Override config with temp-db URL if temp-db is enabled
    let config_override = tempdb_handle.as_ref().map(|handle| {
        figment::Figment::from(Serialized::defaults(("metadata_db.url", handle.url())))
    });

    // Load config with optional temp-db URL override
    let config = Config::load(config_path, true, config_override, build_info)
        .await
        .map_err(Error::ConfigLoad)?;

    // Initialize monitoring with config's opentelemetry settings
    let (tracing_provider, metrics_provider, meter) =
        monitoring::init(config.opentelemetry.as_ref())
            .map_err(|e| Error::MonitoringInit(Box::new(e)))?;

    let metadata_db = config
        .metadata_db()
        .await
        .map_err(Error::MetadataDbConnect)?;

    let worker_id = "worker".parse().expect("Invalid worker ID");
    let config = Arc::new(config);

    // Spawn controller (Admin API) if enabled
    let controller_fut: Pin<Box<dyn Future<Output = _> + Send>> = if admin_server {
        let (addr, fut) =
            controller::service::new(config.clone(), meter.as_ref(), config.addrs.admin_api_addr)
                .await
                .map_err(Error::ServiceInit)?;

        tracing::info!("Controller Admin API running at {}", addr);
        Box::pin(fut)
    } else {
        Box::pin(std::future::pending())
    };

    // Spawn server only if at least one query server is enabled
    let server_fut: Pin<Box<dyn Future<Output = _> + Send>> = if flight_server || jsonl_server {
        let flight_at = if flight_server {
            Some(config.addrs.flight_addr)
        } else {
            None
        };
        let jsonl_at = if jsonl_server {
            Some(config.addrs.jsonl_addr)
        } else {
            None
        };
        let (addrs, fut) = server::service::new(
            config.clone(),
            metadata_db.clone(),
            flight_at,
            jsonl_at,
            meter.as_ref(),
        )
        .await
        .map_err(|err| Error::ServerRun(Box::new(err)))?;

        if let Some(addr) = addrs.flight_addr {
            tracing::info!("Arrow Flight RPC Server running at {}", addr);
        }
        if let Some(addr) = addrs.jsonl_addr {
            tracing::info!("JSON Lines Server running at {}", addr);
        }

        Box::pin(fut)
    } else {
        Box::pin(std::future::pending())
    };

    // Create worker future
    let worker_fut = worker::service::new(worker_id, config.clone(), metadata_db, meter.clone());

    // Wait for worker, server, controller, or tempdb to complete
    let result = tokio::select! {biased;
        _ = tempdb_fut => Err(Error::TempDbStopped),
        res = controller_fut => res.map_err(Error::ControllerRuntime),
        res = worker_fut => res.map_err(Error::WorkerRuntime),
        res = server_fut => res.map_err(Error::ServerRuntime),
    };

    // Deinitialize monitoring
    monitoring::deinit(metrics_provider, tracing_provider)
        .map_err(|e| Error::MonitoringDeinit(Box::new(std::io::Error::other(e))))?;

    result
}

/// Errors that can occur during dev mode execution.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to load configuration.
    ///
    /// This occurs when the configuration file cannot be read or parsed.
    #[error("Failed to load config: {0}")]
    ConfigLoad(#[source] common::config::ConfigError),

    /// Failed to initialize monitoring.
    ///
    /// This occurs when monitoring (tracing/metrics) initialization fails.
    #[error("Failed to initialize monitoring: {0}")]
    MonitoringInit(#[source] BoxError),

    /// Failed to deinitialize monitoring.
    ///
    /// This occurs when monitoring cleanup fails.
    #[error("Failed to deinitialize monitoring: {0}")]
    MonitoringDeinit(#[source] BoxError),

    /// Failed to connect to metadata database.
    ///
    /// This occurs when the metadata database connection cannot be established.
    #[error("Failed to connect to metadata database: {0}")]
    MetadataDbConnect(#[source] common::config::ConfigError),

    /// Temporary database stopped unexpectedly.
    ///
    /// This occurs when the temporary database service stops before the dev command completes.
    #[error("Temporary database stopped unexpectedly")]
    TempDbStopped,

    /// Failed to initialize the controller service (Admin API).
    ///
    /// This occurs during the initialization phase when attempting to bind and
    /// start the Admin API server.
    #[error("Failed to initialize controller service: {0}")]
    ServiceInit(#[source] controller::service::Error),

    /// Failed to start the query server (Arrow Flight RPC and/or JSON Lines).
    ///
    /// This occurs during the initialization phase when attempting to bind and
    /// start the query servers.
    #[error("Failed to start server: {0}")]
    ServerRun(#[source] BoxError),

    /// Controller service (Admin API) encountered a runtime error.
    ///
    /// This occurs after the Admin API server has started successfully but
    /// encounters an error during operation.
    #[error("Controller runtime error: {0}")]
    ControllerRuntime(#[source] BoxError),

    /// Query server encountered a runtime error.
    ///
    /// This occurs after the Arrow Flight RPC and/or JSON Lines servers have
    /// started successfully but encounter an error during operation.
    #[error("Server runtime error: {0}")]
    ServerRuntime(#[source] BoxError),

    /// Worker encountered a runtime error.
    ///
    /// This occurs when the worker process encounters an error during operation.
    #[error("Worker runtime error: {0}")]
    WorkerRuntime(#[source] worker::Error),
}

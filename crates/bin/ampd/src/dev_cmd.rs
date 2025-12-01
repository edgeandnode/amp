use std::{future::Future, pin::Pin, sync::Arc};

use common::{BoxError, config::Config as CommonConfig};
use metadata_db::MetadataDb;

use crate::server_cmd::config_from_common;

pub async fn run(
    config: CommonConfig,
    metadata_db: MetadataDb,
    flight_server: bool,
    jsonl_server: bool,
    admin_server: bool,
    meter: Option<monitoring::telemetry::metrics::Meter>,
) -> Result<(), Error> {
    let worker_id = "worker".parse().expect("Invalid worker ID");
    let config = Arc::new(config);

    // Convert to server-specific config
    let server_config = Arc::new(config_from_common(&config));

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
            server_config.clone(),
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

    // Initialize worker
    let worker_fut = worker::service::new(worker_id, config.clone(), metadata_db, meter.clone())
        .await
        .map_err(Error::WorkerInit)?;

    // Wait for worker, server, or controller to complete
    tokio::select! {biased;
        res = controller_fut => res.map_err(Error::ControllerRuntime)?,
        res = worker_fut => res.map_err(Error::WorkerRuntime)?,
        res = server_fut => res.map_err(Error::ServerRuntime)?,
    }

    Ok(())
}

/// Errors that can occur during dev mode execution.
#[derive(Debug, thiserror::Error)]
pub enum Error {
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

    /// Failed to initialize the worker service.
    ///
    /// This occurs during the worker initialization phase (registration, heartbeat
    /// setup, notification listener setup, or bootstrap).
    #[error("Failed to initialize worker: {0}")]
    WorkerInit(#[source] worker::service::InitError),

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
    WorkerRuntime(#[source] worker::service::RuntimeError),
}

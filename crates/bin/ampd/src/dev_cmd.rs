use std::{pin::Pin, sync::Arc};

use common::{BoxError, config::Config};
use metadata_db::MetadataDb;
use worker::Worker;

use crate::server_cmd;

pub async fn run(
    config: Config,
    metadata_db: MetadataDb,
    flight_server: bool,
    jsonl_server: bool,
    admin_server: bool,
    meter: Option<monitoring::telemetry::metrics::Meter>,
) -> Result<(), Error> {
    let worker_id = "worker".parse().expect("Invalid worker ID");
    let config = Arc::new(config);

    // Spawn controller (Admin API) if enabled
    let controller_fut: Pin<Box<dyn Future<Output = _> + Send>> = if admin_server {
        let (addr, fut) =
            controller::serve(config.addrs.admin_api_addr, config.clone(), meter.as_ref())
                .await
                .map_err(Error::ControllerServe)?;

        tracing::info!("Controller Admin API running at {}", addr);
        Box::pin(fut)
    } else {
        Box::pin(std::future::pending())
    };

    // Spawn server only if at least one query server is enabled
    let server_fut: Pin<Box<dyn Future<Output = _> + Send>> = if flight_server || jsonl_server {
        let (addrs, fut) = server_cmd::run_servers(
            config.clone(),
            metadata_db.clone(),
            flight_server,
            jsonl_server,
            meter.as_ref(),
        )
        .await
        .map_err(Error::ServerRun)?;

        if flight_server {
            tracing::info!("Arrow Flight RPC Server running at {}", addrs.flight_addr);
        }
        if jsonl_server {
            tracing::info!("JSON Lines Server running at {}", addrs.jsonl_addr);
        }

        Box::pin(fut)
    } else {
        Box::pin(std::future::pending())
    };

    // Create worker future
    let worker = Worker::new(config, metadata_db, worker_id, meter.clone());
    let worker_fut = worker.run();

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
    /// Failed to start the controller service (Admin API).
    ///
    /// This occurs during the initialization phase when attempting to bind and
    /// start the Admin API server.
    #[error("Failed to start controller service: {0}")]
    ControllerServe(#[source] BoxError),

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

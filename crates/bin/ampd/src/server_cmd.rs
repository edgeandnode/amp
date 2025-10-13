use std::sync::Arc;

use common::{BoxError, config::Config};
use metadata_db::MetadataDb;

pub async fn run(
    config: Config,
    metadata_db: MetadataDb,
    flight_server: bool,
    jsonl_server: bool,
    meter: Option<&monitoring::telemetry::metrics::Meter>,
) -> Result<(), Error> {
    let config = Arc::new(config);

    if config.max_mem_mb == 0 {
        tracing::info!("Memory limit is unlimited");
    } else {
        tracing::info!("Memory limit is {} MB", config.max_mem_mb);
    }

    tracing::info!(
        "Spill to disk allowed: {}",
        !config.spill_location.is_empty()
    );

    let (addrs, server) = server::serve(config, metadata_db, flight_server, jsonl_server, meter)
        .await
        .map_err(Error::ServerStart)?;

    if flight_server {
        tracing::info!("Arrow Flight RPC server running at {}", addrs.flight_addr);
    }
    if jsonl_server {
        tracing::info!("JSON Lines server running at {}", addrs.jsonl_addr);
    }

    server.await.map_err(Error::ServerRuntime)
}

/// Errors that can occur during server execution.
///
/// This error type covers all failure modes when running the query servers,
/// which provide Arrow Flight RPC and JSON Lines query interfaces.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to start the query server.
    ///
    /// This occurs during the initialization phase when attempting to bind and
    /// start the Arrow Flight RPC and/or JSON Lines servers.
    #[error("Failed to start server: {0}")]
    ServerStart(#[source] BoxError),

    /// Query server encountered a runtime error.
    ///
    /// This occurs after the servers have started successfully but encounter
    /// an error during operation.
    #[error("Server runtime error: {0}")]
    ServerRuntime(#[source] BoxError),
}

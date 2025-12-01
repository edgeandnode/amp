use std::sync::Arc;

use common::{
    BoxError,
    config::{Addrs, Config as CommonConfig},
};
use metadata_db::MetadataDb;
use server::config::Config as ServerConfig;

pub async fn run(
    server_config: ServerConfig,
    metadata_db: MetadataDb,
    addrs: &Addrs,
    flight_server: bool,
    jsonl_server: bool,
    meter: Option<&monitoring::telemetry::metrics::Meter>,
) -> Result<(), Error> {
    if server_config.max_mem_mb == 0 {
        tracing::info!("Memory limit is unlimited");
    } else {
        tracing::info!("Memory limit is {} MB", server_config.max_mem_mb);
    }

    tracing::info!(
        "Spill to disk allowed: {}",
        !server_config.spill_location.is_empty()
    );

    let flight_at = if flight_server {
        Some(addrs.flight_addr)
    } else {
        None
    };
    let jsonl_at = if jsonl_server {
        Some(addrs.jsonl_addr)
    } else {
        None
    };

    let (addrs, server) = server::service::new(
        Arc::new(server_config),
        metadata_db,
        flight_at,
        jsonl_at,
        meter,
    )
    .await
    .map_err(|err| Error::ServerStart(Box::new(err)))?;

    if let Some(addr) = addrs.flight_addr {
        tracing::info!("Arrow Flight RPC server running at {}", addr);
    }
    if let Some(addr) = addrs.jsonl_addr {
        tracing::info!("JSON Lines server running at {}", addr);
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

/// Convert common config to server-specific config
pub fn config_from_common(config: &CommonConfig) -> ServerConfig {
    ServerConfig {
        providers_store: config.providers_store.clone(),
        manifests_store: config.manifests_store.clone(),
        server_microbatch_max_interval: config.server_microbatch_max_interval,
        keep_alive_interval: config.keep_alive_interval,
        max_mem_mb: config.max_mem_mb,
        query_max_mem_mb: config.query_max_mem_mb,
        spill_location: config.spill_location.clone(),
        parquet_cache_size_mb: config.parquet.cache_size_mb,
    }
}

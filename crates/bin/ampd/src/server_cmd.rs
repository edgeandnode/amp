use std::sync::Arc;

use amp_config::{Addrs, Config as CommonConfig};
use amp_data_store::DataStore;
use amp_dataset_store::{
    DatasetStore, manifests::DatasetManifestsStore, providers::ProviderConfigsStore,
};
use amp_object_store::ObjectStoreCreationError;
use common::BoxError;
use monitoring::telemetry::metrics::Meter;
use server::config::Config as ServerConfig;

pub async fn run(
    config: CommonConfig,
    meter: Option<Meter>,
    addrs: &Addrs,
    flight_server: bool,
    jsonl_server: bool,
) -> Result<(), Error> {
    let metadata_db = config
        .metadata_db()
        .await
        .map_err(|err| Error::MetadataDbConnection(Box::new(err)))?;

    let data_store = DataStore::new(
        metadata_db.clone(),
        config.data_store_url.clone(),
        config.parquet.cache_size_mb,
    )
    .map_err(Error::DataStoreCreation)?;

    let dataset_store = {
        let provider_configs_store = ProviderConfigsStore::new(
            amp_object_store::new_with_prefix(
                &config.providers_store_url,
                config.providers_store_url.path(),
            )
            .map_err(Error::ProvidersStoreCreation)?,
        );
        let dataset_manifests_store = DatasetManifestsStore::new(
            amp_object_store::new_with_prefix(
                &config.manifests_store_url,
                config.manifests_store_url.path(),
            )
            .map_err(Error::ManifestsStoreCreation)?,
        );
        DatasetStore::new(
            metadata_db.clone(),
            provider_configs_store,
            dataset_manifests_store,
        )
    };

    let server_config = config_from_common(&config);

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
        data_store,
        dataset_store,
        meter,
        flight_at,
        jsonl_at,
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
    /// Failed to connect to metadata database
    ///
    /// This occurs when the server cannot establish a connection to the
    /// PostgreSQL metadata database.
    #[error("Failed to connect to metadata database: {0}")]
    MetadataDbConnection(#[source] Box<amp_config::ConfigError>),

    /// Failed to create data store
    ///
    /// This occurs when the data store cannot be created from the configured URL.
    #[error("Failed to create data store: {0}")]
    DataStoreCreation(#[source] ObjectStoreCreationError),

    /// Failed to create providers store
    ///
    /// This occurs when the providers store cannot be created from the configured URL.
    #[error("Failed to create providers store: {0}")]
    ProvidersStoreCreation(#[source] ObjectStoreCreationError),

    /// Failed to create manifests store
    ///
    /// This occurs when the manifests store cannot be created from the configured URL.
    #[error("Failed to create manifests store: {0}")]
    ManifestsStoreCreation(#[source] ObjectStoreCreationError),

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
        server_microbatch_max_interval: config.server_microbatch_max_interval,
        keep_alive_interval: config.keep_alive_interval,
        max_mem_mb: config.max_mem_mb,
        query_max_mem_mb: config.query_max_mem_mb,
        spill_location: config.spill_location.clone(),
    }
}

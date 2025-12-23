use std::{future::Future, pin::Pin, sync::Arc};

use amp_config::Config as CommonConfig;
use amp_data_store::DataStore;
use amp_dataset_store::{
    DatasetStore, manifests::DatasetManifestsStore, providers::ProviderConfigsStore,
};
use amp_object_store::ObjectStoreCreationError;
use common::BoxError;
use monitoring::telemetry::metrics::Meter;

use crate::{controller_cmd, server_cmd, worker_cmd};

pub async fn run(
    config: CommonConfig,
    meter: Option<Meter>,
    flight_server: bool,
    jsonl_server: bool,
    admin_server: bool,
) -> Result<(), Error> {
    let worker_id = "worker".parse().expect("Invalid worker ID");

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

    // Spawn controller (Admin API) if enabled
    let controller_fut: Pin<Box<dyn Future<Output = _> + Send>> = if admin_server {
        let controller_config = controller_cmd::config_from_common(&config);
        let (addr, fut) = controller::service::new(
            Arc::new(controller_config),
            metadata_db.clone(),
            data_store.clone(),
            dataset_store.clone(),
            meter.clone(),
            config.addrs.admin_api_addr,
        )
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

        let server_config = server_cmd::config_from_common(&config);
        let (addrs, fut) = server::service::new(
            Arc::new(server_config),
            metadata_db.clone(),
            data_store.clone(),
            dataset_store.clone(),
            meter.clone(),
            flight_at,
            jsonl_at,
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
    let worker_config = worker_cmd::config_from_common(&config);
    let worker_fut = worker::service::new(
        worker_config,
        metadata_db,
        data_store,
        dataset_store,
        meter,
        worker_id,
    )
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

/// Errors that can occur during solo mode execution.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to connect to metadata database
    ///
    /// This occurs when the solo command cannot establish a connection to the
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

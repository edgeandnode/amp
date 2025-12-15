use std::sync::Arc;

use common::{config::Config, store::Store};
use dataset_store::{
    DatasetStore, manifests::DatasetManifestsStore, providers::ProviderConfigsStore,
};
use monitoring::telemetry::metrics::Meter;
use worker::node_id::NodeId;

pub async fn run(config: Config, meter: Option<Meter>, node_id: NodeId) -> Result<(), Error> {
    let metadata_db = config
        .metadata_db()
        .await
        .map_err(|err| Error::MetadataDbConnection(Box::new(err)))?;

    let data_store = Store::new(config.data_store_url.clone())
        .map(Arc::new)
        .map_err(Error::DataStoreCreation)?;
    let providers_store =
        Store::new(config.providers_store_url.clone()).map_err(Error::ProvidersStoreCreation)?;
    let manifests_store =
        Store::new(config.manifests_store_url.clone()).map_err(Error::ManifestsStoreCreation)?;

    let dataset_store = {
        let provider_configs_store = ProviderConfigsStore::new(providers_store.prefixed_store());
        let dataset_manifests_store = DatasetManifestsStore::new(manifests_store.prefixed_store());
        DatasetStore::new(
            metadata_db.clone(),
            provider_configs_store,
            dataset_manifests_store,
        )
    };

    // Convert common config to worker-specific config
    let worker_config = config_from_common(&config);

    // Initialize the worker (setup phase)
    let worker_fut = worker::service::new(
        worker_config,
        metadata_db,
        data_store,
        dataset_store,
        meter,
        node_id,
    )
    .await
    .map_err(Error::Init)?;

    // Run the worker (runtime phase)
    worker_fut.await.map_err(Error::Runtime)
}

/// Errors that can occur during worker execution.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to connect to metadata database
    ///
    /// This occurs when the worker cannot establish a connection to the
    /// PostgreSQL metadata database.
    #[error("Failed to connect to metadata database: {0}")]
    MetadataDbConnection(#[source] Box<common::config::ConfigError>),

    /// Failed to create data store
    ///
    /// This occurs when the data store cannot be created from the configured URL.
    #[error("Failed to create data store: {0}")]
    DataStoreCreation(#[source] common::store::StoreError),

    /// Failed to create providers store
    ///
    /// This occurs when the providers store cannot be created from the configured URL.
    #[error("Failed to create providers store: {0}")]
    ProvidersStoreCreation(#[source] common::store::StoreError),

    /// Failed to create manifests store
    ///
    /// This occurs when the manifests store cannot be created from the configured URL.
    #[error("Failed to create manifests store: {0}")]
    ManifestsStoreCreation(#[source] common::store::StoreError),

    /// Worker initialization failed.
    ///
    /// This occurs during the initialization phase (registration, heartbeat
    /// setup, notification listener setup, or bootstrap).
    #[error("Worker initialization failed")]
    Init(#[source] worker::service::InitError),

    /// Worker runtime error.
    ///
    /// This occurs during the worker's main event loop after successful initialization.
    #[error("Worker runtime error: {0}")]
    Runtime(#[source] worker::service::RuntimeError),
}

/// Convert common::config::Config to worker::config::Config
pub(crate) fn config_from_common(config: &Config) -> worker::config::Config {
    worker::config::Config {
        microbatch_max_interval: config.microbatch_max_interval,
        poll_interval: config.poll_interval,
        keep_alive_interval: config.keep_alive_interval,
        max_mem_mb: config.max_mem_mb,
        query_max_mem_mb: config.query_max_mem_mb,
        spill_location: config.spill_location.clone(),
        parquet: config.parquet.clone(),
        worker_info: worker::info::WorkerInfo {
            version: Some(config.build_info.version.clone()),
            commit_sha: Some(config.build_info.commit_sha.clone()),
            commit_timestamp: Some(config.build_info.commit_timestamp.clone()),
            build_date: Some(config.build_info.build_date.clone()),
        },
    }
}

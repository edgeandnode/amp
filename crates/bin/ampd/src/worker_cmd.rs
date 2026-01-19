use amp_config::Config;
use amp_data_store::DataStore;
use amp_dataset_store::DatasetStore;
use amp_datasets_registry::{DatasetsRegistry, manifests::DatasetManifestsStore};
use amp_object_store::ObjectStoreCreationError;
use amp_providers_registry::{ProviderConfigsStore, ProvidersRegistry};
use monitoring::telemetry::metrics::Meter;
use worker::node_id::NodeId;

pub async fn run(config: Config, meter: Option<Meter>, node_id: NodeId) -> Result<(), Error> {
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

    let providers_registry = {
        let provider_configs_store = ProviderConfigsStore::new(
            amp_object_store::new_with_prefix(
                &config.providers_store_url,
                config.providers_store_url.path(),
            )
            .map_err(Error::ProvidersStoreCreation)?,
        );
        ProvidersRegistry::new(provider_configs_store)
    };

    let datasets_registry = {
        let dataset_manifests_store = DatasetManifestsStore::new(
            amp_object_store::new_with_prefix(
                &config.manifests_store_url,
                config.manifests_store_url.path(),
            )
            .map_err(Error::ManifestsStoreCreation)?,
        );
        DatasetsRegistry::new(metadata_db.clone(), dataset_manifests_store)
    };

    let dataset_store = DatasetStore::new(datasets_registry.clone(), providers_registry.clone());

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

/// Convert config::Config to worker::config::Config
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

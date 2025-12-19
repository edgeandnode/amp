use std::{net::SocketAddr, sync::Arc};

use amp_object_store::ObjectStoreCreationError;
use common::{BoxError, store::Store};
use config::Config as CommonConfig;
use controller::config::Config;
use dataset_store::{
    DatasetStore, manifests::DatasetManifestsStore, providers::ProviderConfigsStore,
};
use monitoring::telemetry::metrics::Meter;

/// Run the controller service (Admin API server)
pub async fn run(config: CommonConfig, meter: Option<Meter>, at: SocketAddr) -> Result<(), Error> {
    let metadata_db = config
        .metadata_db()
        .await
        .map_err(|err| Error::MetadataDbConnection(Box::new(err)))?;

    let data_store = Store::new(metadata_db.clone(), config.data_store_url.clone())
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

    // Convert to controller-specific config
    let controller_config = config_from_common(&config);

    let (addr, server) = controller::service::new(
        Arc::new(controller_config),
        metadata_db,
        data_store,
        dataset_store,
        meter,
        at,
    )
    .await
    .map_err(Error::ServiceInit)?;

    tracing::info!("Controller Admin API running at {}", addr);

    server.await.map_err(Error::Runtime)?;

    Ok(())
}

/// Errors that can occur during controller execution
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to connect to metadata database
    ///
    /// This occurs when the controller cannot establish a connection to the
    /// PostgreSQL metadata database.
    #[error("Failed to connect to metadata database: {0}")]
    MetadataDbConnection(#[source] Box<config::ConfigError>),

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

    /// Failed to initialize the controller service (Admin API)
    ///
    /// This occurs during the initialization phase when attempting to bind and
    /// start the Admin API server.
    #[error("Failed to initialize controller service: {0}")]
    ServiceInit(#[source] controller::service::Error),

    /// Controller service (Admin API) encountered a runtime error
    ///
    /// This occurs after the Admin API server has started successfully but
    /// encounters an error during operation.
    #[error("Controller runtime error: {0}")]
    Runtime(#[source] BoxError),
}

/// Convert common config to controller-specific config
pub fn config_from_common(config: &CommonConfig) -> Config {
    Config {
        build_info: config.build_info.clone(),
    }
}

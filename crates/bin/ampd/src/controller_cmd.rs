use std::{net::SocketAddr, sync::Arc};

use common::{BoxError, config::Config as CommonConfig};
use metadata_db::MetadataDb;

/// Run the controller service (Admin API server)
pub async fn run(
    config: CommonConfig,
    metadata_db: MetadataDb,
    admin_api_addr: SocketAddr,
    meter: Option<&monitoring::telemetry::metrics::Meter>,
) -> Result<(), Error> {
    // Convert to controller-specific config
    let controller_config = Arc::new(config_from_common(&config));

    let (addr, server) =
        controller::service::new(controller_config, metadata_db, meter, admin_api_addr)
            .await
            .map_err(Error::ServiceInit)?;

    tracing::info!("Controller Admin API running at {}", addr);

    server.await.map_err(Error::Runtime)?;

    Ok(())
}

/// Errors that can occur during controller execution
#[derive(Debug, thiserror::Error)]
pub enum Error {
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
pub fn config_from_common(config: &CommonConfig) -> controller::config::Config {
    controller::config::Config {
        providers_store: config.providers_store.clone(),
        manifests_store: config.manifests_store.clone(),
        data_store: config.data_store.clone(),
        build_info: config.build_info.clone(),
    }
}

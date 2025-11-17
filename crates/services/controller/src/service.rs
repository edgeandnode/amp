use std::{future::Future, net::SocketAddr, sync::Arc};

use admin_api::ctx::Ctx;
use axum::{
    Router,
    http::StatusCode,
    routing::get,
    serve::{Listener as _, ListenerExt as _},
};
use common::{BoxError, config::Config, utils::shutdown_signal};
use dataset_store::{
    DatasetStore, manifests::DatasetManifestsStore, providers::ProviderConfigsStore,
};
use opentelemetry_instrumentation_tower::HTTPMetricsLayerBuilder;
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;

use crate::scheduler::Scheduler;

/// Create and initialize the controller service
///
/// Sets up the admin API server with the scheduler, dataset store, and metadata database.
/// Configures health check endpoint, OpenTelemetry metrics, and CORS middleware.
///
/// Returns the bound socket address and a future that runs the server with graceful shutdown.
pub async fn new(
    config: Arc<Config>,
    meter: Option<&monitoring::telemetry::metrics::Meter>,
    at: SocketAddr,
) -> Result<(SocketAddr, impl Future<Output = Result<(), BoxError>>), Error> {
    let metadata_db = config.metadata_db().await.map_err(Error::MetadataDb)?;

    let dataset_store = {
        let provider_configs_store =
            ProviderConfigsStore::new(config.providers_store.prefixed_store());
        let dataset_manifests_store =
            DatasetManifestsStore::new(config.manifests_store.prefixed_store());

        DatasetStore::new(
            metadata_db.clone(),
            provider_configs_store,
            dataset_manifests_store,
        )
    };

    let scheduler = Scheduler::new(config.clone(), metadata_db.clone());

    let ctx = Ctx {
        metadata_db,
        dataset_store,
        scheduler: Arc::new(scheduler),
        config,
    };

    // Create controller router with health check endpoint
    let mut app = Router::new()
        .route("/healthz", get(|| async { StatusCode::OK }))
        .merge(admin_api::router(ctx));

    // Add OpenTelemetry HTTP metrics middleware if meter is provided
    if let Some(meter) = meter {
        let metrics_layer = HTTPMetricsLayerBuilder::builder()
            .with_meter(meter.clone())
            .build()
            .map_err(|err| Error::MetricsLayer(Box::new(err)))?;
        app = app.layer(metrics_layer);
    }

    let listener = TcpListener::bind(at)
        .await
        .map_err(|source| Error::TcpBind { addr: at, source })?
        .tap_io(|tcp_stream| tcp_stream.set_nodelay(true).unwrap());
    let addr = listener.local_addr().map_err(Error::LocalAddr)?;

    let router = app.layer(CorsLayer::permissive());

    // The service future that runs the server with graceful shutdown
    let server = async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .map_err(Into::into)
    };
    Ok((addr, server))
}

/// Errors that can occur when creating the controller service
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to connect to metadata database
    ///
    /// This occurs when:
    /// - Database connection string is invalid or malformed
    /// - Database server is unreachable or not running
    /// - Connection pool initialization fails
    /// - Database authentication fails
    #[error("failed to connect to metadata database: {0}")]
    MetadataDb(#[source] common::config::ConfigError),

    /// Failed to bind TCP listener to the specified address
    ///
    /// This occurs when:
    /// - The address is already in use by another process
    /// - The port requires elevated privileges (e.g., port < 1024)
    /// - The address is not available on this system
    /// - Network interface is not configured
    #[error("failed to bind to {addr}: {source}")]
    TcpBind {
        addr: SocketAddr,
        source: std::io::Error,
    },

    /// Failed to get local address from TCP listener
    ///
    /// This occurs when:
    /// - Socket state is invalid after binding
    /// - System call to retrieve address fails
    #[error("failed to get local address: {0}")]
    LocalAddr(#[source] std::io::Error),

    /// Failed to build OpenTelemetry metrics layer
    ///
    /// This occurs when:
    /// - Metrics configuration is invalid
    /// - Meter provider initialization fails
    /// - OpenTelemetry setup encounters an error
    #[error("failed to build metrics layer: {0}")]
    MetricsLayer(#[source] BoxError),
}

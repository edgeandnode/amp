use std::{future::Future, net::SocketAddr, sync::Arc, time::Duration};

use admin_api::ctx::Ctx;
use axum::{
    Router,
    http::StatusCode,
    routing::get,
    serve::{Listener as _, ListenerExt as _},
};
use common::{BoxError, store::Store};
use dataset_store::DatasetStore;
use metadata_db::MetadataDb;
use monitoring::telemetry::metrics::Meter;
use opentelemetry_instrumentation_tower::HTTPMetricsLayerBuilder;
use tokio::{net::TcpListener, time::MissedTickBehavior};
use tower_http::cors::CorsLayer;

use crate::{config::Config, scheduler::Scheduler};

/// Reconciliation interval for failed job retries
///
/// The scheduler checks for failed jobs ready for retry at this interval.
const RECONCILIATION_INTERVAL: Duration = Duration::from_secs(60);

/// Create and initialize the controller service
///
/// Sets up the admin API server with the scheduler, dataset store, and metadata database.
/// Configures health check endpoint, OpenTelemetry metrics, and CORS middleware.
/// Spawns a background task for failed job reconciliation with exponential backoff retry.
///
/// Returns the bound socket address and a future that runs the server with graceful shutdown.
pub async fn new(
    config: Arc<Config>,
    metadata_db: MetadataDb,
    data_store: Arc<Store>,
    dataset_store: DatasetStore,
    meter: Option<Meter>,
    at: SocketAddr,
) -> Result<(SocketAddr, impl Future<Output = Result<(), BoxError>>), Error> {
    let scheduler = Arc::new(Scheduler::new(metadata_db.clone()));

    let ctx = Ctx {
        metadata_db,
        dataset_store,
        scheduler: scheduler.clone(),
        data_store,
        build_info: config.build_info.clone(),
    };

    // Create controller router with health check endpoint
    let mut app = Router::new()
        .route("/healthz", get(|| async { StatusCode::OK }))
        .merge(admin_api::router(ctx));

    // Add OpenTelemetry HTTP metrics middleware if meter is provided
    if let Some(meter) = meter {
        let metrics_layer = HTTPMetricsLayerBuilder::builder()
            .with_meter(meter)
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

    // Spawn background task for failed job reconciliation
    let scheduler_for_reconciliation = scheduler.clone();
    let reconciliation_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(RECONCILIATION_INTERVAL);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            interval.tick().await;
            if let Err(err) = scheduler_for_reconciliation.reconcile_failed_jobs().await {
                tracing::error!(error = %err, "Failed job reconciliation");
            }
        }
    });

    // The service future that runs the server with graceful shutdown
    let server = async move {
        let server_future = axum::serve(listener, router).with_graceful_shutdown(shutdown_signal());

        tokio::select! {
            result = server_future => {
                result.map_err(Into::into)
            }
            _ = reconciliation_task => {
                Err("Reconciliation task terminated".into())
            }
        }
    };
    Ok((addr, server))
}

/// Errors that can occur when creating the controller service
#[derive(Debug, thiserror::Error)]
pub enum Error {
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

/// Returns a future that completes when a shutdown signal is received.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigint = signal(SignalKind::interrupt()).expect("Failed to install SIGINT handler");
        let mut sigterm =
            signal(SignalKind::terminate()).expect("Failed to install SIGTERM handler");
        tokio::select! {
            _ = sigint.recv() => tracing::info!(signal="SIGINT", "shutdown signal"),
            _ = sigterm.recv() => tracing::info!(signal="SIGTERM", "shutdown signal"),
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
        tracing::info!("shutdown signal");
    }
}

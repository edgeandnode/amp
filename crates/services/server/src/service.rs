//! Server service orchestration
//!
//! This module provides the main entry point for creating and running the server service.
//! It coordinates the Arrow Flight RPC server and JSON Lines HTTP server.

use std::{future::Future, net::SocketAddr, sync::Arc};

use axum::{
    Router,
    http::StatusCode,
    routing::get,
    serve::{Listener as _, ListenerExt as _},
};
use common::{BoxError, utils::shutdown_signal};
use datafusion::error::DataFusionError;
use futures::FutureExt;
use metadata_db::MetadataDb;
use monitoring::{logging, telemetry::metrics::Meter};
use thiserror::Error;
use tokio::net::TcpListener;

use crate::{config::Config, flight, jsonl};

/// Create and initialize the server service
///
/// Sets up the Arrow Flight RPC server and/or JSON Lines HTTP server based on the provided addresses.
/// Creates the internal service instance with query execution capabilities and returns bound
/// addresses along with a future that runs the servers with graceful shutdown.
pub async fn new(
    config: Arc<Config>,
    metadata_db: MetadataDb,
    flight_at: impl Into<Option<SocketAddr>>,
    jsonl_at: impl Into<Option<SocketAddr>>,
    meter: Option<&Meter>,
) -> Result<(BoundAddrs, impl Future<Output = Result<(), BoxError>>), InitError> {
    // Create the internal service instance
    let service = flight::Service::create(config.clone(), metadata_db.clone(), meter).await?;

    // Start Arrow Flight Server if address provided
    let (flight_addr, flight_fut) = match flight_at.into() {
        Some(addr) => new_flight_server(service.clone(), addr)
            .await
            .map(|(addr, fut)| (Some(addr), fut.boxed()))
            .map_err(InitError::Flight)?,
        None => (
            None,
            Box::pin(std::future::pending::<Result<(), BoxError>>()) as _,
        ),
    };

    // Start JSON Lines Server if address provided
    let (jsonl_addr, jsonl_fut) = match jsonl_at.into() {
        Some(addr) => new_jsonl_server(service, addr)
            .await
            .map(|(bound_addr, fut)| (Some(bound_addr), fut.boxed()))
            .map_err(InitError::Jsonl)?,
        None => (
            None,
            Box::pin(std::future::pending::<Result<(), BoxError>>()) as _,
        ),
    };

    // Wait for the services to finish, either due to graceful shutdown or error.
    let addrs = BoundAddrs {
        flight_addr,
        jsonl_addr,
    };
    let fut = async move {
        tokio::select! {
            result = flight_fut => {
                if let Err(err) = &result {
                    tracing::error!(error = %err, error_source = logging::error_source(&**err), "Flight server shutting down due to unexpected error");
                }
                result
            }
            result = jsonl_fut => {
                if let Err(err) = &result {
                    tracing::error!(error = %err, error_source = logging::error_source(&**err), "JSONL server shutting down due to unexpected error");
                }
                result
            }
        }
    };

    Ok((addrs, fut))
}

#[derive(Debug, Clone, Copy)]
pub struct BoundAddrs {
    pub flight_addr: Option<SocketAddr>,
    pub jsonl_addr: Option<SocketAddr>,
}

/// Create and initialize the Arrow Flight server service
async fn new_flight_server(
    ctx: flight::Service,
    at: SocketAddr,
) -> Result<(SocketAddr, impl Future<Output = Result<(), BoxError>>), FlightInitError> {
    let listener = TcpListener::bind(at)
        .await
        .map_err(|source| FlightInitError::Bind { addr: at, source })?
        .tap_io(|tcp_stream| {
            tcp_stream
                .set_nodelay(true)
                .expect("failed to set TCP_NODELAY option")
        });
    let bound_addr = listener.local_addr().map_err(FlightInitError::LocalAddr)?;

    let app = Router::new()
        .route("/healthz", get(|| async { StatusCode::OK }))
        .merge(flight::build_router(ctx));

    // Future that runs the Flight server with graceful shutdown
    let fut = async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .map_err(|err| {
                tracing::error!(error = %err, error_source = logging::error_source(&err), "Flight server error");
                err.into()
            })
    };

    Ok((bound_addr, fut))
}

/// Create and initialize the JSON Lines server service
async fn new_jsonl_server(
    ctx: flight::Service,
    at: SocketAddr,
) -> Result<(SocketAddr, impl Future<Output = Result<(), BoxError>>), JsonlInitError> {
    let listener = TcpListener::bind(at)
        .await
        .map_err(|source| JsonlInitError::Bind { addr: at, source })?
        .tap_io(|tcp_stream| {
            tcp_stream
                .set_nodelay(true)
                .expect("failed to set TCP_NODELAY option")
        });
    let bound_addr = listener.local_addr().map_err(JsonlInitError::LocalAddr)?;

    let app = Router::new()
        .route("/health", get(|| async { StatusCode::OK }))
        .merge(jsonl::build_router(ctx));

    // Future that runs the JSONL server with graceful shutdown
    let fut = async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .map_err(Into::into)
    };

    Ok((bound_addr, fut))
}

/// Errors that can occur when creating the server service
#[derive(Debug, Error)]
pub enum InitError {
    /// Failed to create query environment
    ///
    /// This occurs when:
    /// - DataFusion session configuration is invalid
    /// - Query execution pool initialization fails
    #[error("failed to create query environment: {0}")]
    QueryEnv(#[source] DataFusionError),

    /// Failed to initialize Flight server
    ///
    /// This occurs when:
    /// - TCP listener fails to bind to the specified address
    /// - Local address cannot be retrieved from the listener
    #[error("failed to initialize Flight server: {0}")]
    Flight(#[source] FlightInitError),

    /// Failed to initialize JSONL server
    ///
    /// This occurs when:
    /// - TCP listener fails to bind to the specified address
    /// - Local address cannot be retrieved from the listener
    #[error("failed to initialize JSONL server: {0}")]
    Jsonl(#[source] JsonlInitError),
}

/// Errors that can occur when initializing the Flight server
#[derive(Debug, Error)]
pub enum FlightInitError {
    /// Failed to bind Flight server TCP listener
    ///
    /// This occurs when:
    /// - The address is already in use by another process
    /// - The port requires elevated privileges (e.g., port < 1024)
    /// - The address is not available on this system
    /// - Network interface is not configured
    #[error("failed to bind Flight server to {addr}: {source}")]
    Bind {
        addr: SocketAddr,
        source: std::io::Error,
    },

    /// Failed to get local address from Flight TCP listener
    ///
    /// This occurs when:
    /// - Socket state is invalid after binding
    /// - System call to retrieve address fails
    #[error("failed to get Flight server local address: {0}")]
    LocalAddr(#[source] std::io::Error),
}

/// Errors that can occur when initializing the JSONL server
#[derive(Debug, Error)]
pub enum JsonlInitError {
    /// Failed to bind JSONL server TCP listener
    ///
    /// This occurs when:
    /// - The address is already in use by another process
    /// - The port requires elevated privileges (e.g., port < 1024)
    /// - The address is not available on this system
    /// - Network interface is not configured
    #[error("failed to bind JSONL server to {addr}: {source}")]
    Bind {
        addr: SocketAddr,
        source: std::io::Error,
    },

    /// Failed to get local address from JSONL TCP listener
    ///
    /// This occurs when:
    /// - Socket state is invalid after binding
    /// - System call to retrieve address fails
    #[error("failed to get JSONL server local address: {0}")]
    LocalAddr(#[source] std::io::Error),
}

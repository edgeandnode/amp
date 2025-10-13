use std::{future::Future, net::SocketAddr, sync::Arc};

use arrow_flight::flight_service_server::FlightServiceServer;
use common::{BoxError, BoxResult, config::Config, utils::shutdown_signal};
use futures::{FutureExt, TryFutureExt as _};
use metadata_db::MetadataDb;
use server::service::Service;
use tokio::net::TcpListener;
use tonic::transport::{Server, server::TcpIncoming};

#[derive(Debug, Clone, Copy)]
pub struct BoundAddrs {
    pub flight_addr: SocketAddr,
    pub jsonl_addr: SocketAddr,
}

pub async fn run(
    config: Config,
    metadata_db: MetadataDb,
    flight_server: bool,
    jsonl_server: bool,
    meter: Option<&monitoring::telemetry::metrics::Meter>,
) -> Result<(), Error> {
    let config = Arc::new(config);

    let (_, server) = run_servers(config, metadata_db, flight_server, jsonl_server, meter)
        .await
        .map_err(Error::ServerStart)?;

    server.await.map_err(Error::ServerRuntime)
}

pub async fn run_servers(
    config: Arc<Config>,
    metadata_db: MetadataDb,
    enable_flight: bool,
    enable_jsonl: bool,
    meter: Option<&monitoring::telemetry::metrics::Meter>,
) -> Result<(BoundAddrs, impl Future<Output = BoxResult<()>>), BoxError> {
    if config.max_mem_mb == 0 {
        tracing::info!("Memory limit is unlimited");
    } else {
        tracing::info!("Memory limit is {} MB", config.max_mem_mb);
    }

    tracing::info!(
        "Spill to disk allowed: {}",
        !config.spill_location.is_empty()
    );

    let service = Service::new(config.clone(), metadata_db.clone(), meter).await?;

    let mut addrs = BoundAddrs {
        flight_addr: config.addrs.flight_addr,
        jsonl_addr: config.addrs.jsonl_addr,
    };

    // Start Arrow Flight Server if enabled
    let flight_fut = if enable_flight {
        let listener = TcpListener::bind(config.addrs.flight_addr).await?;
        let addr = listener.local_addr()?;
        addrs.flight_addr = addr;
        tracing::info!("Serving Arrow Flight RPC at {}", addr);

        Server::builder()
            .add_service(FlightServiceServer::new(service.clone()))
            .serve_with_incoming_shutdown(
                TcpIncoming::from_listener(listener, true, None)?,
                shutdown_signal(),
            )
            .map_err(|err| {
                tracing::error!("Flight server error: {}", err);
                err.into()
            })
            .boxed()
    } else {
        Box::pin(std::future::pending())
    };

    // Start JSON Lines Server if enabled
    let jsonl_fut = if enable_jsonl {
        let (jsonl_addr, jsonl_server) =
            server::jsonl::run_server(service, config.addrs.jsonl_addr).await?;
        addrs.jsonl_addr = jsonl_addr;
        tracing::info!("Serving JSON lines at {}", jsonl_addr);

        jsonl_server
            .map_err(|err| {
                tracing::error!("JSON lines server error: {}", err);
                err
            })
            .boxed()
    } else {
        Box::pin(std::future::pending())
    };

    // Wait for the services to finish, either due to graceful shutdown or error.
    let fut = async move {
        tokio::select! {
            result = flight_fut => {
                if let Err(err) = &result {
                    tracing::error!(error=?err, "Flight server shutting down due to unexpected error");
                }
                result
            }
            result = jsonl_fut => {
                if let Err(err) = &result {
                    tracing::error!(error=?err, "JSONL server shutting down due to unexpected error");
                }
                result
            }
        }
    };

    Ok((addrs, fut))
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

use std::{
    future::Future,
    net::{SocketAddr, TcpListener},
    sync::Arc,
};

use arrow_flight::flight_service_server::FlightServiceServer;
use common::{BoxError, BoxResult, config::Config};
use futures::{FutureExt, StreamExt as _, TryFutureExt as _, stream::FuturesUnordered};
use hyper_util::rt::TokioExecutor;
use metadata_db::MetadataDb;
use server::{Service, health::Multiplexer, jsonl};
use tonic::transport::Server;

#[derive(Debug, Clone, Copy)]
pub struct BoundAddrs {
    pub flight_addr: SocketAddr,
    pub jsonl_addr: SocketAddr,
}

pub async fn run(
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

    let mut services_futures = Vec::new();
    let mut bound_addrs = BoundAddrs {
        flight_addr: config.addrs.flight_addr,
        jsonl_addr: config.addrs.jsonl_addr,
    };

    // Start Arrow Flight Server if enabled
    if enable_flight {
        let flight_tcp_listener = TcpListener::bind(config.addrs.flight_addr)?;
        let flight_addr = flight_tcp_listener.local_addr()?;
        flight_tcp_listener.set_nonblocking(true)?;

        // Create gRPC service
        let grpc_service = Server::builder()
            .add_service(FlightServiceServer::new(service.clone()))
            .into_service();

        // Wrap with multiplexer for health checks
        let multiplexed = Multiplexer::new(grpc_service, config.clone(), metadata_db.clone());

        // Serve with hyper
        let listener = tokio::net::TcpListener::from_std(flight_tcp_listener)?;
        let flight_server = async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(conn) => conn,
                    Err(e) => {
                        tracing::error!("Flight server accept error: {}", e);
                        return Err::<(), BoxError>(e.into());
                    }
                };

                let io = hyper_util::rt::TokioIo::new(stream);
                let multiplexed_clone = multiplexed.clone();

                tokio::spawn(async move {
                    // Use auto builder to support both HTTP/1.1 (health checks) and HTTP/2 (gRPC)
                    if let Err(e) =
                        hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                            .serve_connection(io, multiplexed_clone)
                            .await
                    {
                        tracing::debug!("Flight connection error: {}", e);
                    }
                });
            }
        }
        .boxed();

        services_futures.push(flight_server);
        bound_addrs.flight_addr = flight_addr;
        tracing::info!("Serving Arrow Flight RPC at {}", flight_addr);
    }

    // Start JSON Lines Server if enabled
    if enable_jsonl {
        let (jsonl_addr, jsonl_server) = run_jsonl_server(
            service,
            config.clone(),
            metadata_db.clone(),
            config.addrs.jsonl_addr,
        )
        .await?;
        let jsonl_server = jsonl_server
            .map_err(|e| {
                tracing::error!("JSON lines server error: {}", e);
                e
            })
            .boxed();
        services_futures.push(jsonl_server);
        bound_addrs.jsonl_addr = jsonl_addr;
        tracing::info!("Serving JSON lines at {}", jsonl_addr);
    }

    // Wait for the services to finish, either due to graceful shutdown or error.
    let server = async move {
        let mut services: FuturesUnordered<_> = services_futures.into_iter().collect();
        while let Some(result) = services.next().await {
            if result.is_err() {
                tracing::error!("Shutting down due to unexpected error");
            }
            result?
        }
        Ok(())
    };

    Ok((bound_addrs, server))
}

async fn run_jsonl_server(
    service: Service,
    config: Arc<Config>,
    metadata_db: MetadataDb,
    addr: SocketAddr,
) -> BoxResult<(SocketAddr, impl Future<Output = BoxResult<()>>)> {
    let state = jsonl::JsonlState {
        service,
        config,
        metadata_db,
    };

    let app = axum::Router::new()
        .route("/", axum::routing::post(jsonl::handle_request))
        .route("/health", axum::routing::get(jsonl::handle_health))
        .route("/ready", axum::routing::get(jsonl::handle_ready))
        .with_state(state)
        .layer(tower_http::compression::CompressionLayer::new().gzip(true));
    http_common::serve_at(addr, app).await
}

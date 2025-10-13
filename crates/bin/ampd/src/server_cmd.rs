use std::{future::Future, net::SocketAddr, sync::Arc};

use arrow_flight::flight_service_server::FlightServiceServer;
use axum::{
    response::IntoResponse,
    serve::{Listener as _, ListenerExt as _},
};
use common::{
    BoxError, BoxResult, arrow, config::Config, query_context::parse_sql,
    stream_helpers::is_streaming, utils::shutdown_signal,
};
use futures::{
    FutureExt, StreamExt as _, TryFutureExt as _, TryStreamExt as _, stream::FuturesUnordered,
};
use metadata_db::MetadataDb;
use server::service::Service;
use tokio::net::TcpListener;
use tonic::transport::{Server, server::TcpIncoming};
use tower_http::cors::CorsLayer;
use tracing::instrument;

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

    let (addrs, server) = run_servers(config, metadata_db, flight_server, jsonl_server, meter)
        .await
        .map_err(Error::ServerStart)?;

    if flight_server {
        tracing::info!("Arrow Flight RPC Server running at {}", addrs.flight_addr);
    }
    if jsonl_server {
        tracing::info!("JSON Lines Server running at {}", addrs.jsonl_addr);
    }

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

    let mut services_futures = Vec::new();
    let mut bound_addrs = BoundAddrs {
        flight_addr: config.addrs.flight_addr,
        jsonl_addr: config.addrs.jsonl_addr,
    };

    // Start Arrow Flight Server if enabled
    if enable_flight {
        let listener = TcpListener::bind(config.addrs.flight_addr).await?;
        let flight_addr = listener.local_addr()?;
        let flight_server = Server::builder()
            .add_service(FlightServiceServer::new(service.clone()))
            .serve_with_incoming_shutdown(
                TcpIncoming::from_listener(listener, true, None)?,
                shutdown_signal(),
            )
            .map_err(|err| {
                tracing::error!("Flight server error: {}", err);
                err.into()
            })
            .boxed();
        services_futures.push(flight_server);
        bound_addrs.flight_addr = flight_addr;
        tracing::info!("Serving Arrow Flight RPC at {}", flight_addr);
    }

    // Start JSON Lines Server if enabled
    if enable_jsonl {
        let (jsonl_addr, jsonl_server) = run_jsonl_server(service, config.addrs.jsonl_addr).await?;
        let jsonl_server = jsonl_server
            .map_err(|err| {
                tracing::error!("JSON lines server error: {}", err);
                err
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
    addr: SocketAddr,
) -> BoxResult<(SocketAddr, impl Future<Output = BoxResult<()>>)> {
    let app = axum::Router::new()
        .route(
            "/",
            axum::routing::post(handle_jsonl_request).with_state(service),
        )
        .layer(tower_http::compression::CompressionLayer::new().gzip(true));
    let listener = TcpListener::bind(addr)
        .await?
        .tap_io(|tcp_stream| tcp_stream.set_nodelay(true).unwrap());
    let addr = listener.local_addr()?;
    let router = app.layer(CorsLayer::permissive());
    let server = async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .map_err(Into::into)
    };
    Ok((addr, server))
}

#[instrument(skip(service))]
async fn handle_jsonl_request(
    axum::extract::State(service): axum::extract::State<Service>,
    request: String,
) -> axum::response::Response {
    fn error_payload(message: impl std::fmt::Display) -> String {
        serde_json::json!({
            "error_code": "QUERY_ERROR",
            "error_message": message.to_string(),
        })
        .to_string()
    }

    let stream = match service.execute_query(&request).await {
        Ok(stream) => stream,
        Err(err) => return err.into_response(),
    };
    let stream = stream
        .record_batches()
        .map(|result| -> Result<Vec<u8>, BoxError> {
            let batch = result.map_err(error_payload)?;
            let mut buf: Vec<u8> = Default::default();
            let mut writer = arrow::json::writer::LineDelimitedWriter::new(&mut buf);
            writer.write(&batch)?;
            Ok(buf)
        })
        .map_err(error_payload);
    let mut response =
        axum::response::Response::builder().header("content-type", "application/x-ndjson");
    let query = match parse_sql(&request) {
        Ok(query) => query,
        Err(err) => return err.into_response(),
    };
    // For streaming queries, disable compression
    if is_streaming(&query) {
        response = response.header("content-encoding", "identity");
    }
    response
        .body(axum::body::Body::from_stream(stream))
        .unwrap()
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

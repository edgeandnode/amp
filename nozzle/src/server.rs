use std::{
    future::Future,
    net::{SocketAddr, TcpListener},
    sync::Arc,
};

use arrow_flight::flight_service_server::FlightServiceServer;
use axum::response::IntoResponse;
use common::{BoxError, BoxResult, arrow, config::Config};
use dump::worker::Worker;
use futures::{
    FutureExt, StreamExt as _, TryFutureExt as _, TryStreamExt as _, stream::FuturesUnordered,
};
use metadata_db::MetadataDb;
use server::service::Service;
use tonic::transport::{Server, server::TcpIncoming};

#[derive(Debug, Clone, Copy)]
pub struct BoundAddrs {
    pub flight_addr: SocketAddr,
    pub jsonl_addr: SocketAddr,
    pub registry_service_addr: SocketAddr,
    pub admin_api_addr: SocketAddr,
}

pub async fn run(
    config: Arc<Config>,
    metadata_db: Arc<MetadataDb>,
    dev: bool,
    enable_flight: bool,
    enable_jsonl: bool,
    enable_registry: bool,
    enable_admin: bool,
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

    if dev {
        let worker = Worker::new(
            config.clone(),
            metadata_db.clone(),
            "worker".parse().expect("Invalid worker ID"),
        );
        tokio::spawn(async move {
            if let Err(err) = worker.run().await {
                tracing::error!("{err}");
            }
        });
    }

    let service = Service::new(config.clone(), metadata_db.clone()).await?;

    let mut services_futures = Vec::new();
    let mut bound_addrs = BoundAddrs {
        flight_addr: config.addrs.flight_addr,
        jsonl_addr: config.addrs.jsonl_addr,
        registry_service_addr: config.addrs.registry_service_addr,
        admin_api_addr: config.addrs.admin_api_addr,
    };

    // Start Arrow Flight Server if enabled
    if enable_flight {
        let flight_tcp_listener = TcpListener::bind(config.addrs.flight_addr)?;
        let flight_addr = flight_tcp_listener.local_addr()?;
        flight_tcp_listener.set_nonblocking(true)?;
        let flight_server = Server::builder()
            .add_service(FlightServiceServer::new(service.clone()))
            .serve_with_incoming(TcpIncoming::from_listener(
                tokio::net::TcpListener::from_std(flight_tcp_listener)?,
                true,
                None,
            )?)
            .map_err(|e| {
                tracing::error!("Flight server error: {}", e);
                e.into()
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
            .map_err(|e| {
                tracing::error!("JSON lines server error: {}", e);
                e
            })
            .boxed();
        services_futures.push(jsonl_server);
        bound_addrs.jsonl_addr = jsonl_addr;
        tracing::info!("Serving JSON lines at {}", jsonl_addr);
    }

    // Start Registry Server if enabled
    if enable_registry {
        let (registry_service_addr, registry_service) = registry_service::serve(
            config.addrs.registry_service_addr,
            config.clone(),
            metadata_db,
        )
        .await?;
        let registry_service = registry_service
            .map_err(|e| {
                tracing::error!("Registry service error: {}", e);
                e
            })
            .boxed();
        services_futures.push(registry_service);
        bound_addrs.registry_service_addr = registry_service_addr;
        tracing::info!("Registry service running at {}", registry_service_addr);
    }

    // Start Admin API Server if enabled
    if enable_admin {
        let (admin_api_addr, admin_api) =
            admin_api::serve(config.addrs.admin_api_addr, config).await?;
        let admin_api = admin_api
            .map_err(|e| {
                tracing::error!("Admin API error: {}", e);
                e
            })
            .boxed();
        services_futures.push(admin_api);
        bound_addrs.admin_api_addr = admin_api_addr;
        tracing::info!("Admin API running at {}", admin_api_addr);
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
        .layer(
            tower_http::compression::CompressionLayer::new()
                .br(true)
                .gzip(true),
        );
    http_common::serve_at(addr, app).await
}

async fn handle_jsonl_request(
    axum::extract::State(service): axum::extract::State<Service>,
    request: String,
) -> axum::response::Response {
    fn error_payload(message: impl std::fmt::Display) -> String {
        format!(r#"{{"error": "{}"}}"#, message)
    }
    let stream = match service.execute_query(&request).await {
        Ok(stream) => stream,
        Err(err) => return err.into_response(),
    };
    let stream = stream
        .map(|result| -> Result<String, BoxError> {
            let batch = result.map_err(error_payload)?;
            let mut buf: Vec<u8> = Default::default();
            let mut writer = arrow::json::writer::LineDelimitedWriter::new(&mut buf);
            writer.write(&batch)?;
            Ok(String::from_utf8(buf).unwrap())
        })
        .map_err(error_payload);
    axum::response::Response::builder()
        .header("content-type", "application/x-ndjson")
        .body(axum::body::Body::from_stream(stream))
        .unwrap()
}

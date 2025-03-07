use std::{net::SocketAddr, sync::Arc};

use arrow_flight::flight_service_server::FlightServiceServer;
use common::{arrow, config::Config, BoxError};
use futures::{FutureExt, StreamExt as _, TryStreamExt as _};
use metadata_db::MetadataDb;
use server::service::Service;
use tonic::transport::Server;

pub async fn run(
    config: Arc<Config>,
    metadata_db: Option<MetadataDb>,
    no_admin: bool,
) -> Result<(), BoxError> {
    log::info!("memory limit is {} MB", config.max_mem_mb);
    log::info!(
        "spill to disk allowed: {}",
        !config.spill_location.is_empty()
    );

    let service = Service::new(config.clone(), metadata_db.clone())?;

    let flight_addr: SocketAddr = ([0, 0, 0, 0], 1602).into();
    let flight_server = Server::builder()
        .add_service(FlightServiceServer::new(service.clone()))
        .serve(flight_addr);
    log::info!("Serving Arrow Flight RPC at {}", flight_addr);

    let jsonl_addr: SocketAddr = ([0, 0, 0, 0], 1603).into();
    let jsonl_server = run_jsonl_server(service, jsonl_addr);
    log::info!("Serving JSON lines at {}", jsonl_addr);

    let registry_service_addr: SocketAddr = ([0, 0, 0, 0], 1611).into();
    let registry_service =
        registry_service::serve(registry_service_addr, config.clone(), metadata_db);
    log::info!("Registry service running at {}", registry_service_addr);

    let admin_api = match no_admin {
        true => std::future::pending().boxed(),
        false => {
            let admin_api_addr: SocketAddr = ([0, 0, 0, 0], 1610).into();
            log::info!("Admin API running at {}", admin_api_addr);
            admin_api::serve(admin_api_addr, config).boxed()
        }
    };

    tokio::select! {
        result = flight_server => result?,
        result = jsonl_server => result?,
        result = registry_service => result?,
        result = admin_api => result?,
    };
    Err("server shutdown unexpectedly".into())
}

async fn run_jsonl_server(service: Service, addr: SocketAddr) -> Result<(), BoxError> {
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
    http_common::serve_at(addr, app).await?;
    Ok(())
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
        Err(err) => return axum::response::Response::new(error_payload(err.message()).into()),
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

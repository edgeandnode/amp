use std::net::SocketAddr;

use arrow_flight::flight_service_server::FlightServiceServer;
use common::{config::Config, tracing, BoxError};
use log::info;
use tonic::transport::Server;

mod service;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    tracing::register_logger();

    //let dataset = firehose_datasets::evm::dataset("mainnet".to_string());
    let config = std::env::var("NOZZLE_CONFIG")
        .map_err(|_| BoxError::from("no NOZZLE_CONFIG env var set"))?;

    let config = Config::load(config).map_err(|e| format!("failed to load config: {e}"))?;
    info!("memory limit is {} MB", config.max_mem_mb);
    info!(
        "spill to disk allowed: {}",
        !config.spill_location.is_empty()
    );

    let service = service::Service::new(config)?;

    let addr: SocketAddr = ([0, 0, 0, 0], 1602).into();

    info!("Serving at {}", addr);

    Server::builder()
        .add_service(FlightServiceServer::new(service))
        .serve(addr)
        .await?;

    Err("server shutdown unexpectedly, it should run forever".into())
}

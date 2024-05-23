use std::{net::SocketAddr, sync::Arc};

use anyhow::Context;
use arrow_flight::flight_service_server::FlightServiceServer;
use common::{config::Config, tracing, DatasetContext};
use log::info;
use tonic::transport::Server;

mod service;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing::register_logger();

    let dataset = firehose_datasets::evm::dataset("mainnet".to_string());
    let config = std::env::var("NOZZLE_CONFIG").context("no NOZZLE_CONFIG env var set")?;

    let config = Config::load(config.into()).context("failed to load config")?;
    info!("memory limit is {} MB", config.max_mem_mb);
    info!(
        "spill to disk allowed: {}",
        !config.spill_location.is_empty()
    );

    let env = Arc::new((config.to_runtime_env())?);
    let ctx = DatasetContext::new(dataset.clone(), config.data_location, env).await?;
    let svc = FlightServiceServer::new(service::Service::new(ctx));

    let addr: SocketAddr = ([0, 0, 0, 0], 1602).into();

    info!("Serving at {}", addr);

    Server::builder().add_service(svc).serve(addr).await?;
    Ok(())
}

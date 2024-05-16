use std::net::SocketAddr;

use anyhow::Context;
use arrow_flight::flight_service_server::FlightServiceServer;
use common::{config::Config, tracing, DatasetContext};
use log::info;
use tonic::transport::Server;

mod service;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing::register_logger();

    let dataset = firehose_datasets::evm::dataset("mainnet".to_string());
    let config = std::env::var("NOZZLE_CONFIG").context("no NOZZLE_CONFIG env var set")?;
    let config = Config::load(config.into()).context("failed to load config")?;
    let ctx = DatasetContext::new(dataset.clone(), &config).await.unwrap();
    let svc = FlightServiceServer::new(service::Service::new(ctx));

    let addr: SocketAddr = ([0, 0, 0, 0], 1602).into();

    info!("Serving at {}", addr);

    Server::builder().add_service(svc).serve(addr).await?;
    Ok(())
}

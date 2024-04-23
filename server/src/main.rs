use std::net::SocketAddr;

use anyhow::Context;
use arrow_flight::flight_service_server::FlightServiceServer;
use common::DatasetContext;
use tonic::transport::Server;

mod service;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let dataset = firehose_datasets::evm::dataset("mainnet".to_string());
    let data_location = std::env::var("DATA_LOCATION").context("no DATA_LOCATION env var set")?;
    let ctx = DatasetContext::new(dataset.clone(), data_location)
        .await
        .unwrap();
    let svc = FlightServiceServer::new(service::Service::new(ctx));

    let addr: SocketAddr = ([127, 0, 0, 1], 1602).into();

    Server::builder().add_service(svc).serve(addr).await?;
    Ok(())
}

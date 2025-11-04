use std::{future::Future, net::SocketAddr, sync::Arc};

use arrow_flight::flight_service_server::FlightServiceServer;
use axum::{
    http::StatusCode,
    routing::get,
    serve::{Listener as _, ListenerExt as _},
};
use common::{BoxError, BoxResult, config::Config, utils::shutdown_signal};
use futures::{FutureExt, TryFutureExt as _};
use metadata_db::MetadataDb;
use tokio::net::TcpListener;
use tonic::service::Routes;

mod jsonl;
pub mod metrics;
pub mod service;

mod non_empty_record_batch_stream;

use self::service::Service;

pub async fn serve(
    config: Arc<Config>,
    metadata_db: MetadataDb,
    enable_flight: bool,
    enable_jsonl: bool,
    meter: Option<&monitoring::telemetry::metrics::Meter>,
) -> Result<(BoundAddrs, impl Future<Output = BoxResult<()>>), BoxError> {
    let service = Service::new(config.clone(), metadata_db.clone(), meter).await?;

    let mut addrs = BoundAddrs {
        flight_addr: config.addrs.flight_addr,
        jsonl_addr: config.addrs.jsonl_addr,
    };

    // Start Arrow Flight Server if enabled
    let flight_fut = if enable_flight {
        let listener = TcpListener::bind(config.addrs.flight_addr)
            .await?
            .tap_io(|tcp_stream| tcp_stream.set_nodelay(true).unwrap());
        addrs.flight_addr = listener.local_addr()?;

        let app = axum::Router::new()
            .route("/healthz", get(|| async { StatusCode::OK }))
            .merge({
                let mut grpc_builder = Routes::builder();
                grpc_builder.add_service(FlightServiceServer::new(service.clone()));
                grpc_builder.routes().into_axum_router()
            });

        async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown_signal())
                .await
                .map_err(|err| {
                    tracing::error!(error=?err, "Flight server error");
                    err.into()
                })
        }
        .boxed()
    } else {
        Box::pin(std::future::pending())
    };

    // Start JSON Lines Server if enabled
    let jsonl_fut = if enable_jsonl {
        let (jsonl_addr, jsonl_server) =
            jsonl::run_server(service, config.addrs.jsonl_addr).await?;
        addrs.jsonl_addr = jsonl_addr;

        jsonl_server
            .map_err(|err| {
                tracing::error!(error=?err, "JSON lines server error");
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

#[derive(Debug, Clone, Copy)]
pub struct BoundAddrs {
    pub flight_addr: SocketAddr,
    pub jsonl_addr: SocketAddr,
}

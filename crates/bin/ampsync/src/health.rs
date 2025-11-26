//! Health check HTTP server

use std::net::SocketAddr;

use axum::{Router, http::StatusCode, routing::get};
use tokio::net::TcpListener;

/// Start the health check HTTP server.
pub async fn serve(
    addr: SocketAddr,
) -> Result<
    (
        SocketAddr,
        impl std::future::Future<Output = Result<(), std::io::Error>>,
    ),
    std::io::Error,
> {
    let listener = TcpListener::bind(addr).await?;
    let bound_addr = listener.local_addr()?;

    let app = Router::new().route("/healthz", get(|| async { StatusCode::OK }));

    let fut = async move { axum::serve(listener, app).await };

    Ok((bound_addr, fut))
}

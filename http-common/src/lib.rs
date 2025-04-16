use std::fmt::Display;
use std::net::SocketAddr;

use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::serve::ListenerExt as _;
use common::BoxError;
use serde_json::json;
use tokio::net::TcpListener;
use tokio::sync::broadcast;

pub type BoxRequestError = Box<dyn RequestError>;

pub trait RequestError: Display + Send + Sync + 'static {
    fn error_code(&self) -> &'static str;
    fn status_code(&self) -> StatusCode;
}

impl IntoResponse for BoxRequestError {
    fn into_response(self) -> axum::response::Response {
        let res = json!({
            "error_code": self.error_code(),
            "error_message": self.to_string(),
        });

        (self.status_code(), res.to_string()).into_response()
    }
}

impl<E: RequestError> From<E> for BoxRequestError {
    fn from(e: E) -> Self {
        Box::new(e)
    }
}

pub async fn serve_at(
    addr: SocketAddr,
    router: axum::Router,
    mut shutdown: broadcast::Receiver<()>,
) -> Result<(), BoxError> {
    let listener = TcpListener::bind(addr)
        .await?
        .tap_io(|tcp_stream| tcp_stream.set_nodelay(true).unwrap());
    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            shutdown.recv().await.ok();
        })
        .await?;
    Ok(())
}

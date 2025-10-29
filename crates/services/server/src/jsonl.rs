use std::{future::Future, net::SocketAddr};

use axum::{
    body::Body,
    extract::State,
    response::{IntoResponse, Response},
    routing::post,
    serve::{Listener as _, ListenerExt as _},
};
use common::{
    BoxError, BoxResult, arrow, query_context::parse_sql, stream_helpers::is_streaming,
    utils::shutdown_signal,
};
use futures::{StreamExt as _, TryStreamExt as _};
use tokio::net::TcpListener;
use tower_http::{compression::CompressionLayer, cors::CorsLayer};

use crate::service::Service;

pub async fn run_server(
    service: Service,
    addr: SocketAddr,
) -> BoxResult<(SocketAddr, impl Future<Output = BoxResult<()>>)> {
    let listener = TcpListener::bind(addr)
        .await?
        .tap_io(|tcp_stream| tcp_stream.set_nodelay(true).unwrap());
    let addr = listener.local_addr()?;

    let app = axum::Router::new()
        .route("/", post(handle_jsonl_request))
        .layer(CompressionLayer::new().gzip(true))
        .layer(CorsLayer::permissive())
        .with_state(service);

    let fut = async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .map_err(Into::into)
    };
    Ok((addr, fut))
}

#[tracing::instrument(skip_all)]
async fn handle_jsonl_request(State(service): State<Service>, request: String) -> Response {
    let stream = match service.execute_query(&request, None, None).await {
        Ok(stream) => stream,
        Err(err) => return err.into_response(),
    };

    let stream = stream
        .record_batches()
        .map(|res| -> Result<Vec<u8>, BoxError> {
            let batch = res.map_err(error_payload)?;
            let mut buf: Vec<u8> = Default::default();
            let mut writer = arrow::json::writer::LineDelimitedWriter::new(&mut buf);
            writer.write(&batch)?;
            Ok(buf)
        })
        .map_err(error_payload);

    let mut response = Response::builder().header("content-type", "application/x-ndjson");
    let query = match parse_sql(&request) {
        Ok(query) => query,
        Err(err) => return err.into_response(),
    };

    // For streaming queries, disable compression
    if is_streaming(&query) {
        response = response.header("content-encoding", "identity");
    }

    response.body(Body::from_stream(stream)).unwrap()
}

fn error_payload(message: impl std::fmt::Display) -> String {
    serde_json::json!({
        "error_code": "QUERY_ERROR",
        "error_message": message.to_string(),
    })
    .to_string()
}

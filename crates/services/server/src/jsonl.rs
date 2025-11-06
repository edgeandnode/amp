use axum::{
    Router,
    body::Body,
    extract::State,
    response::{IntoResponse, Response},
    routing::post,
};
use common::{BoxError, arrow, query_context::parse_sql, stream_helpers::is_streaming};
use futures::{StreamExt as _, TryStreamExt as _};
use tower_http::{compression::CompressionLayer, cors::CorsLayer};

use crate::flight::Service;

/// Build the JSON Lines HTTP router
///
/// Creates an axum router with a POST endpoint at "/" that accepts SQL queries
/// and returns results in JSON Lines format. Includes gzip compression and
/// permissive CORS middleware.
pub fn build_router(service: Service) -> Router {
    Router::new()
        .route("/", post(handle_jsonl_request))
        .layer(CompressionLayer::new().gzip(true))
        .layer(CorsLayer::permissive())
        .with_state(service)
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

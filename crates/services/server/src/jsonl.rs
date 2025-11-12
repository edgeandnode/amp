use axum::{
    Router,
    body::Body,
    extract::State,
    response::{IntoResponse, Response},
    routing::post,
};
use common::{BoxError, arrow, stream_helpers::is_streaming};
use datasets_derived::sql_str::SqlStr;
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
    // Step 1: Validate SQL string (non-empty, meaningful content)
    let sql_str = match request.parse::<SqlStr>() {
        Ok(sql) => sql,
        Err(err) => return bad_request_resp("INVALID_SQL_STRING", err),
    };

    // Step 2: Parse SQL statement (validate syntax, ensure single statement)
    let query = match common::sql::parse(&sql_str) {
        Ok(query) => query,
        Err(err) => return bad_request_resp("SQL_PARSE_ERROR", err),
    };

    // Step 3: Reject streaming queries
    if is_streaming(&query) {
        return bad_request_resp(
            "STREAMING_NOT_SUPPORTED",
            "Streaming queries (SETTINGS stream = true) are not supported on the JSONL endpoint. Please use the Arrow Flight endpoint instead.",
        );
    }

    // Step 4: Execute query
    let stream = match service.execute_query(&sql_str, None, None).await {
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

    Response::builder()
        .header("content-type", "application/x-ndjson")
        .body(Body::from_stream(stream))
        .unwrap()
}

/// Build a BAD_REQUEST error response with the given error code and message
fn bad_request_resp(code: &str, message: impl std::fmt::Display) -> Response {
    (
        axum::http::StatusCode::BAD_REQUEST,
        axum::Json(serde_json::json!({
            "error_code": code,
            "error_message": message.to_string(),
        })),
    )
        .into_response()
}

/// Build an error payload string for streaming errors
fn error_payload(message: impl std::fmt::Display) -> String {
    serde_json::json!({
        "error_code": "QUERY_ERROR",
        "error_message": message.to_string(),
    })
    .to_string()
}

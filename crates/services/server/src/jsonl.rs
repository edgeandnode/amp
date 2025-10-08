use std::{sync::Arc, time::Duration};

use axum::response::IntoResponse;
use common::{BoxError, config::Config, query_context::parse_sql, stream_helpers::is_streaming};
use futures::{StreamExt, TryStreamExt};
use metadata_db::MetadataDb;
use tracing::instrument;

use crate::{
    Service,
    health::{check_database, check_storage, check_workers},
};

/// State for JSON Lines server endpoints
#[derive(Clone)]
pub struct JsonlState {
    pub service: Service,
    pub config: Arc<Config>,
    pub metadata_db: MetadataDb,
}

/// Health check handler
pub async fn handle_health() -> &'static str {
    "OK"
}

/// Readiness check handler
#[instrument(skip(state))]
pub async fn handle_ready(
    axum::extract::State(state): axum::extract::State<JsonlState>,
) -> axum::response::Response {
    // Run all checks concurrently with overall timeout, short-circuit on first failure
    let result = tokio::time::timeout(Duration::from_millis(500), async {
        futures::try_join!(
            check_database(&state.metadata_db),
            check_storage(&state.config),
            check_workers(&state.metadata_db)
        )
    })
    .await;

    match result {
        Ok(Ok(_)) => (axum::http::StatusCode::OK, "OK").into_response(),
        Ok(Err(e)) => (axum::http::StatusCode::SERVICE_UNAVAILABLE, e).into_response(),
        Err(_) => (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "Readiness check timeout",
        )
            .into_response(),
    }
}

/// Query request handler
#[instrument(skip(state))]
pub async fn handle_request(
    axum::extract::State(state): axum::extract::State<JsonlState>,
    request: String,
) -> axum::response::Response {
    fn error_payload(message: impl std::fmt::Display) -> String {
        // Use http-common error format
        serde_json::json!({
            "error_code": "QUERY_ERROR",
            "error_message": message.to_string(),
        })
        .to_string()
    }

    let stream = match state.service.execute_query(&request).await {
        Ok(stream) => stream,
        Err(err) => return err.into_response(),
    };
    let stream = stream
        .record_batches()
        .map(|result| -> Result<Vec<u8>, BoxError> {
            let batch = result.map_err(error_payload)?;
            let mut buf: Vec<u8> = Default::default();
            let mut writer = common::arrow::json::writer::LineDelimitedWriter::new(&mut buf);
            writer.write(&batch)?;
            Ok(buf)
        })
        .map_err(error_payload);
    let mut response =
        axum::response::Response::builder().header("content-type", "application/x-ndjson");
    let query = match parse_sql(&request) {
        Ok(query) => query,
        Err(err) => return err.into_response(),
    };
    // For streaming queries, disable compression
    if is_streaming(&query) {
        response = response.header("content-encoding", "identity");
    }
    response
        .body(axum::body::Body::from_stream(stream))
        .unwrap()
}

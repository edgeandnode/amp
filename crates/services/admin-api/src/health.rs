use std::time::Duration;

use axum::{extract::State, response::IntoResponse};
use server::health::{check_database, check_storage, check_workers};

use crate::ctx::Ctx;

/// Health check handler
pub async fn handle_health() -> &'static str {
    "OK"
}

/// Readiness check handler
pub async fn handle_ready(State(ctx): State<Ctx>) -> impl IntoResponse {
    // Run all checks concurrently with overall timeout, short-circuit on first failure
    let result = tokio::time::timeout(Duration::from_millis(500), async {
        futures::try_join!(
            check_database(&ctx.metadata_db),
            check_storage(&ctx.config),
            check_workers(&ctx.metadata_db)
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

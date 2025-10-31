//! Workers get all handler

use axum::{Json, extract::State, http::StatusCode};
use worker::NodeId;

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
};

/// Handler for the `GET /workers` endpoint
///
/// Retrieves and returns a list of all workers from the metadata database.
///
/// ## Response
/// - **200 OK**: Returns all workers with their information
/// - **500 Internal Server Error**: Database connection or query error
///
/// ## Error Codes
/// - `METADATA_DB_ERROR`: Internal database error occurred
///
/// This handler:
/// - Fetches all workers from the metadata database
/// - Converts worker records to API response format with ISO 8601 RFC3339 timestamps
/// - Returns a structured response with worker information including node IDs and last heartbeat times
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        get,
        path = "/workers",
        tag = "workers",
        operation_id = "workers_list",
        responses(
            (status = 200, description = "Successfully retrieved workers", body = WorkersResponse),
            (status = 500, description = "Internal server error", body = crate::handlers::error::ErrorResponse)
        )
    )
)]
pub async fn handler(State(ctx): State<Ctx>) -> Result<Json<WorkersResponse>, ErrorResponse> {
    // Fetch all workers from metadata DB
    let workers = metadata_db::workers::list(&ctx.metadata_db)
        .await
        .map_err(|err| {
            tracing::debug!(error=?err, "failed to list workers");
            Error::MetadataDbError(err)
        })?;

    let workers = workers.into_iter().map(Into::into).collect();

    Ok(Json(WorkersResponse { workers }))
}

/// Collection response for worker listings
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct WorkersResponse {
    /// List of workers
    pub workers: Vec<WorkerInfo>,
}

/// Errors that can occur during worker listing
///
/// This enum represents all possible error conditions that can occur
/// when handling a `GET /workers` request.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An error occurred while querying the metadata database
    ///
    /// This covers database connection issues, query failures,
    /// and other internal database errors.
    #[error("metadata db error: {0}")]
    MetadataDbError(#[from] metadata_db::Error),
}

impl IntoErrorResponse for Error {
    /// Returns the error code string for API responses
    ///
    /// These error codes are returned in the API response body to help
    /// clients programmatically identify and handle different error types.
    fn error_code(&self) -> &'static str {
        match self {
            Error::MetadataDbError(_) => "METADATA_DB_ERROR",
        }
    }

    /// Returns the appropriate HTTP status code for each error type
    ///
    /// Maps internal error types to standard HTTP status codes:
    /// - Database Error → 500 Internal Server Error
    fn status_code(&self) -> StatusCode {
        match self {
            Error::MetadataDbError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

/// Worker information returned by the API
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct WorkerInfo {
    /// ID of the worker node
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub node_id: NodeId,
    /// Last heartbeat timestamp in ISO 8601 RFC3339 format
    pub heartbeat_at: String,
}

impl From<metadata_db::Worker> for WorkerInfo {
    fn from(worker: metadata_db::Worker) -> Self {
        Self {
            node_id: worker.node_id.into(),
            heartbeat_at: worker.heartbeat_at.to_rfc3339(),
        }
    }
}

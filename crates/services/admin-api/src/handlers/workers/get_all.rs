//! Workers get all handler

use axum::{Json, extract::State, http::StatusCode};
use monitoring::logging;
use worker::node_id::NodeId;

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
};

/// Handler for the `GET /workers` endpoint
///
/// Retrieves and returns a list of all workers from the scheduler.
///
/// ## Response
/// - **200 OK**: Returns all workers with their information
/// - **500 Internal Server Error**: Scheduler query error
///
/// ## Error Codes
/// - `SCHEDULER_LIST_WORKERS_ERROR`: Failed to retrieve workers list from scheduler
///
/// This handler:
/// - Fetches all workers from the scheduler
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
    // Fetch all workers from scheduler
    let workers = ctx.scheduler.list_workers().await.map_err(|err| {
        tracing::debug!(error = %err, error_source = logging::error_source(&err), "failed to list workers");
        Error::SchedulerListWorkers(err)
    })?;

    let workers = workers.into_iter().map(Into::into).collect();

    Ok(Json(WorkersResponse { workers }))
}

/// Collection response for worker listings
///
/// Contains a list of all registered workers in the system with their
/// basic information including node identifiers and last heartbeat times.
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct WorkersResponse {
    /// List of all registered workers
    ///
    /// Each worker entry contains the node ID and last heartbeat timestamp.
    /// Workers are ordered by their database insertion order.
    pub workers: Vec<WorkerInfo>,
}

/// Errors that can occur during worker listing
///
/// This enum represents all possible error conditions that can occur
/// when handling a `GET /workers` request.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to list workers from the scheduler
    ///
    /// This occurs when:
    /// - Database connection fails or is lost during the query
    /// - Query execution encounters an internal database error
    /// - Connection pool is exhausted or unavailable
    #[error("failed to list workers: {0}")]
    SchedulerListWorkers(#[source] crate::scheduler::ListWorkersError),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::SchedulerListWorkers(_) => "SCHEDULER_LIST_WORKERS_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::SchedulerListWorkers(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

/// Worker information returned by the API
///
/// Contains basic identification and liveness information for a worker node.
/// This is a lightweight summary view suitable for list endpoints.
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct WorkerInfo {
    /// Unique identifier for the worker node
    ///
    /// A persistent identifier that uniquely identifies this worker across registrations
    /// and heartbeats. Used for tracking and managing individual worker instances.
    ///
    /// Must start with a letter and contain only alphanumeric characters, underscores,
    /// hyphens, and dots.
    #[cfg_attr(
        feature = "utoipa",
        schema(
            value_type = String,
            pattern = r"^[a-zA-Z][a-zA-Z0-9_\-\.]*$",
            examples("worker-01h2xcejqtf2nbrexx3vqjhp41", "indexer-node-1", "amp_worker.prod")
        )
    )]
    pub node_id: NodeId,

    /// Last heartbeat timestamp (RFC3339 format)
    ///
    /// The most recent time this worker sent a heartbeat signal. Workers send
    /// periodic heartbeats to indicate they are alive and processing work.
    /// A stale heartbeat indicates the worker may be down or unreachable.
    #[cfg_attr(
        feature = "utoipa",
        schema(format = "date-time", examples("2025-01-15T17:20:15.456789Z"))
    )]
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

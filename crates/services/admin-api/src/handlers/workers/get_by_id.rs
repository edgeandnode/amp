//! Workers get by ID handler

use axum::{
    Json,
    extract::{Path, State, rejection::PathRejection},
    http::StatusCode,
};
use worker::{NodeId, WorkerInfo};

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
};

/// Handler for the `GET /workers/{id}` endpoint
///
/// Retrieves and returns a specific worker by its node ID from the scheduler.
///
/// ## Path Parameters
/// - `id`: The unique node identifier of the worker to retrieve
///
/// ## Response
/// - **200 OK**: Returns the worker information as JSON with detailed metadata
/// - **400 Bad Request**: Invalid node ID format (not parseable as NodeId)
/// - **404 Not Found**: Worker with the given node ID does not exist
/// - **500 Internal Server Error**: Scheduler query error
///
/// ## Error Codes
/// - `INVALID_WORKER_ID`: The provided ID is not a valid worker node identifier
/// - `WORKER_NOT_FOUND`: No worker exists with the given node ID
/// - `SCHEDULER_GET_WORKER_ERROR`: Failed to retrieve worker from scheduler
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        get,
        path = "/workers/{id}",
        tag = "workers",
        operation_id = "workers_get",
        params(
            ("id" = String, Path, description = "Worker node ID")
        ),
        responses(
            (status = 200, description = "Successfully retrieved worker information", body = WorkerDetailResponse),
            (status = 400, description = "Invalid worker ID", body = crate::handlers::error::ErrorResponse),
            (status = 404, description = "Worker not found", body = crate::handlers::error::ErrorResponse),
            (status = 500, description = "Internal server error", body = crate::handlers::error::ErrorResponse)
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    path: Result<Path<NodeId>, PathRejection>,
) -> Result<Json<WorkerDetailResponse>, ErrorResponse> {
    let id = match path {
        Ok(Path(path)) => path,
        Err(err) => {
            tracing::debug!(error=?err, "invalid worker node ID in path");
            return Err(Error::InvalidId(err).into());
        }
    };

    match ctx.scheduler.get_worker_by_id(&id).await {
        Ok(Some(worker)) => Ok(Json(worker.into())),
        Ok(None) => Err(Error::NotFound { id }.into()),
        Err(err) => {
            tracing::debug!(error=?err, worker_id=?id, "failed to get worker by ID");
            Err(Error::SchedulerGetWorker(err).into())
        }
    }
}

/// Detailed worker information returned by the API
///
/// Contains comprehensive information about a worker node including its identity,
/// lifecycle timestamps, and build metadata. This response enables monitoring of
/// worker health, version tracking, and operational status.
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct WorkerDetailResponse {
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

    /// Worker metadata including version and build information
    ///
    /// Contains detailed build and version information for this worker,
    /// including git version, commit details, and build timestamps.
    pub info: WorkerMetadata,

    /// Timestamp when the worker was first created in the system (RFC3339 format)
    ///
    /// The initial registration time of this worker. This timestamp never changes
    /// and represents when the worker first appeared in the system.
    #[cfg_attr(
        feature = "utoipa",
        schema(format = "date-time", examples("2025-01-15T14:30:00.123456Z"))
    )]
    pub created_at: String,

    /// Timestamp when the worker last registered (RFC3339 format)
    ///
    /// Updated each time a worker re-registers with the system. Workers typically
    /// re-register on startup or reconnection. Use this to track worker restarts.
    #[cfg_attr(
        feature = "utoipa",
        schema(format = "date-time", examples("2025-01-15T16:45:30.789012Z"))
    )]
    pub registered_at: String,

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

/// Worker metadata containing build and version information
///
/// This struct captures comprehensive build and version details for a worker node,
/// enabling tracking of deployed versions and troubleshooting version-specific issues.
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct WorkerMetadata {
    /// Version string including git describe output
    ///
    /// Format: `v{major}.{minor}.{patch}[-{commits_since_tag}-g{short_sha}][-dirty]`
    ///
    /// The "-dirty" suffix indicates uncommitted changes in the working directory.
    /// Returns "unknown" if version information is not available.
    #[cfg_attr(
        feature = "utoipa",
        schema(examples(
            "v0.0.22",
            "v0.0.22-dirty",
            "v0.0.22-15-g8b065bde",
            "v0.0.22-15-g8b065bde-dirty",
            "unknown"
        ))
    )]
    pub version: String,

    /// Full Git commit SHA (40-character hexadecimal)
    ///
    /// The complete SHA-1 hash of the commit from which this worker was built.
    /// Used for precise version identification and source code correlation.
    ///
    /// Returns "unknown" if commit information is not available.
    #[cfg_attr(
        feature = "utoipa",
        schema(examples("8b065bde9c1a2f3e4d5c6b7a8e9f0a1b2c3d4e5f", "unknown"))
    )]
    pub commit_sha: String,

    /// Timestamp when the commit was created (RFC3339 format)
    ///
    /// The date and time when the source code commit was made to the repository.
    /// Helps correlate worker versions with development timeline.
    ///
    /// Returns "unknown" if timestamp is not available.
    #[cfg_attr(
        feature = "utoipa",
        schema(
            format = "date-time",
            examples("2025-01-15T14:30:00Z", "2025-01-15T09:30:00-05:00", "unknown")
        )
    )]
    pub commit_timestamp: String,

    /// Date and time when the worker binary was built (RFC3339 format)
    ///
    /// The timestamp when the build process completed. May differ from commit
    /// timestamp, especially for CI/CD builds or local development builds.
    ///
    /// Returns "unknown" if build date is not available.
    #[cfg_attr(
        feature = "utoipa",
        schema(
            format = "date-time",
            examples("2025-01-15T15:45:30Z", "2025-01-15T10:45:30-05:00", "unknown")
        )
    )]
    pub build_date: String,
}

/// Default function returning "unknown" string
fn default_unknown() -> String {
    "unknown".to_string()
}

impl Default for WorkerMetadata {
    fn default() -> Self {
        Self {
            version: default_unknown(),
            commit_sha: default_unknown(),
            commit_timestamp: default_unknown(),
            build_date: default_unknown(),
        }
    }
}

impl From<metadata_db::Worker> for WorkerDetailResponse {
    fn from(worker: metadata_db::Worker) -> Self {
        // Convert the WorkerInfoOwned (JSON RawValue wrapper) from the database
        // into the strongly-typed WorkerInfo struct from the worker crate.
        // If deserialization fails or fields are missing, fall back to defaults.
        let info = worker
            .info
            .try_into()
            .map(|info: WorkerInfo| WorkerMetadata {
                version: info.version.unwrap_or(default_unknown()),
                commit_sha: info.commit_sha.unwrap_or(default_unknown()),
                commit_timestamp: info.commit_timestamp.unwrap_or(default_unknown()),
                build_date: info.build_date.unwrap_or(default_unknown()),
            })
            .unwrap_or_default();

        Self {
            node_id: worker.node_id.into(),
            created_at: worker.created_at.to_rfc3339(),
            registered_at: worker.registered_at.to_rfc3339(),
            heartbeat_at: worker.heartbeat_at.to_rfc3339(),
            info,
        }
    }
}

/// Errors that can occur during worker retrieval
///
/// This enum represents all possible error conditions that can occur
/// when handling a `GET /workers/{id}` request, from path parsing
/// to scheduler operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The worker node ID in the URL path is invalid
    ///
    /// This occurs when:
    /// - The ID cannot be parsed as a valid node identifier
    /// - The path parameter is missing or malformed
    /// - The ID format does not match the expected NodeId type
    #[error("invalid worker node ID: {0}")]
    InvalidId(#[source] PathRejection),

    /// The requested worker was not found
    ///
    /// This occurs when:
    /// - No worker exists with the specified node ID
    /// - The worker was removed after the request was made
    /// - The node ID refers to a nonexistent record
    #[error("worker '{id}' not found")]
    NotFound { id: NodeId },

    /// Failed to retrieve worker from the scheduler
    ///
    /// This occurs when:
    /// - Database connection fails or is lost during the query
    /// - Query execution encounters an internal database error
    /// - Connection pool is exhausted or unavailable
    #[error("failed to get worker by ID: {0}")]
    SchedulerGetWorker(#[source] crate::scheduler::GetWorkerError),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidId(_) => "INVALID_WORKER_ID",
            Error::NotFound { .. } => "WORKER_NOT_FOUND",
            Error::SchedulerGetWorker(_) => "SCHEDULER_GET_WORKER_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidId(_) => StatusCode::BAD_REQUEST,
            Error::NotFound { .. } => StatusCode::NOT_FOUND,
            Error::SchedulerGetWorker(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

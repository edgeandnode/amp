//! Jobs stop handler

use axum::{
    extract::{Path, State, rejection::PathRejection},
    http::StatusCode,
};
use worker::JobId;

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
    scheduler::StopJobError,
};

/// Handler for the `PUT /jobs/{id}/stop` endpoint
///
/// Stops a running job using the specified job ID. This is an idempotent
/// operation that handles job termination requests safely.
///
/// ## Path Parameters
/// - `id`: The unique identifier of the job to stop (must be a valid JobId)
///
/// ## Response
/// - **200 OK**: Job stop request processed successfully
/// - **400 Bad Request**: Invalid job ID format (not parseable as JobId)
/// - **404 Not Found**: Job with the given ID does not exist
/// - **409 Conflict**: Job cannot be stopped from current state (already terminal or invalid transition)
/// - **500 Internal Server Error**: Database connection or scheduler error
///
/// ## Error Codes
/// - `INVALID_JOB_ID`: The provided ID is not a valid job identifier
/// - `JOB_NOT_FOUND`: No job exists with the given ID
/// - `JOB_CONFLICT`: Job is in a state that cannot be stopped (terminal or invalid transition)
/// - `METADATA_DB_ERROR`: Internal database error occurred
///
/// ## Behavior
/// This handler provides idempotent job stopping with the following characteristics:
/// - Jobs already in terminal states return conflict error
/// - Only running/scheduled jobs can be transitioned to stop-requested
/// - The scheduler handles atomic status updates and worker notifications
/// - Database layer validates state transitions and prevents race conditions
///
/// ## State Transitions
/// Valid stop transitions:
/// - Scheduled → StopRequested
/// - Running → StopRequested
///
/// Invalid transitions (return conflict):
/// - Completed → (no change)
/// - Failed → (no change)
/// - Stopped → (no change)
/// - Unknown → (no change)
///
/// The handler:
/// - Validates and extracts the job ID from the URL path
/// - Retrieves current job status to validate it exists
/// - Delegates to scheduler for atomic stop operation with worker notification
/// - Returns appropriate HTTP status codes and error messages
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        put,
        path = "/jobs/{id}/stop",
        tag = "jobs",
        operation_id = "jobs_stop",
        params(
            ("id" = i64, Path, description = "Job ID")
        ),
        responses(
            (status = 200, description = "Job stop request processed successfully"),
            (status = 400, description = "Invalid job ID"),
            (status = 404, description = "Job not found"),
            (status = 409, description = "Job cannot be stopped from current state"),
            (status = 500, description = "Internal server error")
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    path: Result<Path<JobId>, PathRejection>,
) -> Result<StatusCode, ErrorResponse> {
    let id = match path {
        Ok(Path(path)) => path,
        Err(err) => {
            tracing::debug!(error=?err, "invalid job ID in path");
            return Err(Error::InvalidId { err }.into());
        }
    };

    // Get current job status to validate it can be stopped
    let job = ctx.metadata_db.get_job(&id).await.map_err(|err| {
        tracing::debug!(error=?err, job_id=?id, "failed to get job");
        Error::MetadataDbError(err)
    })?;

    let job = match job {
        Some(job) => job,
        None => return Err(Error::NotFound { id }.into()),
    };

    // Delegate to scheduler for atomic stop operation
    // The database layer handles validation
    ctx.scheduler
        .stop_job(&id, &job.node_id.into())
        .await
        .map_err(|err| {
            match err {
                StopJobError::JobNotFound => {
                    Error::NotFound { id }
                }
                StopJobError::JobAlreadyTerminated { status } => {
                    tracing::debug!(job_id=?id, actual_status=?status, "job already in terminal state");
                    Error::Conflict {
                        message: format!("Job is already in terminal state: {}", status)
                    }
                }
                StopJobError::StateConflict { current_status } => {
                    tracing::debug!(job_id=?id, current_status=?current_status, "cannot stop job from current state");
                    Error::Conflict {
                        message: format!("Cannot stop job from current state: {}", current_status)
                    }
                }
                StopJobError::MetadataDb(metadata_err) => {
                    tracing::error!(error=?metadata_err, job_id=?id, "metadata database error");
                    Error::MetadataDbError(metadata_err)
                }
            }
        })?;

    Ok(StatusCode::OK)
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The job ID in the URL path is invalid
    ///
    /// This occurs when the ID cannot be parsed as a valid JobId.
    #[error("invalid job ID: {err}")]
    InvalidId {
        /// The rejection details from Axum's path extractor
        err: PathRejection,
    },

    /// Job not found
    #[error("job '{id}' not found")]
    NotFound {
        /// The job ID that was not found
        id: JobId,
    },

    /// Metadata DB error
    #[error("metadata db error: {0}")]
    MetadataDbError(#[from] metadata_db::Error),

    /// Job state conflict (cannot perform operation)
    #[error("job conflict: {message}")]
    Conflict { message: String },
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidId { .. } => "INVALID_JOB_ID",
            Error::NotFound { .. } => "JOB_NOT_FOUND",
            Error::MetadataDbError(_) => "METADATA_DB_ERROR",
            Error::Conflict { .. } => "JOB_CONFLICT",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidId { .. } => StatusCode::BAD_REQUEST,
            Error::NotFound { .. } => StatusCode::NOT_FOUND,
            Error::MetadataDbError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::Conflict { .. } => StatusCode::CONFLICT,
        }
    }
}

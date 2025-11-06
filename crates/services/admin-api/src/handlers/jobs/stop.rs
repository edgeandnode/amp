//! Jobs stop handler

use axum::{
    extract::{Path, State, rejection::PathRejection},
    http::StatusCode,
};
use worker::JobId;

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
    scheduler,
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
/// - **200 OK**: Job stop request processed successfully, or job already in terminal state (idempotent)
/// - **400 Bad Request**: Invalid job ID format (not parseable as JobId)
/// - **404 Not Found**: Job with the given ID does not exist
/// - **500 Internal Server Error**: Database connection or scheduler error
///
/// ## Error Codes
/// - `INVALID_JOB_ID`: The provided ID is not a valid job identifier
/// - `JOB_NOT_FOUND`: No job exists with the given ID
/// - `STOP_JOB_ERROR`: Database error during stop operation execution
/// - `UNEXPECTED_STATE_CONFLICT`: Internal state machine error (indicates a bug)
///
/// ## Idempotent Behavior
/// This handler is idempotent - stopping a job that's already in a terminal state returns success (200).
/// This allows clients to safely retry stop requests without worrying about conflict errors.
///
/// The desired outcome of a stop request is that the job is not running. If the job is already
/// stopped, completed, or failed, this outcome is achieved, so we return success.
///
/// ## Behavior
/// This handler provides idempotent job stopping with the following characteristics:
/// - Jobs already in terminal states (Stopped, Completed, Failed) return success (idempotent)
/// - Only running/scheduled jobs transition to stop-requested state
/// - Job lookup and stop request are performed atomically within a single transaction
/// - Database layer validates state transitions and prevents race conditions
///
/// ## State Transitions
/// Valid stop transitions:
/// - Scheduled → StopRequested (200 OK)
/// - Running → StopRequested (200 OK)
///
/// Already terminal (idempotent - return success):
/// - Stopped → no change (200 OK)
/// - Completed → no change (200 OK)
/// - Failed → no change (200 OK)
///
/// The handler:
/// - Validates and extracts the job ID from the URL path
/// - Delegates to scheduler for atomic stop operation (job lookup + stop + worker notification)
/// - Returns success if job is already in terminal state (idempotent)
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
            (status = 200, description = "Job stop request processed successfully, or job already in terminal state (idempotent)"),
            (status = 400, description = "Invalid job ID", body = crate::handlers::error::ErrorResponse),
            (status = 404, description = "Job not found", body = crate::handlers::error::ErrorResponse),
            (status = 500, description = "Internal server error", body = crate::handlers::error::ErrorResponse)
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    path: Result<Path<JobId>, PathRejection>,
) -> Result<StatusCode, ErrorResponse> {
    let id = match path {
        Ok(Path(id)) => id,
        Err(err) => {
            tracing::debug!(error=?err, "invalid job ID in path");
            return Err(Error::InvalidId { err }.into());
        }
    };

    // Attempt to stop the job - this operation is atomic and includes job lookup
    if let Err(err) = ctx.scheduler.stop_job(id).await {
        return match err {
            scheduler::StopJobError::JobNotFound => {
                tracing::debug!(job_id=%id, "Job not found");
                Err(Error::NotFound { id }.into())
            }
            scheduler::StopJobError::JobAlreadyTerminated { status } => {
                // Idempotent behavior: job is already in terminal state
                tracing::debug!(job_id=%id, status=%status, "Job already in terminal state, returning success (idempotent)");
                Ok(StatusCode::OK)
            }
            scheduler::StopJobError::StateConflict { current_status } => {
                // Unexpected state conflict - this shouldn't happen with current state machine
                tracing::error!(
                    job_id=%id,
                    current_status=%current_status,
                    "Unexpected state conflict during stop operation"
                );
                Err(Error::UnexpectedStateConflict {
                    id,
                    current_status: current_status.to_string(),
                }
                .into())
            }
            scheduler::StopJobError::BeginTransaction(_)
            | scheduler::StopJobError::GetJob(_)
            | scheduler::StopJobError::UpdateJobStatus(_)
            | scheduler::StopJobError::CommitTransaction(_)
            | scheduler::StopJobError::SendNotification(_) => {
                tracing::error!(error=?err, job_id=%id, "Database error during stop operation");
                Err(Error::StopJob { id, source: err }.into())
            }
        };
    }

    tracing::info!(job_id=%id, "Job stop request processed successfully");
    Ok(StatusCode::OK)
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The job ID in the URL path is invalid
    ///
    /// This occurs when the ID cannot be parsed as a valid JobId.
    #[error("invalid job ID: {err}")]
    InvalidId { err: PathRejection },

    /// Job not found
    ///
    /// This occurs when the job ID is valid but no job
    /// record exists with that ID in the metadata database.
    #[error("job '{id}' not found")]
    NotFound { id: JobId },

    /// Database error during stop operation
    ///
    /// This error occurs when the scheduler encounters a database error
    /// while executing the atomic stop operation (job lookup, state transition, or worker notification).
    ///
    /// This occurs when:
    /// - Database connection fails or is lost during the transaction
    /// - Transaction conflicts or deadlocks occur
    /// - Database constraint violations are encountered
    #[error("failed to stop job '{id}'")]
    StopJob {
        id: JobId,
        source: scheduler::StopJobError,
    },

    /// Unexpected state conflict during stop operation
    ///
    /// This error indicates an internal inconsistency in the state machine.
    /// It should not occur under normal operation and indicates a bug that
    /// requires investigation.
    ///
    /// This occurs when:
    /// - State machine has invalid transition rules
    /// - Concurrent state modifications occur without proper locking
    /// - Database state corruption is detected
    #[error("unexpected state conflict for job '{id}': current state is '{current_status}'")]
    UnexpectedStateConflict { id: JobId, current_status: String },
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidId { .. } => "INVALID_JOB_ID",
            Error::NotFound { .. } => "JOB_NOT_FOUND",
            Error::StopJob { .. } => "STOP_JOB_ERROR",
            Error::UnexpectedStateConflict { .. } => "UNEXPECTED_STATE_CONFLICT",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidId { .. } => StatusCode::BAD_REQUEST,
            Error::NotFound { .. } => StatusCode::NOT_FOUND,
            Error::StopJob { .. } => StatusCode::INTERNAL_SERVER_ERROR,

            // Internal server error for unexpected state conflicts (indicates a bug)
            Error::UnexpectedStateConflict { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

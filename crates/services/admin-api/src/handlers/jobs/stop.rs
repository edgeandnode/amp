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
/// - **200 OK**: Job stop request processed successfully, or job already in terminal state (idempotent)
/// - **400 Bad Request**: Invalid job ID format (not parseable as JobId)
/// - **404 Not Found**: Job with the given ID does not exist
/// - **500 Internal Server Error**: Database connection or scheduler error
///
/// ## Error Codes
/// - `INVALID_JOB_ID`: The provided ID is not a valid job identifier
/// - `JOB_NOT_FOUND`: No job exists with the given ID
/// - `GET_JOB_DB_ERROR`: Database error while fetching job information
/// - `STOP_JOB_DB_ERROR`: Database error during stop operation execution
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
/// - The scheduler handles atomic status updates and worker notifications
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
/// - Retrieves current job status to validate it exists
/// - Returns success if job is already in terminal state (idempotent)
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

    // Get current job status to validate it exists before attempting stop
    let job = ctx.metadata_db.get_job(&id).await.map_err(|err| {
        tracing::error!(
            error=?err,
            job_id=%id,
            "Database error while fetching job for stop operation"
        );
        Error::GetJobDbError { id, source: err }
    })?;

    let job = match job {
        Some(job) => {
            tracing::debug!(
                job_id=%id,
                status=%job.status,
                node_id=%job.node_id,
                "Job found, proceeding with stop operation"
            );
            job
        }
        None => {
            tracing::debug!(job_id=%id, "Job not found");
            return Err(Error::NotFound { id }.into());
        }
    };

    // Delegate to scheduler for atomic stop operation
    // The database layer handles validation
    match ctx.scheduler.stop_job(&id, &job.node_id.into()).await {
        Ok(()) => {
            tracing::info!(job_id=%id, "Job stop request processed successfully");
            Ok(StatusCode::OK)
        }
        Err(err) => match err {
            StopJobError::JobNotFound => {
                // Job was deleted between our get and stop calls
                tracing::warn!(job_id=%id, "Job not found during stop operation (race condition)");
                Err(Error::NotFound { id }.into())
            }
            StopJobError::JobAlreadyTerminated { status } => {
                // Idempotent behavior: job is already in terminal state
                tracing::debug!(job_id=%id, status=%status, "Job already in terminal state, returning success (idempotent)");
                Ok(StatusCode::OK)
            }
            StopJobError::StateConflict { current_status } => {
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
            StopJobError::MetadataDb(metadata_err) => {
                tracing::error!(error=?metadata_err, job_id=%id, "Metadata database error during stop operation");
                Err(Error::StopJobDbError {
                    id,
                    source: metadata_err,
                }
                .into())
            }
        },
    }
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
    ///
    /// This occurs when the job ID is valid but no job
    /// record exists with that ID in the metadata database.
    ///
    /// Note: This can also occur as a race condition when a job is deleted
    /// between the initial get operation and the stop operation.
    #[error("job '{id}' not found")]
    NotFound {
        /// The job ID that was not found
        id: JobId,
    },

    /// Database error while fetching job information
    ///
    /// This error occurs specifically during the initial job lookup operation
    /// before attempting to stop the job.
    ///
    /// Common scenarios:
    /// - Database connection failures
    /// - Connection pool exhaustion
    /// - Query timeout
    /// - Transient network issues
    ///
    /// Debugging hints:
    /// - Check database connectivity
    /// - Verify connection pool settings
    /// - Monitor database performance metrics
    #[error("database error while fetching job '{id}': {source}")]
    GetJobDbError {
        /// The job ID that was being fetched
        id: JobId,
        /// The underlying database error
        source: metadata_db::Error,
    },

    /// Database error during stop operation
    ///
    /// This error occurs when the scheduler encounters a database error
    /// while executing the stop operation (state transition, worker notification, etc.).
    ///
    /// Common scenarios:
    /// - Transaction conflicts or deadlocks
    /// - Connection lost during multi-step operation
    /// - Database constraint violations
    /// - Worker notification database write failures
    ///
    /// Debugging hints:
    /// - Check for concurrent job modifications
    /// - Review database transaction logs
    /// - Verify worker notification system health
    #[error("database error during stop operation for job '{id}': {source}")]
    StopJobDbError {
        /// The job ID being stopped
        id: JobId,
        /// The underlying database error
        source: metadata_db::Error,
    },

    /// Unexpected state conflict during stop operation
    ///
    /// This error indicates an internal inconsistency in the state machine.
    /// It should not occur under normal operation and indicates a bug that
    /// requires investigation.
    ///
    /// Common scenarios:
    /// - State machine has invalid transition rules
    /// - Concurrent state modifications without proper locking
    /// - Database state corruption
    #[error("unexpected state conflict for job '{id}': current state is '{current_status}'")]
    UnexpectedStateConflict {
        /// The job ID that experienced the conflict
        id: JobId,
        /// The current status that caused the conflict
        current_status: String,
    },
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidId { .. } => "INVALID_JOB_ID",
            Error::NotFound { .. } => "JOB_NOT_FOUND",
            Error::GetJobDbError { .. } => "GET_JOB_DB_ERROR",
            Error::StopJobDbError { .. } => "STOP_JOB_DB_ERROR",
            Error::UnexpectedStateConflict { .. } => "UNEXPECTED_STATE_CONFLICT",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidId { .. } => StatusCode::BAD_REQUEST,
            Error::NotFound { .. } => StatusCode::NOT_FOUND,
            Error::GetJobDbError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Error::StopJobDbError { .. } => StatusCode::INTERNAL_SERVER_ERROR,

            // Internal server error for unexpected state conflicts (indicates a bug)
            Error::UnexpectedStateConflict { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

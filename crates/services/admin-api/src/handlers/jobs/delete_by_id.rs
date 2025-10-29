//! Jobs delete by ID handler

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

/// Handler for the `DELETE /jobs/{id}` endpoint
///
/// Deletes a job by its ID if it's in a terminal state (Completed, Stopped, or Failed).
/// This is a safe, idempotent operation that only removes finalized jobs from the system.
///
/// ## Path Parameters
/// - `id`: The unique identifier of the job to delete (must be a valid JobId)
///
/// ## Response
/// - **204 No Content**: Job was successfully deleted or does not exist (idempotent)
/// - **400 Bad Request**: Invalid job ID format (not parseable as JobId)
/// - **409 Conflict**: Job exists but is not in a terminal state (cannot be deleted)
/// - **500 Internal Server Error**: Database error occurred
///
/// ## Error Codes
/// - `INVALID_JOB_ID`: The provided ID is not a valid job identifier
/// - `JOB_CONFLICT`: Job exists but is not in a terminal state
/// - `GET_JOB_ERROR`: Failed to retrieve job from scheduler (database error)
/// - `DELETE_JOB_ERROR`: Failed to delete job from scheduler (database error)
///
/// ## Idempotent Behavior
/// This handler is idempotent - deleting a non-existent job returns 204 (success).
/// This allows clients to safely retry deletions without worrying about 404 errors.
///
/// ## Behavior
/// This handler provides safe job deletion with the following characteristics:
/// - Only jobs in terminal states (Completed, Stopped, Failed) can be deleted
/// - Non-terminal jobs are protected from accidental deletion
/// - Non-existent jobs return success (idempotent behavior)
/// - Database layer ensures atomic deletion
///
/// ## Terminal States
/// Jobs can only be deleted when in these states:
/// - Completed → Safe to delete
/// - Stopped → Safe to delete  
/// - Failed → Safe to delete
///
/// Protected states (cannot be deleted):
/// - Scheduled → Job is waiting to run
/// - Running → Job is actively executing
/// - StopRequested → Job is being stopped
/// - Stopping → Job is in process of stopping
/// - Unknown → Invalid state
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        delete,
        path = "/jobs/{id}",
        tag = "jobs",
        operation_id = "jobs_delete",
        params(
            ("id" = i64, Path, description = "Job ID")
        ),
        responses(
            (status = 204, description = "Job deleted successfully or does not exist (idempotent)"),
            (status = 400, description = "Invalid job ID", body = crate::handlers::error::ErrorResponse),
            (status = 409, description = "Job cannot be deleted (not in terminal state)", body = crate::handlers::error::ErrorResponse),
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

    // First, check if the job exists and get its status
    let job = ctx.scheduler.get_job(&id).await.map_err(|err| {
        tracing::debug!(job_id=%id, error=?err, "failed to get job");
        Error::GetJob(err)
    })?;

    let Some(job) = job else {
        // Idempotent behavior: return success if job doesn't exist
        tracing::debug!(job_id=%id, "job not found, returning success (idempotent)");
        return Ok(StatusCode::NO_CONTENT);
    };

    // Check if the job is in a terminal state
    if !job.status.is_terminal() {
        tracing::debug!(job_id=%id, status=%job.status, "job is not in terminal state");
        return Err(Error::Conflict {
            id,
            status: job.status,
        }
        .into());
    }

    // Attempt to delete the job
    let deleted = ctx.scheduler.delete_job(&id).await.map_err(|err| {
        tracing::error!(job_id=%id, error=?err, "failed to delete job");
        Error::DeleteJob(err)
    })?;

    if deleted {
        tracing::info!(job_id=%id, "successfully deleted terminal job");
    } else {
        tracing::warn!(job_id=%id, "deletion did not affect any rows, but job should have been deletable");
    }

    Ok(StatusCode::NO_CONTENT)
}

/// Errors that can occur during job deletion by ID
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The job ID in the URL path is invalid
    ///
    /// This occurs when:
    /// - The ID cannot be parsed as a valid integer
    /// - The path parameter is missing or malformed
    /// - The ID format does not match the expected JobId type
    #[error("invalid job ID: {err}")]
    InvalidId {
        /// The rejection details from Axum's path extractor
        err: PathRejection,
    },

    /// Job exists but cannot be deleted (not in terminal state)
    ///
    /// This occurs when:
    /// - The job is currently running or scheduled
    /// - The job is in the process of being stopped
    /// - Attempting to delete a non-terminal job (protected state)
    #[error("job '{id}' cannot be deleted from current state: {status}")]
    Conflict {
        /// The job ID that cannot be deleted
        id: JobId,
        /// The current status of the job
        status: metadata_db::JobStatus,
    },

    /// Failed to retrieve job from scheduler
    ///
    /// This occurs when:
    /// - Database connection fails or is lost during the query
    /// - Query execution encounters an internal database error
    /// - Connection pool is exhausted or unavailable
    #[error("failed to get job")]
    GetJob(#[source] scheduler::GetJobError),

    /// Failed to delete job from scheduler
    ///
    /// This occurs when:
    /// - Database connection fails or is lost during deletion
    /// - Delete operation encounters an internal database error
    /// - Connection pool is exhausted or unavailable
    #[error("failed to delete job")]
    DeleteJob(#[source] scheduler::DeleteJobError),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidId { .. } => "INVALID_JOB_ID",
            Error::Conflict { .. } => "JOB_CONFLICT",
            Error::GetJob(_) => "GET_JOB_ERROR",
            Error::DeleteJob(_) => "DELETE_JOB_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidId { .. } => StatusCode::BAD_REQUEST,
            Error::Conflict { .. } => StatusCode::CONFLICT,
            Error::GetJob(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::DeleteJob(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

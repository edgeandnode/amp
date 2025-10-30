//! Jobs get by ID handler

use axum::{
    Json,
    extract::{Path, State, rejection::PathRejection},
    http::StatusCode,
};
use worker::JobId;

use super::job_info::JobInfo;
use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
    scheduler,
};

/// Handler for the `GET /jobs/{id}` endpoint
///
/// Retrieves and returns a specific job by its ID from the metadata database.
///
/// ## Path Parameters
/// - `id`: The unique identifier of the job to retrieve (must be a valid JobId)
///
/// ## Response
/// - **200 OK**: Returns the job information as JSON
/// - **400 Bad Request**: Invalid job ID format (not parseable as JobId)
/// - **404 Not Found**: Job with the given ID does not exist
/// - **500 Internal Server Error**: Database connection or query error
///
/// ## Error Codes
/// - `INVALID_JOB_ID`: The provided ID is not a valid job identifier
/// - `JOB_NOT_FOUND`: No job exists with the given ID
/// - `GET_JOB_ERROR`: Failed to retrieve job from scheduler (database error)
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        get,
        path = "/jobs/{id}",
        tag = "jobs",
        operation_id = "jobs_get",
        params(
            ("id" = String, Path, description = "Job ID")
        ),
        responses(
            (status = 200, description = "Successfully retrieved job information", body = JobInfo),
            (status = 400, description = "Invalid job ID", body = crate::handlers::error::ErrorResponse),
            (status = 404, description = "Job not found", body = crate::handlers::error::ErrorResponse),
            (status = 500, description = "Internal server error", body = crate::handlers::error::ErrorResponse)
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    path: Result<Path<JobId>, PathRejection>,
) -> Result<Json<JobInfo>, ErrorResponse> {
    let id = match path {
        Ok(Path(path)) => path,
        Err(err) => {
            tracing::debug!(error=?err, "invalid job ID in path");
            return Err(Error::InvalidId { err }.into());
        }
    };

    match ctx.scheduler.get_job(&id).await {
        Ok(Some(job)) => Ok(Json(job.into())),
        Ok(None) => Err(Error::NotFound { id }.into()),
        Err(err) => {
            tracing::debug!(error=?err, job_id=?id, "failed to get job");
            Err(Error::GetJob(err).into())
        }
    }
}

/// Errors that can occur during job retrieval
///
/// This enum represents all possible error conditions that can occur
/// when handling a `GET /jobs/{id}` request, from path parsing
/// to database operations.
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

    /// The requested job was not found
    ///
    /// This occurs when:
    /// - No job exists with the specified ID in the database
    /// - The job was deleted after the request was made
    /// - The job ID refers to a nonexistent record
    #[error("job '{id}' not found")]
    NotFound {
        /// The job ID that was not found
        id: JobId,
    },

    /// Failed to retrieve job from scheduler
    ///
    /// This occurs when:
    /// - Database connection fails or is lost during the query
    /// - Query execution encounters an internal database error
    /// - Connection pool is exhausted or unavailable
    #[error("failed to get job")]
    GetJob(#[source] scheduler::GetJobError),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidId { .. } => "INVALID_JOB_ID",
            Error::NotFound { .. } => "JOB_NOT_FOUND",
            Error::GetJob(_) => "GET_JOB_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidId { .. } => StatusCode::BAD_REQUEST,
            Error::NotFound { .. } => StatusCode::NOT_FOUND,
            Error::GetJob(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

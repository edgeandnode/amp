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
/// - `METADATA_DB_ERROR`: Internal database error occurred
///
/// This handler:
/// - Validates and extracts the job ID from the URL path
/// - Queries the metadata database for the job with full details
/// - Returns appropriate HTTP status codes and error messages
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
            (status = 400, description = "Invalid job ID"),
            (status = 404, description = "Job not found"),
            (status = 500, description = "Internal server error")
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

    match ctx.metadata_db.get_job_by_id_with_details(&id).await {
        Ok(Some(job)) => Ok(Json(job.into())),
        Ok(None) => Err(Error::NotFound { id }.into()),
        Err(err) => {
            tracing::debug!(error=?err, job_id=?id, "failed to get job");
            Err(Error::MetadataDbError(err).into())
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
    /// This occurs when the ID cannot be parsed as a valid JobId.
    #[error("invalid job ID: {err}")]
    InvalidId {
        /// The rejection details from Axum's path extractor
        err: PathRejection,
    },

    /// The requested job was not found in the database
    ///
    /// This occurs when the job ID is valid but no job
    /// record exists with that ID in the metadata database.
    #[error("job '{id}' not found")]
    NotFound {
        /// The job ID that was not found
        id: JobId,
    },

    /// An error occurred while querying the metadata database
    ///
    /// This covers database connection issues, query failures,
    /// and other internal database errors.
    #[error("metadata db error: {0}")]
    MetadataDbError(#[from] metadata_db::Error),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidId { .. } => "INVALID_JOB_ID",
            Error::NotFound { .. } => "JOB_NOT_FOUND",
            Error::MetadataDbError(_) => "METADATA_DB_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidId { .. } => StatusCode::BAD_REQUEST,
            Error::NotFound { .. } => StatusCode::NOT_FOUND,
            Error::MetadataDbError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

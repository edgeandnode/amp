//! Jobs stop handlerLi

use axum::{
    extract::{Path, State, rejection::PathRejection},
    http::StatusCode,
};
use http_common::{BoxRequestError, RequestError};
use metadata_db::JobId;

use crate::{ctx::Ctx, scheduler::StopJobError};

/// Handler for the `PUT /jobs/{id}/stop` endpoint
///
/// This is an idempotent operation that handles job stop requests by delegating
/// to the Scheduler which atomically updates status and notifies worker.
/// The database layer handles all validation and idempotent behavior.
#[tracing::instrument(skip_all, err)]
pub async fn handler(
    State(ctx): State<Ctx>,
    path: Result<Path<JobId>, PathRejection>,
) -> Result<StatusCode, BoxRequestError> {
    let id = match path {
        Ok(Path(path)) => path,
        Err(err) => {
            tracing::debug!(error=?err, "invalid job ID in path");
            return Err(Error::InvalidId { err }.into());
        }
    };

    // Get current job status to validate it can be stopped
    let job = ctx
        .metadata_db
        .get_job_by_id_with_details(&id)
        .await
        .map_err(|err| {
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
        .stop_job(&id, &job.node_id)
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

impl RequestError for Error {
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

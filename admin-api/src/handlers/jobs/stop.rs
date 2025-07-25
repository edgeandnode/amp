//! Jobs stop handler

use axum::{
    extract::{Path, State},
    http::StatusCode,
};
use common::BoxError;
use http_common::{BoxRequestError, RequestError};
use metadata_db::{JobId, JobStatus};

use crate::ctx::Ctx;

/// Handler for the `PUT /jobs/{id}/stop` endpoint
///
/// This is an idempotent operation that handles job stop requests by:
/// 1. Fetching current job status and validating the request
/// 2. Returning OK if job is already stopping (idempotent behavior)
/// 3. Delegating to Scheduler which atomically updates status and notifies worker
#[tracing::instrument(skip_all, err)]
pub async fn handler(
    State(ctx): State<Ctx>,
    Path(id): Path<JobId>,
) -> Result<StatusCode, BoxRequestError> {
    // Get current job status to validate it can be stopped
    let job = ctx
        .metadata_db
        .get_job_with_details(&id)
        .await
        .map_err(|err| {
            tracing::debug!(error=?err, job_id=?id, "failed to get job");
            Error::MetadataDbError(err)
        })?;

    let job = match job {
        Some(job) => job,
        None => return Err(Error::NotFound { id: id.to_string() }.into()),
    };

    // Validate job can be stopped
    match job.status {
        JobStatus::Completed | JobStatus::Failed | JobStatus::Stopped => {
            return Err(Error::Conflict {
                message: format!("Job {} is already in terminal state: {:?}", id, job.status),
            }
            .into());
        }
        JobStatus::StopRequested | JobStatus::Stopping => {
            // Idempotent: job is already stopping or stop was already requested
            return Ok(StatusCode::OK);
        }
        JobStatus::Scheduled | JobStatus::Running => {
            // Valid states to stop
        }
        JobStatus::Unknown => {
            return Err(Error::Conflict {
                message: format!("Job {} has unknown status", id),
            }
            .into());
        }
    }

    // Delegate to scheduler for atomic stop operation
    ctx.scheduler
        .stop_job(&id, &job.node_id)
        .await
        .map_err(|err| {
            tracing::error!(error=?err, job_id=?id, "failed to stop job");
            Error::InvalidRequest(err)
        })?;

    Ok(StatusCode::OK)
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Job not found
    #[error("job '{id}' not found")]
    NotFound { id: String },

    /// Metadata DB error
    #[error("metadata db error: {0}")]
    MetadataDbError(#[from] metadata_db::Error),

    /// Invalid request
    #[error("invalid request: {0}")]
    InvalidRequest(BoxError),

    /// Job state conflict (cannot perform operation)
    #[error("job conflict: {message}")]
    Conflict { message: String },
}

impl RequestError for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::NotFound { .. } => "JOB_NOT_FOUND",
            Error::MetadataDbError(_) => "METADATA_DB_ERROR",
            Error::InvalidRequest(_) => "INVALID_REQUEST",
            Error::Conflict { .. } => "JOB_CONFLICT",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::NotFound { .. } => StatusCode::NOT_FOUND,
            Error::MetadataDbError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::InvalidRequest(_) => StatusCode::BAD_REQUEST,
            Error::Conflict { .. } => StatusCode::CONFLICT,
        }
    }
}

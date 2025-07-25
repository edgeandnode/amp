//! Jobs get by ID handler

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use http_common::{BoxRequestError, RequestError};
use metadata_db::JobId;

use super::get_all::JobInfo;
use crate::ctx::Ctx;

/// Handler for the `GET /jobs/{id}` endpoint
///
/// Retrieves and returns a specific job by its ID from the metadata database.
///
/// This handler:
/// - Extracts the job ID from the URL path
/// - Calls the metadata DB to get the job with full details by ID
/// - Returns the job information or a 404 if not found
#[tracing::instrument(skip_all, err)]
pub async fn handler(
    State(ctx): State<Ctx>,
    Path(id): Path<JobId>,
) -> Result<Json<JobInfo>, BoxRequestError> {
    // Fetch job with details from metadata DB
    let job = ctx
        .metadata_db
        .get_job_with_details(&id)
        .await
        .map_err(|err| {
            tracing::debug!(error=?err, job_id=?id, "failed to get job");
            Error::MetadataDbError(err)
        })?;

    match job {
        Some(job) => Ok(Json(JobInfo::from(job))),
        None => Err(Error::NotFound { id: id.to_string() }.into()),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Job not found
    #[error("job '{id}' not found")]
    NotFound { id: String },

    /// Metadata DB error
    #[error("metadata db error: {0}")]
    MetadataDbError(#[from] metadata_db::Error),
}

impl RequestError for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::NotFound { .. } => "JOB_NOT_FOUND",
            Error::MetadataDbError(_) => "METADATA_DB_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::NotFound { .. } => StatusCode::NOT_FOUND,
            Error::MetadataDbError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

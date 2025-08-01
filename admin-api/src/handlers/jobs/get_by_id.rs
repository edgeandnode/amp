//! Jobs get by ID handler

use axum::{
    Json,
    extract::{Path, State},
};
use http_common::BoxRequestError;
use metadata_db::JobId;

use super::{error::Error, get_all::JobInfo};
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

//! Jobs get all handler

use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
};
use common::BoxError;
use http_common::{BoxRequestError, RequestError};
use metadata_db::{JobId, JobWithDetails};
use serde::{Deserialize, Serialize};

use crate::ctx::Ctx;

/// Query parameters for the jobs listing endpoint
#[derive(Debug, Deserialize)]
pub struct JobsQuery {
    /// Maximum number of jobs to return (default: 50, max: 1000)
    #[serde(default = "default_limit")]
    limit: usize,

    /// ID of the last job from the previous page for pagination
    last_job_id: Option<JobId>,
}

fn default_limit() -> usize {
    50
}

/// Handler for the `GET /jobs` endpoint
///
/// Retrieves and returns a paginated list of jobs from the metadata database.
///
/// This handler:
/// - Accepts query parameters for pagination (limit, last_job_id)
/// - Validates the limit parameter (max 1000)
/// - Calls the metadata DB to list jobs with pagination
/// - Returns a structured response with jobs and next cursor
#[tracing::instrument(skip_all, err)]
pub async fn handler(
    State(ctx): State<Ctx>,
    Query(query): Query<JobsQuery>,
) -> Result<Json<JobsResponse>, BoxRequestError> {
    // Validate limit
    let limit = if query.limit > 1000 {
        return Err(Error::InvalidRequest("limit cannot be greater than 1000".into()).into());
    } else if query.limit == 0 {
        return Err(Error::InvalidRequest("limit must be greater than 0".into()).into());
    } else {
        query.limit
    };

    // Fetch jobs from metadata DB
    let jobs = ctx
        .metadata_db
        .list_jobs_with_details(limit as i64, query.last_job_id) // SAFETY: limit is capped at 1000 by validation above
        .await
        .map_err(|err| {
            tracing::debug!(error=?err, "failed to list jobs");
            Error::MetadataDbError(err)
        })?;

    // Determine next cursor (ID of the last job in this page)
    let next_cursor = jobs.last().map(|job| job.id);

    let response = JobsResponse {
        jobs: jobs.into_iter().map(JobInfo::from).collect(),
        next_cursor,
    };

    Ok(Json(response))
}

/// API response containing job information
#[derive(Debug, Serialize)]
pub struct JobsResponse {
    /// List of jobs
    pub jobs: Vec<JobInfo>,
    /// Cursor for the next page of results (None if no more results)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<JobId>,
}

/// Represents job information for the API response
#[derive(Debug, Serialize)]
pub struct JobInfo {
    /// Unique identifier for the job
    pub id: JobId,
    /// ID of the worker node this job is scheduled for
    pub node_id: String,
    /// Current status of the job
    pub status: String,
    /// Job descriptor (contains job-specific parameters)
    pub descriptor: serde_json::Value,
    /// Job creation timestamp (ISO 8601 format)
    pub created_at: String,
    /// Job last update timestamp (ISO 8601 format)  
    pub updated_at: String,
}

impl From<JobWithDetails> for JobInfo {
    fn from(job: JobWithDetails) -> Self {
        Self {
            id: job.id,
            node_id: job.node_id.to_string(),
            status: job.status.to_string(),
            descriptor: job.desc,
            created_at: job.created_at.to_rfc3339(),
            updated_at: job.updated_at.to_rfc3339(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Metadata DB error
    #[error("metadata db error: {0}")]
    MetadataDbError(#[from] metadata_db::Error),

    /// Invalid request
    #[error("invalid request: {0}")]
    InvalidRequest(BoxError),
}

impl RequestError for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::MetadataDbError(_) => "METADATA_DB_ERROR",
            Error::InvalidRequest(_) => "INVALID_REQUEST",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::MetadataDbError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::InvalidRequest(_) => StatusCode::BAD_REQUEST,
        }
    }
}

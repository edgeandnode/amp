//! Jobs get all handler

use axum::{
    Json,
    extract::{Query, State, rejection::QueryRejection},
    http::StatusCode,
};
use serde::{Deserialize, Serialize};
use worker::job::JobId;

use super::job_info::JobInfo;
use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
    scheduler,
};

/// Default number of jobs returned per page
const DEFAULT_PAGE_LIMIT: usize = 50;

/// Maximum number of jobs allowed per page
const MAX_PAGE_LIMIT: usize = 1000;

/// Query parameters for the jobs listing endpoint
#[derive(Debug, Deserialize)]
pub struct QueryParams {
    /// Maximum number of jobs to return (default: 50, max: 1000)
    #[serde(default = "default_limit")]
    limit: usize,

    /// ID of the last job from the previous page for pagination
    last_job_id: Option<JobId>,
}

fn default_limit() -> usize {
    DEFAULT_PAGE_LIMIT
}

/// Handler for the `GET /jobs` endpoint
///
/// Retrieves and returns a paginated list of jobs from the metadata database.
///
/// ## Query Parameters
/// - `limit`: Maximum number of jobs to return (default: 50, max: 1000)
/// - `last_job_id`: ID of the last job from previous page for cursor-based pagination
///
/// ## Response
/// - **200 OK**: Returns paginated job data with next cursor
/// - **400 Bad Request**: Invalid limit parameter (0, negative, or > 1000)
/// - **500 Internal Server Error**: Database connection or query error
///
/// ## Error Codes
/// - `INVALID_QUERY_PARAMETERS`: Invalid query parameters (malformed or unparseable)
/// - `LIMIT_TOO_LARGE`: Limit exceeds maximum allowed value (>1000)
/// - `LIMIT_INVALID`: Limit is zero
/// - `LIST_JOBS_ERROR`: Failed to list jobs from scheduler (database error)
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        get,
        path = "/jobs",
        tag = "jobs",
        operation_id = "jobs_list",
        params(
            ("limit" = Option<usize>, Query, description = "Maximum number of jobs to return (default: 50, max: 1000)"),
            ("last_job_id" = Option<String>, Query, description = "ID of the last job from the previous page for pagination")
        ),
        responses(
            (status = 200, description = "Successfully retrieved jobs", body = JobsResponse),
            (status = 400, description = "Invalid query parameters", body = crate::handlers::error::ErrorResponse),
            (status = 500, description = "Internal server error", body = crate::handlers::error::ErrorResponse)
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    query: Result<Query<QueryParams>, QueryRejection>,
) -> Result<Json<JobsResponse>, ErrorResponse> {
    let query = match query {
        Ok(Query(query)) => query,
        Err(err) => {
            tracing::debug!(error=?err, "invalid query parameters");
            return Err(Error::InvalidQueryParams { err }.into());
        }
    };
    // Validate limit
    let limit = if query.limit > MAX_PAGE_LIMIT {
        return Err(Error::LimitTooLarge {
            limit: query.limit,
            max: MAX_PAGE_LIMIT,
        }
        .into());
    } else if query.limit == 0 {
        return Err(Error::LimitInvalid.into());
    } else {
        query.limit
    };

    // Fetch jobs from scheduler
    let jobs = ctx
        .scheduler
        .list_jobs(
            limit as i64,
            query.last_job_id, // SAFETY: limit is capped at 1000 by validation above
        )
        .await
        .map_err(|err| {
            tracing::debug!(error=?err, "failed to list jobs");
            Error::ListJobs(err)
        })?;

    // Determine next cursor (ID of the last job in this page)
    let next_cursor = jobs.last().map(|job| job.id.into());
    let jobs = jobs
        .into_iter()
        .take(limit)
        .map(Into::into)
        .collect::<Vec<_>>();

    Ok(Json(JobsResponse { jobs, next_cursor }))
}

/// API response containing job information
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct JobsResponse {
    /// List of jobs
    pub jobs: Vec<JobInfo>,
    /// Cursor for the next page of results (None if no more results)
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "utoipa", schema(value_type = Option<i64>))]
    pub next_cursor: Option<JobId>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The query parameters are invalid or malformed
    ///
    /// This occurs when:
    /// - The limit parameter has an invalid integer format
    /// - The last_job_id parameter has an invalid format
    /// - Required query parameters are missing
    #[error("invalid query parameters: {err}")]
    InvalidQueryParams {
        /// The rejection details from Axum's query extractor
        err: QueryRejection,
    },

    /// The requested limit exceeds the maximum allowed value
    ///
    /// This occurs when:
    /// - The limit parameter is greater than 1000
    /// - Client requests more results than the server allows in one page
    /// - Pagination constraint is violated
    #[error("limit {limit} exceeds maximum allowed limit of {max}")]
    LimitTooLarge {
        /// The requested limit value
        limit: usize,
        /// The maximum allowed limit
        max: usize,
    },

    /// The requested limit is invalid (zero)
    ///
    /// This occurs when:
    /// - The limit parameter is explicitly set to 0
    /// - Client requests zero results (meaningless pagination)
    /// - Invalid pagination request is made
    #[error("limit must be greater than 0")]
    LimitInvalid,

    /// Failed to list jobs from scheduler
    ///
    /// This occurs when:
    /// - Database connection fails or is lost during pagination query
    /// - Query execution encounters an internal database error
    /// - Connection pool is exhausted or unavailable
    #[error("failed to list jobs")]
    ListJobs(#[source] scheduler::ListJobsError),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidQueryParams { .. } => "INVALID_QUERY_PARAMETERS",
            Error::LimitTooLarge { .. } => "LIMIT_TOO_LARGE",
            Error::LimitInvalid => "LIMIT_INVALID",
            Error::ListJobs(_) => "LIST_JOBS_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidQueryParams { .. } => StatusCode::BAD_REQUEST,
            Error::LimitTooLarge { .. } => StatusCode::BAD_REQUEST,
            Error::LimitInvalid => StatusCode::BAD_REQUEST,
            Error::ListJobs(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

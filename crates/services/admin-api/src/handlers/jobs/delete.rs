//! Jobs delete handler

use axum::{
    extract::{Query, State, rejection::QueryRejection},
    http::StatusCode,
};
use common::BoxError;
use metadata_db::JobStatus;

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
    scheduler,
};

/// Query parameters for the delete jobs endpoint
#[derive(Debug, serde::Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct QueryParams {
    /// Status filter for which jobs to delete
    pub status: JobStatusFilter,
}

/// Handler for the `DELETE /jobs?status=<filter>` endpoint
///
/// Deletes jobs based on status filter. Supports deleting jobs by various status criteria.
///
/// ## Query Parameters
/// - `status=terminal`: Delete all jobs in terminal states (Completed, Stopped, Failed)
/// - `status=completed`: Delete all completed jobs
/// - `status=stopped`: Delete all stopped jobs
/// - `status=error`: Delete all failed jobs
///
/// ## Response
/// - **204 No Content**: Operation completed successfully
/// - **400 Bad Request**: Invalid or missing status query parameter
/// - **500 Internal Server Error**: Database error occurred
///
/// ## Error Codes
/// - `INVALID_QUERY_PARAM`: Invalid or missing status parameter
/// - `DELETE_JOBS_BY_STATUS_ERROR`: Failed to delete jobs by status from scheduler (database error)
///
/// ## Behavior
/// This handler provides bulk job cleanup with the following characteristics:
/// - Only jobs in terminal states (Completed, Stopped, Failed) are deleted
/// - Non-terminal jobs are completely protected from deletion
/// - Database layer ensures atomic bulk deletion
/// - Safe to call even when no terminal jobs exist
///
/// ## Terminal States
/// Jobs are deleted when in these states:
/// - Completed → Safe to delete
/// - Stopped → Safe to delete
/// - Failed → Safe to delete
///
/// Protected states (never deleted):
/// - Scheduled → Job is waiting to run
/// - Running → Job is actively executing
/// - StopRequested → Job is being stopped
/// - Stopping → Job is in process of stopping
/// - Unknown → Invalid state
///
/// ## Usage
/// This endpoint is typically used for:
/// - Periodic cleanup of completed jobs
/// - Administrative maintenance
/// - Freeing up database storage
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        delete,
        path = "/jobs",
        tag = "jobs",
        operation_id = "jobs_delete_many",
        params(
            ("status" = JobStatusFilter, Query, description = "Status filter for jobs to delete")
        ),
        responses(
            (status = 204, description = "Jobs deleted successfully"),
            (status = 400, description = "Invalid query parameters", body = crate::handlers::error::ErrorResponse),
            (status = 500, description = "Internal server error", body = crate::handlers::error::ErrorResponse)
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    query: Result<Query<QueryParams>, QueryRejection>,
) -> Result<StatusCode, ErrorResponse> {
    let query = match query {
        Ok(Query(query)) => query,
        Err(err) => {
            tracing::debug!(error=?err, "invalid query parameters");
            return Err(Error::InvalidQueryParam { err }.into());
        }
    };

    let deleted_count = match query.status {
        JobStatusFilter::Terminal => ctx
            .scheduler
            .delete_jobs_by_status(JobStatus::terminal_statuses())
            .await
            .map_err(Error::DeleteJobsByStatus),
        JobStatusFilter::Completed => ctx
            .scheduler
            .delete_jobs_by_status([JobStatus::Completed])
            .await
            .map_err(Error::DeleteJobsByStatus),
        JobStatusFilter::Stopped => ctx
            .scheduler
            .delete_jobs_by_status([JobStatus::Stopped])
            .await
            .map_err(Error::DeleteJobsByStatus),
        JobStatusFilter::Error => ctx
            .scheduler
            .delete_jobs_by_status([JobStatus::Failed])
            .await
            .map_err(Error::DeleteJobsByStatus),
    }?;

    if deleted_count > 0 {
        tracing::info!(status_filter=%query.status, deleted_count, "successfully deleted jobs");
    } else {
        tracing::debug!(status_filter=%query.status, "no jobs to delete");
    }

    Ok(StatusCode::NO_CONTENT)
}

/// Status filter options for job deletion
#[derive(Debug)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "utoipa", schema(as = String))]
pub enum JobStatusFilter {
    /// Delete all jobs in terminal states (Completed, Stopped, Failed)
    Terminal,
    /// Delete all completed jobs
    Completed,
    /// Delete all stopped jobs
    Stopped,
    /// Delete all failed jobs
    Error,
}

impl std::fmt::Display for JobStatusFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobStatusFilter::Terminal => "terminal",
            JobStatusFilter::Completed => "completed",
            JobStatusFilter::Stopped => "stopped",
            JobStatusFilter::Error => "error",
        }
        .fmt(f)
    }
}

impl std::str::FromStr for JobStatusFilter {
    type Err = BoxError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            s if s.eq_ignore_ascii_case("terminal") => Ok(JobStatusFilter::Terminal),
            s if s.eq_ignore_ascii_case("completed") => Ok(JobStatusFilter::Completed),
            s if s.eq_ignore_ascii_case("stopped") => Ok(JobStatusFilter::Stopped),
            s if s.eq_ignore_ascii_case("error") => Ok(JobStatusFilter::Error),
            _ => Err(format!("invalid status filter: {}", s).into()),
        }
    }
}

impl<'de> serde::Deserialize<'de> for JobStatusFilter {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        let s: &str = serde::Deserialize::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// Errors that can occur during bulk job deletion
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid query parameters
    ///
    /// This occurs when:
    /// - The status parameter is missing from the query string
    /// - The status parameter has an invalid value (not terminal/completed/stopped/error)
    /// - Query string syntax is malformed
    #[error("invalid query parameters: {err}")]
    InvalidQueryParam {
        /// The rejection details from Axum's query extractor
        err: QueryRejection,
    },

    /// Failed to delete jobs by status from scheduler
    ///
    /// This occurs when:
    /// - Database connection fails or is lost during bulk deletion
    /// - Status-filtered delete operation encounters an internal database error
    /// - Connection pool is exhausted or unavailable
    #[error("failed to delete jobs by status")]
    DeleteJobsByStatus(#[source] scheduler::DeleteJobsByStatusError),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidQueryParam { .. } => "INVALID_QUERY_PARAM",
            Error::DeleteJobsByStatus(_) => "DELETE_JOBS_BY_STATUS_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidQueryParam { .. } => StatusCode::BAD_REQUEST,
            Error::DeleteJobsByStatus(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

//! Job events handler

use amp_worker_core::jobs::job_id::JobId;
use axum::{
    Json,
    extract::{Path, State, rejection::PathRejection},
    http::StatusCode,
};
use monitoring::logging;

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
    scheduler,
};

/// Handler for the `GET /jobs/{id}/events` endpoint
///
/// Retrieves all lifecycle events for a specific job from the event log.
///
/// ## Path Parameters
/// - `id`: The unique identifier of the job
///
/// ## Response
/// - **200 OK**: Returns all events for the job
/// - **400 Bad Request**: Invalid job ID format
/// - **404 Not Found**: Job with the given ID does not exist
/// - **500 Internal Server Error**: Database connection or query error
///
/// ## Error Codes
/// - `INVALID_JOB_ID`: The provided ID is not a valid job identifier
/// - `JOB_NOT_FOUND`: No job exists with the given ID
/// - `GET_JOB_ERROR`: Failed to retrieve job from scheduler
/// - `GET_JOB_EVENTS_ERROR`: Failed to retrieve job events from the database
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        get,
        path = "/jobs/{id}/events",
        tag = "jobs",
        operation_id = "get_job_events",
        params(
            ("id" = String, Path, description = "Job ID")
        ),
        responses(
            (status = 200, description = "Successfully retrieved job events", body = JobEventsResponse),
            (status = 400, description = "Invalid job ID", body = crate::handlers::error::ErrorResponse),
            (status = 404, description = "Job not found", body = crate::handlers::error::ErrorResponse),
            (status = 500, description = "Internal server error", body = crate::handlers::error::ErrorResponse)
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    path: Result<Path<JobId>, PathRejection>,
) -> Result<Json<JobEventsResponse>, ErrorResponse> {
    let job_id = match path {
        Ok(Path(id)) => id,
        Err(err) => {
            tracing::debug!(
                error = %err,
                error_source = logging::error_source(&err),
                "invalid job ID in path"
            );
            return Err(Error::InvalidId { source: err }.into());
        }
    };

    match ctx.scheduler.get_job(job_id).await {
        Ok(Some(_)) => {}
        Ok(None) => return Err(Error::NotFound { id: job_id }.into()),
        Err(err) => {
            tracing::debug!(
                job_id = %job_id,
                error = %err,
                error_source = logging::error_source(&err),
                "failed to get job"
            );
            return Err(Error::GetJob(err).into());
        }
    }

    let events = ctx
        .scheduler
        .list_events_for_job(job_id)
        .await
        .map_err(|err| {
            tracing::error!(
                job_id = %job_id,
                error = %err,
                error_source = logging::error_source(&err),
                "failed to get job events"
            );
            Error::GetEvents(err)
        })?;

    let response_events = events
        .into_iter()
        .map(|e| JobEventInfo {
            id: e.id,
            created_at: e.created_at.to_rfc3339(),
            node_id: e.node_id,
            event_type: e.event_type,
        })
        .collect();

    Ok(Json(JobEventsResponse {
        job_id: *job_id,
        events: response_events,
    }))
}

/// Response body for GET /jobs/{id}/events endpoint.
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct JobEventsResponse {
    /// Job ID
    pub job_id: i64,
    /// List of lifecycle events
    pub events: Vec<JobEventInfo>,
}

/// A single job lifecycle event.
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct JobEventInfo {
    /// Event ID
    pub id: i64,
    /// Event timestamp in ISO 8601 / RFC 3339 format
    pub created_at: String,
    /// ID of the worker node that recorded this event
    pub node_id: String,
    /// Event type (e.g., SCHEDULED, RUNNING, COMPLETED, ERROR, FATAL, STOPPED)
    pub event_type: String,
}

/// Errors that can occur during job events retrieval
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The job ID in the URL path is invalid
    ///
    /// This occurs when the path parameter cannot be parsed as a valid `JobId`
    /// (e.g., negative numbers or non-integer values).
    #[error("invalid job ID: {source}")]
    InvalidId {
        #[source]
        source: PathRejection,
    },

    /// The requested job was not found
    ///
    /// This occurs when the job ID is valid but no job with that ID exists
    /// in the scheduler.
    #[error("job '{id}' not found")]
    NotFound { id: JobId },

    /// Failed to retrieve job from scheduler
    ///
    /// This occurs when the scheduler query fails due to a database connection
    /// error or internal scheduler failure.
    #[error("failed to get job")]
    GetJob(#[source] scheduler::GetJobError),

    /// Failed to retrieve job events from the database
    ///
    /// This occurs when the job_events table query fails due to a database
    /// connection error or query execution failure.
    #[error("failed to get job events")]
    GetEvents(#[source] scheduler::GetEventsForJobError),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidId { .. } => "INVALID_JOB_ID",
            Error::NotFound { .. } => "JOB_NOT_FOUND",
            Error::GetJob(_) => "GET_JOB_ERROR",
            Error::GetEvents(_) => "GET_JOB_EVENTS_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidId { .. } => StatusCode::BAD_REQUEST,
            Error::NotFound { .. } => StatusCode::NOT_FOUND,
            Error::GetJob(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::GetEvents(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

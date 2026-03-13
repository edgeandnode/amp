//! Job event detail handler

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

/// Handler for the `GET /jobs/{id}/events/{event_id}` endpoint
///
/// Retrieves the full details of a single lifecycle event for a job, including
/// the optional detail payload (e.g., job descriptor for SCHEDULED events).
///
/// ## Path Parameters
/// - `id`: The unique identifier of the job
/// - `event_id`: The unique identifier of the event
///
/// ## Response
/// - **200 OK**: Returns the full event details
/// - **400 Bad Request**: Invalid job ID or event ID format
/// - **404 Not Found**: Job or event not found
/// - **500 Internal Server Error**: Database connection or query error
///
/// ## Error Codes
/// - `INVALID_JOB_ID`: The provided job ID or event ID is not valid
/// - `JOB_NOT_FOUND`: No job exists with the given ID
/// - `EVENT_NOT_FOUND`: No event exists with the given ID for the specified job
/// - `GET_JOB_ERROR`: Failed to retrieve job from scheduler
/// - `GET_JOB_EVENT_ERROR`: Failed to retrieve job event from the database
///
/// This handler:
/// - Validates and extracts the job ID and event ID from the URL path
/// - Verifies the job exists in the scheduler
/// - Queries the event by ID for the specified job
/// - Returns the full event details including the optional detail payload
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        get,
        path = "/jobs/{id}/events/{event_id}",
        tag = "jobs",
        operation_id = "get_job_event_by_id",
        params(
            ("id" = String, Path, description = "Job ID"),
            ("event_id" = String, Path, description = "Event ID")
        ),
        responses(
            (status = 200, description = "Successfully retrieved job event", body = JobEventDetailResponse),
            (status = 400, description = "Invalid job ID or event ID", body = crate::handlers::error::ErrorResponse),
            (status = 404, description = "Job or event not found", body = crate::handlers::error::ErrorResponse),
            (status = 500, description = "Internal server error", body = crate::handlers::error::ErrorResponse)
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    path: Result<Path<(JobId, i64)>, PathRejection>,
) -> Result<Json<JobEventDetailResponse>, ErrorResponse> {
    let (job_id, event_id) = match path {
        Ok(Path((job_id, event_id))) => (job_id, event_id),
        Err(err) => {
            tracing::debug!(
                error = %err,
                error_source = logging::error_source(&err),
                "invalid path parameters"
            );
            return Err(Error::InvalidPath { source: err }.into());
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

    let event = ctx
        .scheduler
        .get_event_for_job(job_id, event_id)
        .await
        .map_err(|err| {
            tracing::error!(
                job_id = %job_id,
                event_id = %event_id,
                error = %err,
                error_source = logging::error_source(&err),
                "failed to get job event"
            );
            Error::GetEvent(err)
        })?;

    match event {
        Some(event) => Ok(Json(JobEventDetailResponse {
            id: event.id,
            job_id: *event.job_id,
            created_at: event.created_at.to_rfc3339(),
            node_id: event.node_id,
            event_type: event.event_type,
            detail: event.detail.map(|d| {
                // SAFETY: EventDetailOwned guarantees valid JSON at construction time.
                // If parsing somehow fails, fall back to the raw string as a JSON string value.
                serde_json::from_str(d.as_str())
                    .unwrap_or_else(|_| serde_json::Value::String(d.as_str().to_owned()))
            }),
        })),
        None => Err(Error::EventNotFound { job_id, event_id }.into()),
    }
}

/// Response body for GET /jobs/{id}/events/{event_id} endpoint.
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct JobEventDetailResponse {
    /// Event ID
    pub id: i64,
    /// Job ID
    pub job_id: i64,
    /// Event timestamp in ISO 8601 / RFC 3339 format
    pub created_at: String,
    /// ID of the worker node that recorded this event
    pub node_id: String,
    /// Event type (e.g., SCHEDULED, RUNNING, COMPLETED, ERROR, FATAL, STOPPED)
    pub event_type: String,
    /// Optional structured detail payload
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<serde_json::Value>,
}

/// Errors that can occur during job event detail retrieval
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The path parameters are invalid
    ///
    /// This occurs when the job ID or event ID cannot be parsed from the URL path.
    #[error("invalid path parameters: {source}")]
    InvalidPath {
        #[source]
        source: PathRejection,
    },

    /// The requested job was not found
    ///
    /// This occurs when the job ID is valid but no job with that ID exists
    /// in the scheduler.
    #[error("job '{id}' not found")]
    NotFound { id: JobId },

    /// The requested event was not found for the given job
    ///
    /// This occurs when the event ID does not exist or does not belong to
    /// the specified job.
    #[error("event '{event_id}' not found for job '{job_id}'")]
    EventNotFound { job_id: JobId, event_id: i64 },

    /// Failed to retrieve job from scheduler
    ///
    /// This occurs when the scheduler query fails due to a database connection
    /// error or internal scheduler failure.
    #[error("failed to get job")]
    GetJob(#[source] scheduler::GetJobError),

    /// Failed to retrieve job event from the database
    ///
    /// This occurs when the job_events table query fails due to a database
    /// connection error or query execution failure.
    #[error("failed to get job event")]
    GetEvent(#[source] scheduler::GetEventForJobError),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidPath { .. } => "INVALID_JOB_ID",
            Error::NotFound { .. } => "JOB_NOT_FOUND",
            Error::EventNotFound { .. } => "EVENT_NOT_FOUND",
            Error::GetJob(_) => "GET_JOB_ERROR",
            Error::GetEvent(_) => "GET_JOB_EVENT_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidPath { .. } => StatusCode::BAD_REQUEST,
            Error::NotFound { .. } => StatusCode::NOT_FOUND,
            Error::EventNotFound { .. } => StatusCode::NOT_FOUND,
            Error::GetJob(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::GetEvent(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

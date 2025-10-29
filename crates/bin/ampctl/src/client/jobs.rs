//! Jobs management API client.
//!
//! Provides methods for interacting with the `/jobs` endpoints of the admin API.

use worker::JobId;

use super::{
    Client,
    error::{ApiError, ErrorResponse},
};

/// Build URL path for listing jobs.
///
/// GET `/jobs`
fn jobs_list() -> &'static str {
    "/jobs"
}

/// Build URL path for getting a job by ID.
///
/// GET `/jobs/{id}`
fn job_get_by_id(id: &JobId) -> String {
    format!("/jobs/{id}")
}

/// Build URL path for stopping a job.
///
/// PUT `/jobs/{id}/stop`
fn job_stop(id: &JobId) -> String {
    format!("/jobs/{id}/stop")
}

/// Build URL path for deleting a job by ID.
///
/// DELETE `/jobs/{id}`
fn job_delete_by_id(id: &JobId) -> String {
    format!("/jobs/{id}")
}

/// Build URL path for deleting jobs by status filter.
///
/// DELETE `/jobs`
fn jobs_delete() -> &'static str {
    "/jobs"
}

/// Client for jobs-related API operations.
///
/// Created via [`Client::jobs`](crate::client::Client::jobs).
#[derive(Debug)]
pub struct JobsClient<'a> {
    client: &'a Client,
}

impl<'a> JobsClient<'a> {
    /// Create a new jobs client.
    pub(crate) fn new(client: &'a Client) -> Self {
        Self { client }
    }

    /// Get a job by ID.
    ///
    /// GETs from `/jobs/{id}` endpoint.
    ///
    /// Returns `None` if the job does not exist (404).
    ///
    /// # Errors
    ///
    /// Returns [`GetError`] for network errors, API errors (400/500),
    /// or unexpected responses.
    #[tracing::instrument(skip(self), fields(job_id = %id))]
    pub async fn get(&self, id: &JobId) -> Result<Option<JobInfo>, GetError> {
        let url = self
            .client
            .base_url()
            .join(&job_get_by_id(id))
            .expect("valid URL");

        tracing::debug!("Sending GET request");

        let response = self
            .client
            .http()
            .get(url.as_str())
            .send()
            .await
            .map_err(|err| GetError::Network {
                url: url.to_string(),
                source: err,
            })?;

        let status = response.status();
        tracing::debug!(status = %status, "Received API response");

        match status.as_u16() {
            200 => {
                let job: JobInfo = response.json().await.map_err(|err| {
                    tracing::error!(error = %err, "Failed to parse job response");
                    GetError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: format!("Failed to parse response: {}", err),
                    }
                })?;
                Ok(Some(job))
            }
            404 => {
                tracing::debug!("Job not found");
                Ok(None)
            }
            400 | 500 => {
                let text = response.text().await.map_err(|err| {
                    tracing::error!(status = %status, error = %err, "Failed to read error response");
                    GetError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: format!("Failed to read error response: {}", err),
                    }
                })?;

                let error_response: ErrorResponse = serde_json::from_str(&text).map_err(|err| {
                    tracing::error!(status = %status, error = %err, "Failed to parse error response");
                    GetError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: text.clone(),
                    }
                })?;

                match error_response.error_code.as_str() {
                    "INVALID_JOB_ID" => Err(GetError::InvalidJobId(error_response.into())),
                    "GET_JOB_ERROR" => Err(GetError::GetJobError(error_response.into())),
                    _ => Err(GetError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: text,
                    }),
                }
            }
            _ => {
                let text = response
                    .text()
                    .await
                    .unwrap_or_else(|_| String::from("Failed to read response body"));
                Err(GetError::UnexpectedResponse {
                    status: status.as_u16(),
                    message: text,
                })
            }
        }
    }

    /// Stop a job by ID.
    ///
    /// PUTs to `/jobs/{id}/stop` endpoint.
    ///
    /// This operation is idempotent - stopping a job that's already in a terminal state returns success (200).
    ///
    /// # Errors
    ///
    /// Returns [`StopError`] for network errors, API errors (400/404/500),
    /// or unexpected responses.
    #[tracing::instrument(skip(self), fields(job_id = %id))]
    pub async fn stop(&self, id: &JobId) -> Result<(), StopError> {
        let url = self
            .client
            .base_url()
            .join(&job_stop(id))
            .expect("valid URL");

        tracing::debug!("Sending PUT request to stop job");

        let response = self
            .client
            .http()
            .put(url.as_str())
            .send()
            .await
            .map_err(|err| StopError::Network {
                url: url.to_string(),
                source: err,
            })?;

        let status = response.status();
        tracing::debug!(status = %status, "Received API response");

        match status.as_u16() {
            200 => {
                tracing::info!("Job stop request processed successfully");
                Ok(())
            }
            400 | 404 | 500 => {
                let text = response.text().await.map_err(|err| {
                    tracing::error!(status = %status, error = %err, "Failed to read error response");
                    StopError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: format!("Failed to read error response: {}", err),
                    }
                })?;

                let error_response: ErrorResponse = serde_json::from_str(&text).map_err(|err| {
                    tracing::error!(status = %status, error = %err, "Failed to parse error response");
                    StopError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: text.clone(),
                    }
                })?;

                match error_response.error_code.as_str() {
                    "INVALID_JOB_ID" => Err(StopError::InvalidJobId(error_response.into())),
                    "JOB_NOT_FOUND" => Err(StopError::NotFound(error_response.into())),
                    "STOP_JOB_ERROR" => Err(StopError::StopJobError(error_response.into())),
                    "UNEXPECTED_STATE_CONFLICT" => {
                        Err(StopError::UnexpectedStateConflict(error_response.into()))
                    }
                    _ => Err(StopError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: text,
                    }),
                }
            }
            _ => {
                let text = response
                    .text()
                    .await
                    .unwrap_or_else(|_| String::from("Failed to read response body"));
                Err(StopError::UnexpectedResponse {
                    status: status.as_u16(),
                    message: text,
                })
            }
        }
    }

    /// Delete a job by ID.
    ///
    /// DELETEs to `/jobs/{id}` endpoint.
    ///
    /// This operation is idempotent - deleting a non-existent job returns success (204).
    ///
    /// # Errors
    ///
    /// Returns [`DeleteByIdError`] for network errors, API errors (400/409/500),
    /// or unexpected responses.
    #[tracing::instrument(skip(self), fields(job_id = %id))]
    pub async fn delete_by_id(&self, id: &JobId) -> Result<(), DeleteByIdError> {
        let url = self
            .client
            .base_url()
            .join(&job_delete_by_id(id))
            .expect("valid URL");

        tracing::debug!("Sending DELETE request");

        let response = self
            .client
            .http()
            .delete(url.as_str())
            .send()
            .await
            .map_err(|err| DeleteByIdError::Network {
                url: url.to_string(),
                source: err,
            })?;

        let status = response.status();
        tracing::debug!(status = %status, "Received API response");

        match status.as_u16() {
            204 => {
                tracing::info!("Job deleted successfully");
                Ok(())
            }
            400 | 409 | 500 => {
                let text = response.text().await.map_err(|err| {
                    tracing::error!(status = %status, error = %err, "Failed to read error response");
                    DeleteByIdError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: format!("Failed to read error response: {}", err),
                    }
                })?;

                let error_response: ErrorResponse = serde_json::from_str(&text).map_err(|err| {
                    tracing::error!(status = %status, error = %err, "Failed to parse error response");
                    DeleteByIdError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: text.clone(),
                    }
                })?;

                match error_response.error_code.as_str() {
                    "INVALID_JOB_ID" => Err(DeleteByIdError::InvalidJobId(error_response.into())),
                    "JOB_CONFLICT" => Err(DeleteByIdError::Conflict(error_response.into())),
                    "GET_JOB_ERROR" => Err(DeleteByIdError::GetJobError(error_response.into())),
                    "DELETE_JOB_ERROR" => {
                        Err(DeleteByIdError::DeleteJobError(error_response.into()))
                    }
                    _ => Err(DeleteByIdError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: text,
                    }),
                }
            }
            _ => {
                let text = response
                    .text()
                    .await
                    .unwrap_or_else(|_| String::from("Failed to read response body"));
                Err(DeleteByIdError::UnexpectedResponse {
                    status: status.as_u16(),
                    message: text,
                })
            }
        }
    }

    /// Delete jobs, optionally filtered by status.
    ///
    /// DELETEs to `/jobs?status={filter}` endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`DeleteByStatusError`] for network errors, API errors (400/500),
    /// or unexpected responses.
    #[tracing::instrument(skip(self))]
    pub async fn delete(
        &self,
        status: Option<&JobStatusFilter>,
    ) -> Result<(), DeleteByStatusError> {
        let mut url = self
            .client
            .base_url()
            .join(jobs_delete())
            .expect("valid URL");

        // Add query parameters
        {
            let mut query_pairs = url.query_pairs_mut();
            if let Some(status) = status {
                query_pairs.append_pair("status", &status.to_string());
            }
        }

        tracing::debug!("Sending DELETE request");

        let response = self
            .client
            .http()
            .delete(url.as_str())
            .send()
            .await
            .map_err(|err| DeleteByStatusError::Network {
                url: url.to_string(),
                source: err,
            })?;

        let status = response.status();
        tracing::debug!(status = %status, "Received API response");

        match status.as_u16() {
            204 => {
                tracing::info!("Jobs deleted successfully");
                Ok(())
            }
            400 | 500 => {
                let text = response.text().await.map_err(|err| {
                    tracing::error!(status = %status, error = %err, "Failed to read error response");
                    DeleteByStatusError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: format!("Failed to read error response: {}", err),
                    }
                })?;

                let error_response: ErrorResponse = serde_json::from_str(&text).map_err(|err| {
                    tracing::error!(status = %status, error = %err, "Failed to parse error response");
                    DeleteByStatusError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: text.clone(),
                    }
                })?;

                match error_response.error_code.as_str() {
                    "INVALID_QUERY_PARAM" => Err(DeleteByStatusError::InvalidQueryParam(
                        error_response.into(),
                    )),
                    "DELETE_JOBS_BY_STATUS_ERROR" => Err(
                        DeleteByStatusError::DeleteJobsByStatusError(error_response.into()),
                    ),
                    _ => Err(DeleteByStatusError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: text,
                    }),
                }
            }
            _ => {
                let text = response
                    .text()
                    .await
                    .unwrap_or_else(|_| String::from("Failed to read response body"));
                Err(DeleteByStatusError::UnexpectedResponse {
                    status: status.as_u16(),
                    message: text,
                })
            }
        }
    }

    /// List jobs with pagination.
    ///
    /// GETs from `/jobs` endpoint with optional limit and last_job_id parameters.
    ///
    /// # Errors
    ///
    /// Returns [`ListError`] for network errors, API errors (400/500),
    /// or unexpected responses.
    #[tracing::instrument(skip(self))]
    pub async fn list(
        &self,
        limit: Option<usize>,
        last_job_id: Option<JobId>,
    ) -> Result<JobsResponse, ListError> {
        let mut url = self.client.base_url().join(jobs_list()).expect("valid URL");

        // Add query parameters
        {
            let mut query_pairs = url.query_pairs_mut();
            if let Some(limit) = limit {
                query_pairs.append_pair("limit", &limit.to_string());
            }
            if let Some(last_job_id) = last_job_id {
                query_pairs.append_pair("last_job_id", &last_job_id.to_string());
            }
        }

        tracing::debug!("Sending GET request to list jobs");

        let response = self
            .client
            .http()
            .get(url.as_str())
            .send()
            .await
            .map_err(|err| ListError::Network {
                url: url.to_string(),
                source: err,
            })?;

        let status = response.status();
        tracing::debug!(status = %status, "Received API response");

        match status.as_u16() {
            200 => {
                let jobs_response = response.json::<JobsResponse>().await.map_err(|err| {
                    tracing::error!(error = %err, "Failed to parse jobs response");
                    ListError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: format!("Failed to parse response: {}", err),
                    }
                })?;
                Ok(jobs_response)
            }
            400 | 500 => {
                let text = response.text().await.map_err(|err| {
                    tracing::error!(status = %status, error = %err, "Failed to read error response");
                    ListError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: format!("Failed to read error response: {}", err),
                    }
                })?;

                let error_response: ErrorResponse = serde_json::from_str(&text).map_err(|err| {
                    tracing::error!(status = %status, error = %err, "Failed to parse error response");
                    ListError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: text.clone(),
                    }
                })?;

                match error_response.error_code.as_str() {
                    "INVALID_QUERY_PARAMETERS" => {
                        Err(ListError::InvalidQueryParameters(error_response.into()))
                    }
                    "LIMIT_TOO_LARGE" => Err(ListError::LimitTooLarge(error_response.into())),
                    "LIMIT_INVALID" => Err(ListError::LimitInvalid(error_response.into())),
                    "LIST_JOBS_ERROR" => Err(ListError::ListJobsError(error_response.into())),
                    _ => Err(ListError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: text,
                    }),
                }
            }
            _ => {
                let text = response
                    .text()
                    .await
                    .unwrap_or_else(|_| String::from("Failed to read response body"));
                Err(ListError::UnexpectedResponse {
                    status: status.as_u16(),
                    message: text,
                })
            }
        }
    }
}

/// Response body for GET /jobs endpoint.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct JobsResponse {
    /// List of jobs
    pub jobs: Vec<JobInfo>,
    /// Cursor for the next page of results (None if no more results)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<JobId>,
}

/// Job information from the API.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct JobInfo {
    /// Job ID (unique identifier)
    pub id: JobId,
    /// Job creation timestamp in ISO 8601 / RFC 3339 format
    pub created_at: String,
    /// Job last update timestamp in ISO 8601 / RFC 3339 format
    pub updated_at: String,
    /// ID of the worker node this job is scheduled for
    pub node_id: String,
    /// Current status of the job (Scheduled, Running, Completed, Stopped, Failed, etc.)
    pub status: String,
    /// Job descriptor containing job-specific parameters as JSON
    pub descriptor: serde_json::Value,
}

/// Status filter for bulk job deletion.
#[derive(Debug, Clone, Copy)]
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
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            s if s.eq_ignore_ascii_case("terminal") => Ok(JobStatusFilter::Terminal),
            s if s.eq_ignore_ascii_case("completed") => Ok(JobStatusFilter::Completed),
            s if s.eq_ignore_ascii_case("stopped") => Ok(JobStatusFilter::Stopped),
            s if s.eq_ignore_ascii_case("error") => Ok(JobStatusFilter::Error),
            _ => Err(format!("invalid status filter: {}", s)),
        }
    }
}

/// Errors that can occur when listing jobs.
#[derive(Debug, thiserror::Error)]
pub enum ListError {
    /// The query parameters are invalid or malformed (400, INVALID_QUERY_PARAMETERS)
    ///
    /// This occurs when query parameters cannot be parsed, such as:
    /// - Invalid integer format for limit or last_job_id
    /// - Malformed query string syntax
    #[error("invalid query parameters")]
    InvalidQueryParameters(#[source] ApiError),

    /// The requested limit exceeds the maximum allowed value (400, LIMIT_TOO_LARGE)
    ///
    /// This occurs when the limit parameter is greater than the maximum
    /// allowed page size (1000).
    #[error("limit too large")]
    LimitTooLarge(#[source] ApiError),

    /// The requested limit is invalid (400, LIMIT_INVALID)
    ///
    /// This occurs when the limit parameter is 0, which would result
    /// in no items being returned.
    #[error("limit invalid")]
    LimitInvalid(#[source] ApiError),

    /// Failed to list jobs from scheduler (500, LIST_JOBS_ERROR)
    ///
    /// This occurs when:
    /// - Database connection fails or is lost during pagination query
    /// - Query execution encounters an internal database error
    /// - Connection pool is exhausted or unavailable
    #[error("failed to list jobs")]
    ListJobsError(#[source] ApiError),

    /// Network or connection error
    #[error("network error connecting to {url}")]
    Network { url: String, source: reqwest::Error },

    /// Unexpected response from API
    #[error("unexpected response (status {status}): {message}")]
    UnexpectedResponse { status: u16, message: String },
}

/// Errors that can occur when getting a job.
#[derive(Debug, thiserror::Error)]
pub enum GetError {
    /// The job ID in the URL path is invalid (400, INVALID_JOB_ID)
    ///
    /// This occurs when the ID cannot be parsed as a valid JobId.
    #[error("invalid job ID")]
    InvalidJobId(#[source] ApiError),

    /// Failed to retrieve job from scheduler (500, GET_JOB_ERROR)
    ///
    /// This occurs when:
    /// - Database connection fails or is lost during the query
    /// - Query execution encounters an internal database error
    /// - Connection pool is exhausted or unavailable
    #[error("failed to get job")]
    GetJobError(#[source] ApiError),

    /// Network or connection error
    #[error("network error connecting to {url}")]
    Network { url: String, source: reqwest::Error },

    /// Unexpected response from API
    #[error("unexpected response (status {status}): {message}")]
    UnexpectedResponse { status: u16, message: String },
}

/// Errors that can occur when stopping a job.
#[derive(Debug, thiserror::Error)]
pub enum StopError {
    /// The job ID in the URL path is invalid (400, INVALID_JOB_ID)
    ///
    /// This occurs when the ID cannot be parsed as a valid JobId.
    #[error("invalid job ID")]
    InvalidJobId(#[source] ApiError),

    /// Job not found (404, JOB_NOT_FOUND)
    ///
    /// This occurs when the job ID is valid but no job
    /// record exists with that ID in the metadata database.
    #[error("job not found")]
    NotFound(#[source] ApiError),

    /// Database error during stop operation (500, STOP_JOB_ERROR)
    ///
    /// This occurs when:
    /// - Database connection fails or is lost during the transaction
    /// - Transaction conflicts or deadlocks occur
    /// - Database constraint violations are encountered
    #[error("failed to stop job")]
    StopJobError(#[source] ApiError),

    /// Unexpected state conflict during stop operation (500, UNEXPECTED_STATE_CONFLICT)
    ///
    /// This indicates an internal inconsistency in the state machine.
    /// It should not occur under normal operation and indicates a bug.
    #[error("unexpected state conflict")]
    UnexpectedStateConflict(#[source] ApiError),

    /// Network or connection error
    #[error("network error connecting to {url}")]
    Network { url: String, source: reqwest::Error },

    /// Unexpected response from API
    #[error("unexpected response (status {status}): {message}")]
    UnexpectedResponse { status: u16, message: String },
}

/// Errors that can occur when deleting a job by ID.
#[derive(Debug, thiserror::Error)]
pub enum DeleteByIdError {
    /// The job ID in the URL path is invalid (400, INVALID_JOB_ID)
    ///
    /// This occurs when the ID cannot be parsed as a valid JobId.
    #[error("invalid job ID")]
    InvalidJobId(#[source] ApiError),

    /// Job exists but cannot be deleted (409, JOB_CONFLICT)
    ///
    /// This occurs when the job is not in a terminal state.
    /// Only jobs in terminal states (Completed, Stopped, Failed) can be deleted.
    #[error("job cannot be deleted from current state")]
    Conflict(#[source] ApiError),

    /// Failed to retrieve job from scheduler (500, GET_JOB_ERROR)
    ///
    /// This occurs when:
    /// - Database connection fails or is lost during the query
    /// - Query execution encounters an internal database error
    /// - Connection pool is exhausted or unavailable
    #[error("failed to get job")]
    GetJobError(#[source] ApiError),

    /// Failed to delete job from scheduler (500, DELETE_JOB_ERROR)
    ///
    /// This occurs when:
    /// - Database connection fails or is lost during deletion
    /// - Delete operation encounters an internal database error
    /// - Connection pool is exhausted or unavailable
    #[error("failed to delete job")]
    DeleteJobError(#[source] ApiError),

    /// Network or connection error
    #[error("network error connecting to {url}")]
    Network { url: String, source: reqwest::Error },

    /// Unexpected response from API
    #[error("unexpected response (status {status}): {message}")]
    UnexpectedResponse { status: u16, message: String },
}

/// Errors that can occur when deleting jobs by status filter.
#[derive(Debug, thiserror::Error)]
pub enum DeleteByStatusError {
    /// Invalid query parameters (400, INVALID_QUERY_PARAM)
    ///
    /// This occurs when:
    /// - Invalid status filter provided
    /// - Status parameter is malformed
    #[error("invalid query parameter")]
    InvalidQueryParam(#[source] ApiError),

    /// Failed to delete jobs by status from scheduler (500, DELETE_JOBS_BY_STATUS_ERROR)
    ///
    /// This occurs when:
    /// - Database connection fails or is lost during bulk deletion
    /// - Status-filtered delete operation encounters an internal database error
    /// - Connection pool is exhausted or unavailable
    #[error("failed to delete jobs by status")]
    DeleteJobsByStatusError(#[source] ApiError),

    /// Network or connection error
    #[error("network error connecting to {url}")]
    Network { url: String, source: reqwest::Error },

    /// Unexpected response from API
    #[error("unexpected response (status {status}): {message}")]
    UnexpectedResponse { status: u16, message: String },
}

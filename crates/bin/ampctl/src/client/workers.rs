//! Workers management API client.
//!
//! Provides methods for interacting with the `/workers` endpoints of the admin API.

use monitoring::logging;
use worker::node_id::NodeId;

use super::{
    Client,
    error::{ApiError, ErrorResponse},
};

/// Build URL path for listing workers.
///
/// GET `/workers`
fn workers_list() -> &'static str {
    "workers"
}

/// Build URL path for getting a worker by ID.
///
/// GET `/workers/{id}`
fn worker_get_by_id(id: &NodeId) -> String {
    format!("workers/{id}")
}

/// Client for workers-related API operations.
///
/// Created via [`Client::workers`](crate::client::Client::workers).
#[derive(Debug)]
pub struct WorkersClient<'a> {
    client: &'a Client,
}

impl<'a> WorkersClient<'a> {
    /// Create a new workers client.
    pub(crate) fn new(client: &'a Client) -> Self {
        Self { client }
    }

    /// Get a worker by node ID.
    ///
    /// GETs from `/workers/{id}` endpoint.
    ///
    /// Returns `None` if the worker does not exist (404).
    ///
    /// # Errors
    ///
    /// Returns [`GetError`] for network errors, API errors (400/500),
    /// or unexpected responses.
    #[tracing::instrument(skip(self), fields(node_id = %id))]
    pub async fn get(&self, id: &NodeId) -> Result<Option<WorkerDetailResponse>, GetError> {
        let url = self
            .client
            .base_url()
            .join(&worker_get_by_id(id))
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
                let worker: WorkerDetailResponse = response.json().await.map_err(|err| {
                    tracing::error!(error = %err, error_source = logging::error_source(&err), "Failed to parse worker response");
                    GetError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: format!("Failed to parse response: {}", err),
                    }
                })?;
                Ok(Some(worker))
            }
            404 => {
                tracing::debug!("Worker not found");
                Ok(None)
            }
            400 | 500 => {
                let text = response.text().await.map_err(|err| {
                    tracing::error!(status = %status, error = %err, error_source = logging::error_source(&err), "Failed to read error response");
                    GetError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: format!("Failed to read error response: {}", err),
                    }
                })?;

                let error_response: ErrorResponse = serde_json::from_str(&text).map_err(|err| {
                    tracing::error!(status = %status, error = %err, error_source = logging::error_source(&err), "Failed to parse error response");
                    GetError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: text.clone(),
                    }
                })?;

                match error_response.error_code.as_str() {
                    "INVALID_WORKER_ID" => Err(GetError::InvalidWorkerId(error_response.into())),
                    "WORKER_NOT_FOUND" => Err(GetError::WorkerNotFound(error_response.into())),
                    "SCHEDULER_GET_WORKER_ERROR" => {
                        Err(GetError::SchedulerGetWorker(error_response.into()))
                    }
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

    /// List all workers.
    ///
    /// GETs from `/workers` endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`ListError`] for network errors, API errors (500),
    /// or unexpected responses.
    #[tracing::instrument(skip(self))]
    pub async fn list(&self) -> Result<WorkersResponse, ListError> {
        let url = self
            .client
            .base_url()
            .join(workers_list())
            .expect("valid URL");

        tracing::debug!("Sending GET request to list workers");

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
                let workers_response = response.json::<WorkersResponse>().await.map_err(|err| {
                    tracing::error!(error = %err, error_source = logging::error_source(&err), "Failed to parse workers response");
                    ListError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: format!("Failed to parse response: {}", err),
                    }
                })?;
                Ok(workers_response)
            }
            500 => {
                let text = response.text().await.map_err(|err| {
                    tracing::error!(status = %status, error = %err, error_source = logging::error_source(&err), "Failed to read error response");
                    ListError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: format!("Failed to read error response: {}", err),
                    }
                })?;

                let error_response: ErrorResponse = serde_json::from_str(&text).map_err(|err| {
                    tracing::error!(status = %status, error = %err, error_source = logging::error_source(&err), "Failed to parse error response");
                    ListError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: text.clone(),
                    }
                })?;

                match error_response.error_code.as_str() {
                    "SCHEDULER_LIST_WORKERS_ERROR" => {
                        Err(ListError::SchedulerListWorkers(error_response.into()))
                    }
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

/// Response body for GET /workers endpoint.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct WorkersResponse {
    /// List of workers
    pub workers: Vec<WorkerInfo>,
}

/// Worker information from the API (list view).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorkerInfo {
    /// Unique identifier for the worker node
    pub node_id: String,
    /// Last heartbeat timestamp in ISO 8601 / RFC 3339 format
    pub heartbeat_at: String,
}

/// Detailed worker information from the API (single worker view).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorkerDetailResponse {
    /// Unique identifier for the worker node
    pub node_id: String,
    /// Timestamp when the worker was first created in the system (RFC3339 format)
    pub created_at: String,
    /// Timestamp when the worker last registered (RFC3339 format)
    pub registered_at: String,
    /// Last heartbeat timestamp (RFC3339 format)
    pub heartbeat_at: String,
    /// Worker metadata including version and build information
    pub info: WorkerMetadata,
}

/// Worker metadata containing build and version information.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorkerMetadata {
    /// Version string (e.g., "1.0.0" or git describe output)
    pub version: String,
    /// Full Git commit SHA (40-character hexadecimal)
    pub commit_sha: String,
    /// Timestamp when the commit was created (RFC3339 format)
    pub commit_timestamp: String,
    /// Date and time when the worker binary was built (RFC3339 format)
    pub build_date: String,
}

/// Errors that can occur when listing workers.
#[derive(Debug, thiserror::Error)]
pub enum ListError {
    /// Failed to retrieve workers list from scheduler (500, SCHEDULER_LIST_WORKERS_ERROR)
    ///
    /// This occurs when:
    /// - Database connection fails or is lost during the query
    /// - Query execution encounters an internal database error
    /// - Connection pool is exhausted or unavailable
    #[error("failed to list workers")]
    SchedulerListWorkers(#[source] ApiError),

    /// Network or connection error
    #[error("network error connecting to {url}")]
    Network { url: String, source: reqwest::Error },

    /// Unexpected response from API
    #[error("unexpected response (status {status}): {message}")]
    UnexpectedResponse { status: u16, message: String },
}

/// Errors that can occur when getting a worker.
#[derive(Debug, thiserror::Error)]
pub enum GetError {
    /// The worker ID in the URL path is invalid (400, INVALID_WORKER_ID)
    ///
    /// This occurs when the ID cannot be parsed as a valid node identifier.
    #[error("invalid worker ID")]
    InvalidWorkerId(#[source] ApiError),

    /// Worker not found (404, WORKER_NOT_FOUND)
    ///
    /// This occurs when the worker ID is valid but no worker
    /// record exists with that ID.
    #[error("worker not found")]
    WorkerNotFound(#[source] ApiError),

    /// Failed to retrieve worker from scheduler (500, SCHEDULER_GET_WORKER_ERROR)
    ///
    /// This occurs when:
    /// - Database connection fails or is lost during the query
    /// - Query execution encounters an internal database error
    /// - Connection pool is exhausted or unavailable
    #[error("failed to get worker")]
    SchedulerGetWorker(#[source] ApiError),

    /// Network or connection error
    #[error("network error connecting to {url}")]
    Network { url: String, source: reqwest::Error },

    /// Unexpected response from API
    #[error("unexpected response (status {status}): {message}")]
    UnexpectedResponse { status: u16, message: String },
}

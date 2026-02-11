//! Revisions management API client.
//!
//! Provides methods for interacting with the `/revisions` endpoints of the admin API.

use monitoring::logging;

use super::{
    Client,
    error::{ApiError, ErrorResponse},
};

/// Build URL path for activating a table revision.
///
/// POST `/revisions/{id}/activate`
fn revision_activate(id: u64) -> String {
    format!("revisions/{id}/activate")
}

/// Build URL path for deactivating table revisions.
///
/// POST `/revisions/deactivate`
fn revision_deactivate() -> &'static str {
    "revisions/deactivate"
}

/// Client for revision-related API operations.
///
/// Created via [`Client::revisions`](crate::Client::revisions).
#[derive(Debug)]
pub struct RevisionsClient<'a> {
    client: &'a Client,
}

impl<'a> RevisionsClient<'a> {
    /// Create a new revisions client.
    pub(crate) fn new(client: &'a Client) -> Self {
        Self { client }
    }

    /// Activate a table revision by location ID.
    ///
    /// POSTs to `/revisions/{id}/activate` endpoint.
    ///
    /// Resolves the dataset reference, then atomically deactivates all existing
    /// revisions for the table and marks the specified revision as active.
    ///
    /// # Errors
    ///
    /// Returns [`ActivateError`] for network errors, API errors (400/404/500),
    /// or unexpected responses.
    #[tracing::instrument(skip(self), fields(location_id = %location_id, dataset = %dataset, table_name = %table_name))]
    pub async fn activate(
        &self,
        location_id: u64,
        dataset: &str,
        table_name: &str,
    ) -> Result<(), ActivateError> {
        let url = self
            .client
            .base_url()
            .join(&revision_activate(location_id))
            .expect("valid URL");

        tracing::debug!(url = %url, "Sending POST request to activate table revision");

        let payload = ActivationPayload {
            table_name: table_name.to_owned(),
            dataset: dataset.to_owned(),
        };

        let response = self
            .client
            .http()
            .post(url.as_str())
            .json(&payload)
            .send()
            .await
            .map_err(|err| ActivateError::Network {
                url: url.to_string(),
                source: err,
            })?;

        let status = response.status();
        tracing::debug!(status = %status, "Received API response");

        match status.as_u16() {
            200 => {
                tracing::debug!("Table revision activated successfully");
                Ok(())
            }
            400 | 404 | 500 => {
                let text = response.text().await.map_err(|err| {
                    tracing::error!(status = %status, error = %err, error_source = logging::error_source(&err), "Failed to read error response");
                    ActivateError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: format!("Failed to read error response: {err}"),
                    }
                })?;

                let error_response: ErrorResponse =
                    serde_json::from_str(&text).map_err(|err| {
                        tracing::error!(status = %status, error = %err, error_source = logging::error_source(&err), "Failed to parse error response");
                        ActivateError::UnexpectedResponse {
                            status: status.as_u16(),
                            message: text.clone(),
                        }
                    })?;

                match error_response.error_code.as_str() {
                    "INVALID_PATH_PARAMETERS" => {
                        Err(ActivateError::InvalidPath(error_response.into()))
                    }
                    "DATASET_NOT_FOUND" => {
                        Err(ActivateError::DatasetNotFound(error_response.into()))
                    }
                    "TABLE_NOT_FOUND" => Err(ActivateError::TableNotFound(error_response.into())),
                    "ACTIVATE_TABLE_REVISION_ERROR" => {
                        Err(ActivateError::ActivateRevision(error_response.into()))
                    }
                    "RESOLVE_REVISION_ERROR" => {
                        Err(ActivateError::ResolveRevision(error_response.into()))
                    }
                    _ => Err(ActivateError::UnexpectedResponse {
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
                Err(ActivateError::UnexpectedResponse {
                    status: status.as_u16(),
                    message: text,
                })
            }
        }
    }

    /// Deactivate all revisions for a table.
    ///
    /// POSTs to `/revisions/deactivate` endpoint.
    ///
    /// Resolves the dataset reference, then marks all revisions for the
    /// specified table as inactive.
    ///
    /// # Errors
    ///
    /// Returns [`DeactivateError`] for network errors, API errors (404/500),
    /// or unexpected responses.
    #[tracing::instrument(skip(self), fields(dataset = %dataset, table_name = %table_name))]
    pub async fn deactivate(&self, dataset: &str, table_name: &str) -> Result<(), DeactivateError> {
        let url = self
            .client
            .base_url()
            .join(revision_deactivate())
            .expect("valid URL");

        tracing::debug!("Sending POST request to deactivate table revisions");

        let payload = DeactivationPayload {
            table_name: table_name.to_owned(),
            dataset: dataset.to_owned(),
        };

        let response = self
            .client
            .http()
            .post(url.as_str())
            .json(&payload)
            .send()
            .await
            .map_err(|err| DeactivateError::Network {
                url: url.to_string(),
                source: err,
            })?;

        let status = response.status();
        tracing::debug!(status = %status, "Received API response");

        match status.as_u16() {
            200 => {
                tracing::debug!("Table revisions deactivated successfully");
                Ok(())
            }
            404 | 500 => {
                let text = response.text().await.map_err(|err| {
                    tracing::error!(status = %status, error = %err, error_source = logging::error_source(&err), "Failed to read error response");
                    DeactivateError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: format!("Failed to read error response: {err}"),
                    }
                })?;

                let error_response: ErrorResponse =
                    serde_json::from_str(&text).map_err(|err| {
                        tracing::error!(status = %status, error = %err, error_source = logging::error_source(&err), "Failed to parse error response");
                        DeactivateError::UnexpectedResponse {
                            status: status.as_u16(),
                            message: text.clone(),
                        }
                    })?;

                match error_response.error_code.as_str() {
                    "DATASET_NOT_FOUND" => {
                        Err(DeactivateError::DatasetNotFound(error_response.into()))
                    }
                    "TABLE_NOT_FOUND" => Err(DeactivateError::TableNotFound(error_response.into())),
                    "DEACTIVATE_TABLE_REVISION_ERROR" => {
                        Err(DeactivateError::DeactivateRevision(error_response.into()))
                    }
                    "RESOLVE_REVISION_ERROR" => {
                        Err(DeactivateError::ResolveRevision(error_response.into()))
                    }
                    _ => Err(DeactivateError::UnexpectedResponse {
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
                Err(DeactivateError::UnexpectedResponse {
                    status: status.as_u16(),
                    message: text,
                })
            }
        }
    }
}

/// Request payload for activating a table revision.
#[derive(Debug, serde::Serialize)]
struct ActivationPayload {
    table_name: String,
    dataset: String,
}

/// Request payload for deactivating table revisions.
#[derive(Debug, serde::Serialize)]
struct DeactivationPayload {
    table_name: String,
    dataset: String,
}

/// Errors that can occur when activating a table revision.
#[derive(Debug, thiserror::Error)]
pub enum ActivateError {
    /// Invalid path parameters (400, INVALID_PATH_PARAMETERS)
    ///
    /// The location ID in the URL path is invalid.
    #[error("invalid path parameters")]
    InvalidPath(#[source] ApiError),

    /// Dataset or revision not found (404, DATASET_NOT_FOUND)
    ///
    /// The specified dataset or revision does not exist.
    #[error("dataset not found")]
    DatasetNotFound(#[source] ApiError),

    /// Table not found (404, TABLE_NOT_FOUND)
    ///
    /// No physical table exists for the given dataset and table name.
    #[error("table not found")]
    TableNotFound(#[source] ApiError),

    /// Failed to activate table revision (500, ACTIVATE_TABLE_REVISION_ERROR)
    ///
    /// The database transaction to activate the revision failed.
    #[error("failed to activate table revision")]
    ActivateRevision(#[source] ApiError),

    /// Failed to resolve revision (500, RESOLVE_REVISION_ERROR)
    ///
    /// Failed to resolve the dataset reference to a manifest hash.
    #[error("failed to resolve revision")]
    ResolveRevision(#[source] ApiError),

    /// Network or connection error
    #[error("network error connecting to {url}")]
    Network { url: String, source: reqwest::Error },

    /// Unexpected response from API
    #[error("unexpected response (status {status}): {message}")]
    UnexpectedResponse { status: u16, message: String },
}

/// Errors that can occur when deactivating table revisions.
#[derive(Debug, thiserror::Error)]
pub enum DeactivateError {
    /// Dataset or revision not found (404, DATASET_NOT_FOUND)
    ///
    /// The specified dataset or revision does not exist.
    #[error("dataset not found")]
    DatasetNotFound(#[source] ApiError),

    /// Table not found (404, TABLE_NOT_FOUND)
    ///
    /// No physical table exists for the given dataset and table name.
    #[error("table not found")]
    TableNotFound(#[source] ApiError),

    /// Failed to deactivate table revisions (500, DEACTIVATE_TABLE_REVISION_ERROR)
    ///
    /// The database operation to mark revisions as inactive failed.
    #[error("failed to deactivate table revisions")]
    DeactivateRevision(#[source] ApiError),

    /// Failed to resolve revision (500, RESOLVE_REVISION_ERROR)
    ///
    /// Failed to resolve the dataset reference to a manifest hash.
    #[error("failed to resolve revision")]
    ResolveRevision(#[source] ApiError),

    /// Network or connection error
    #[error("network error connecting to {url}")]
    Network { url: String, source: reqwest::Error },

    /// Unexpected response from API
    #[error("unexpected response (status {status}): {message}")]
    UnexpectedResponse { status: u16, message: String },
}

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
fn revision_activate(id: i64) -> String {
    format!("revisions/{id}/activate")
}

/// Build URL path for getting a revision by location ID.
///
/// GET `/revisions/{id}`
fn revision_get_by_id(id: i64) -> String {
    format!("revisions/{id}")
}

/// Build URL path for listing all revisions.
///
/// GET `/revisions` with optional `?active=true|false` and `?limit=x` query parameters
fn revisions_list(active: Option<bool>, limit: Option<i64>) -> String {
    let mut params = Vec::new();
    if let Some(v) = active {
        params.push(format!("active={v}"));
    }
    if let Some(v) = limit {
        params.push(format!("limit={v}"));
    }
    if params.is_empty() {
        "revisions".to_owned()
    } else {
        format!("revisions?{}", params.join("&"))
    }
}

/// Build URL path for deactivating table revisions.
///
/// POST `/revisions/deactivate`
fn revision_deactivate() -> &'static str {
    "revisions/deactivate"
}

/// Build URL path for restoring a revision.
///
/// POST `/revisions/{id}/restore`
fn revision_restore(id: i64) -> String {
    format!("revisions/{id}/restore")
}

/// Build URL path for creating a table revision.
///
/// POST `/revisions`
fn revision_create() -> &'static str {
    "revisions"
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

    /// Get a revision by location ID.
    ///
    /// Sends GET to `/revisions/{id}` endpoint.
    ///
    /// Returns `None` if the revision is not found (404).
    ///
    /// # Errors
    ///
    /// Returns [`GetByIdError`] for network errors, API errors (400/500),
    /// or unexpected responses.
    #[tracing::instrument(skip(self), fields(location_id = %location_id))]
    pub async fn get_by_id(&self, location_id: i64) -> Result<Option<RevisionInfo>, GetByIdError> {
        let url = self
            .client
            .base_url()
            .join(&revision_get_by_id(location_id))
            .expect("valid URL");

        tracing::debug!(url = %url, "Sending GET request to retrieve revision");

        let response = self
            .client
            .http()
            .get(url.as_str())
            .send()
            .await
            .map_err(|err| GetByIdError::Network {
                url: url.to_string(),
                source: err,
            })?;

        let status = response.status();
        tracing::debug!(status = %status, "Received API response");

        match status.as_u16() {
            200 => {
                let info =
                    response
                        .json()
                        .await
                        .map_err(|err| GetByIdError::UnexpectedResponse {
                            status: 200,
                            message: format!("Failed to parse response: {err}"),
                        })?;
                Ok(Some(info))
            }
            404 => {
                let text =
                    response
                        .text()
                        .await
                        .map_err(|err| GetByIdError::UnexpectedResponse {
                            status: 404,
                            message: format!("Failed to read error response: {err}"),
                        })?;

                let error_response: ErrorResponse =
                    serde_json::from_str(&text).map_err(|_| GetByIdError::UnexpectedResponse {
                        status: 404,
                        message: text.clone(),
                    })?;

                match error_response.error_code.as_str() {
                    "REVISION_NOT_FOUND" => Ok(None),
                    _ => Err(GetByIdError::UnexpectedResponse {
                        status: 404,
                        message: text,
                    }),
                }
            }
            400 | 500 => {
                let text = response.text().await.map_err(|err| {
                    tracing::error!(status = %status, error = %err, error_source = logging::error_source(&err), "Failed to read error response");
                    GetByIdError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: format!("Failed to read error response: {err}"),
                    }
                })?;

                let error_response: ErrorResponse =
                    serde_json::from_str(&text).map_err(|err| {
                        tracing::error!(status = %status, error = %err, error_source = logging::error_source(&err), "Failed to parse error response");
                        GetByIdError::UnexpectedResponse {
                            status: status.as_u16(),
                            message: text.clone(),
                        }
                    })?;

                match error_response.error_code.as_str() {
                    "INVALID_PATH_PARAMETERS" => {
                        Err(GetByIdError::InvalidPath(error_response.into()))
                    }
                    "GET_REVISION_BY_LOCATION_ID_ERROR" => {
                        Err(GetByIdError::GetRevision(error_response.into()))
                    }
                    _ => Err(GetByIdError::UnexpectedResponse {
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
                Err(GetByIdError::UnexpectedResponse {
                    status: status.as_u16(),
                    message: text,
                })
            }
        }
    }

    /// List all revisions, optionally filtered by active status.
    ///
    /// Sends GET to `/revisions` endpoint with optional `?active=true|false`
    /// and `?limit=x` query parameters.
    ///
    /// # Errors
    ///
    /// Returns [`ListError`] for network errors, API errors (400/500),
    /// or unexpected responses.
    #[tracing::instrument(skip(self))]
    pub async fn list(
        &self,
        active: Option<bool>,
        limit: Option<i64>,
    ) -> Result<Vec<RevisionInfo>, ListError> {
        let url = self
            .client
            .base_url()
            .join(&revisions_list(active, limit))
            .expect("valid URL");

        tracing::debug!(url = %url, "Sending GET request to list revisions");

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
                let revisions =
                    response
                        .json()
                        .await
                        .map_err(|err| ListError::UnexpectedResponse {
                            status: 200,
                            message: format!("Failed to parse response: {err}"),
                        })?;
                Ok(revisions)
            }
            400 | 500 => {
                let text = response.text().await.map_err(|err| {
                    tracing::error!(status = %status, error = %err, error_source = logging::error_source(&err), "Failed to read error response");
                    ListError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: format!("Failed to read error response: {err}"),
                    }
                })?;

                let error_response: ErrorResponse =
                    serde_json::from_str(&text).map_err(|err| {
                        tracing::error!(status = %status, error = %err, error_source = logging::error_source(&err), "Failed to parse error response");
                        ListError::UnexpectedResponse {
                            status: status.as_u16(),
                            message: text.clone(),
                        }
                    })?;

                match error_response.error_code.as_str() {
                    "INVALID_QUERY_PARAMETERS" => {
                        Err(ListError::InvalidQueryParams(error_response.into()))
                    }
                    "LIST_ALL_TABLE_REVISIONS_ERROR" => {
                        Err(ListError::ListAllTableRevisions(error_response.into()))
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

    /// Register a table revision from a given path.
    ///
    /// POSTs to `/revisions` endpoint.
    ///
    /// Registers an inactive, unlinked physical table revision record in the
    /// metadata database. The revision is not activated and no `physical_tables`
    /// entry is created.
    ///
    /// # Errors
    ///
    /// Returns [`RegisterError`] for network errors, API errors (404/500),
    /// or unexpected responses.
    #[tracing::instrument(skip(self), fields(dataset = %dataset, table_name = %table_name, path = %path))]
    pub async fn register(
        &self,
        dataset: &str,
        table_name: &str,
        path: &str,
    ) -> Result<RegisterResponse, RegisterError> {
        // SAFETY: `revision_create()` returns a constant relative path segment,
        // joining it to a valid base URL cannot fail.
        let url = self
            .client
            .base_url()
            .join(revision_create())
            .expect("valid URL");

        tracing::debug!(url = %url, "sending POST request to create table revision");

        let payload = RegisterPayload {
            table_name: table_name.to_owned(),
            dataset: dataset.to_owned(),
            path: path.to_owned(),
        };

        let response = self
            .client
            .http()
            .post(url.as_str())
            .json(&payload)
            .send()
            .await
            .map_err(|err| RegisterError::Network {
                url: url.to_string(),
                source: err,
            })?;

        let status = response.status();
        tracing::debug!(status = %status, "received API response");

        match status.as_u16() {
            200 => {
                let info =
                    response
                        .json()
                        .await
                        .map_err(|err| RegisterError::UnexpectedResponse {
                            status: 200,
                            message: format!("Failed to parse response: {err}"),
                        })?;
                tracing::debug!("table revision created");
                Ok(info)
            }
            404 | 500 => {
                let text = response.text().await.map_err(|err| {
                    tracing::error!(
                        status = %status,
                        error = %err,
                        error_source = logging::error_source(&err),
                        "failed to read error response"
                    );
                    RegisterError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: format!("Failed to read error response: {err}"),
                    }
                })?;

                let error_response: ErrorResponse = serde_json::from_str(&text).map_err(|err| {
                    tracing::error!(
                        status = %status,
                        error = %err,
                        error_source = logging::error_source(&err),
                        "failed to parse error response"
                    );
                    RegisterError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: text.clone(),
                    }
                })?;

                match error_response.error_code.as_str() {
                    "DATASET_NOT_FOUND" => {
                        Err(RegisterError::DatasetNotFound(error_response.into()))
                    }
                    "RESOLVE_REVISION_ERROR" => {
                        Err(RegisterError::ResolveRevision(error_response.into()))
                    }
                    "REGISTER_TABLE_REVISION_ERROR" => {
                        Err(RegisterError::RegisterTableRevision(error_response.into()))
                    }
                    _ => Err(RegisterError::UnexpectedResponse {
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
                Err(RegisterError::UnexpectedResponse {
                    status: status.as_u16(),
                    message: text,
                })
            }
        }
    }

    /// Restore a revision by re-registering its files from object storage.
    ///
    /// POSTs to `/revisions/{id}/restore` endpoint.
    ///
    /// Looks up the revision by location ID, lists all files in its object
    /// storage directory, reads Parquet metadata, and registers each file in
    /// the metadata database.
    ///
    /// # Errors
    ///
    /// Returns [`RestoreError`] for network errors, API errors (400/404/500),
    /// or unexpected responses.
    #[tracing::instrument(skip(self), fields(location_id = %location_id))]
    pub async fn restore(&self, location_id: i64) -> Result<RestoreResponse, RestoreError> {
        // SAFETY: `revision_restore()` returns a well-formed relative path segment,
        // joining it to a valid base URL cannot fail.
        let url = self
            .client
            .base_url()
            .join(&revision_restore(location_id))
            .expect("valid URL");

        tracing::debug!(url = %url, "sending POST request to restore revision");

        let response = self
            .client
            .http()
            .post(url.as_str())
            .send()
            .await
            .map_err(|err| RestoreError::Network {
                url: url.to_string(),
                source: err,
            })?;

        let status = response.status();
        tracing::debug!(status = %status, "received API response");

        match status.as_u16() {
            200 => {
                let restore_response =
                    response
                        .json()
                        .await
                        .map_err(|err| RestoreError::UnexpectedResponse {
                            status: 200,
                            message: format!("failed to parse response: {err}"),
                        })?;
                tracing::debug!("revision restored");
                Ok(restore_response)
            }
            400 | 404 | 500 => {
                let text = response.text().await.map_err(|err| {
                    tracing::error!(
                        status = %status,
                        error = %err,
                        error_source = logging::error_source(&err),
                        "failed to read error response"
                    );
                    RestoreError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: format!("Failed to read error response: {err}"),
                    }
                })?;

                let error_response: ErrorResponse = serde_json::from_str(&text).map_err(|err| {
                    tracing::error!(
                        status = %status,
                        error = %err,
                        error_source = logging::error_source(&err),
                        "failed to parse error response"
                    );
                    RestoreError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: text.clone(),
                    }
                })?;

                match error_response.error_code.as_str() {
                    "INVALID_PATH_PARAMETERS" => {
                        Err(RestoreError::InvalidPath(error_response.into()))
                    }
                    "REVISION_NOT_FOUND" => {
                        Err(RestoreError::RevisionNotFound(error_response.into()))
                    }
                    "GET_REVISION_BY_LOCATION_ID_ERROR" => {
                        Err(RestoreError::GetRevision(error_response.into()))
                    }
                    "REGISTER_FILES_ERROR" => {
                        Err(RestoreError::RegisterFiles(error_response.into()))
                    }
                    _ => Err(RestoreError::UnexpectedResponse {
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
                Err(RestoreError::UnexpectedResponse {
                    status: status.as_u16(),
                    message: text,
                })
            }
        }
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
        location_id: i64,
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
                    "TABLE_NOT_IN_MANIFEST" => {
                        Err(ActivateError::TableNotInManifest(error_response.into()))
                    }
                    "TABLE_NOT_REGISTERED" => {
                        Err(ActivateError::TableNotRegistered(error_response.into()))
                    }
                    "GET_DATASET_ERROR" => Err(ActivateError::GetDataset(error_response.into())),
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

/// Request payload for registering a table revision.
#[derive(Debug, serde::Serialize)]
struct RegisterPayload {
    table_name: String,
    dataset: String,
    path: String,
}

/// Response from registering a table revision.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct RegisterResponse {
    /// Location ID assigned to the new revision.
    pub location_id: i64,
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

    /// Table not found in manifest (404, TABLE_NOT_IN_MANIFEST)
    ///
    /// The table name does not exist in the dataset manifest.
    #[error("table not found in manifest")]
    TableNotInManifest(#[source] ApiError),

    /// Table not registered in data store (404, TABLE_NOT_REGISTERED)
    ///
    /// No physical table is registered for the given dataset and table name.
    #[error("table not registered")]
    TableNotRegistered(#[source] ApiError),

    /// Failed to load dataset (500, GET_DATASET_ERROR)
    ///
    /// Failed to load the dataset from its manifest.
    #[error("failed to load dataset")]
    GetDataset(#[source] ApiError),

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

/// Revision information returned by the API.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RevisionInfo {
    /// Unique identifier for this revision (location ID).
    pub id: i64,
    /// Relative path to the storage location.
    pub path: String,
    /// Whether this revision is currently active.
    pub active: bool,
    /// Writer job ID responsible for populating this revision, if one exists.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub writer: Option<i64>,
    /// Metadata about the revision.
    pub metadata: RevisionMetadataInfo,
}

/// Revision metadata returned by the API.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RevisionMetadataInfo {
    /// Dataset namespace.
    pub dataset_namespace: String,
    /// Dataset name.
    pub dataset_name: String,
    /// Manifest hash.
    pub manifest_hash: String,
    /// Table name.
    pub table_name: String,
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

/// Errors that can occur when listing revisions.
#[derive(Debug, thiserror::Error)]
pub enum ListError {
    /// Invalid query parameters (400, INVALID_QUERY_PARAMETERS)
    ///
    /// The query string could not be parsed.
    #[error("invalid query parameters")]
    InvalidQueryParams(#[source] ApiError),

    /// Failed to list table revisions (500, LIST_ALL_TABLE_REVISIONS_ERROR)
    ///
    /// An error occurred in the data store while listing table revisions.
    #[error("failed to list table revisions")]
    ListAllTableRevisions(#[source] ApiError),

    /// Network or connection error
    #[error("network error connecting to {url}")]
    Network { url: String, source: reqwest::Error },

    /// Unexpected response from API
    #[error("unexpected response (status {status}): {message}")]
    UnexpectedResponse { status: u16, message: String },
}

/// Errors that can occur when getting a revision by location ID.
#[derive(Debug, thiserror::Error)]
pub enum GetByIdError {
    /// Invalid path parameters (400, INVALID_PATH_PARAMETERS)
    ///
    /// The location ID in the URL path is invalid.
    #[error("invalid path parameters")]
    InvalidPath(#[source] ApiError),

    /// Failed to get revision (500, GET_REVISION_BY_LOCATION_ID_ERROR)
    ///
    /// The database query to get the revision failed.
    #[error("failed to get revision")]
    GetRevision(#[source] ApiError),

    /// Network or connection error
    #[error("network error connecting to {url}")]
    Network { url: String, source: reqwest::Error },

    /// Unexpected response from API
    #[error("unexpected response (status {status}): {message}")]
    UnexpectedResponse { status: u16, message: String },
}

/// Errors that can occur when registering a table revision.
#[derive(Debug, thiserror::Error)]
pub enum RegisterError {
    /// Dataset or revision not found (404, DATASET_NOT_FOUND)
    ///
    /// The specified dataset or revision does not exist.
    #[error("dataset not found")]
    DatasetNotFound(#[source] ApiError),

    /// Failed to resolve revision (500, RESOLVE_REVISION_ERROR)
    ///
    /// Failed to resolve the dataset reference to a manifest hash.
    #[error("failed to resolve revision")]
    ResolveRevision(#[source] ApiError),

    /// Failed to register table revision (500, REGISTER_TABLE_REVISION_ERROR)
    ///
    /// The database operation to register the table revision failed.
    #[error("failed to register table revision")]
    RegisterTableRevision(#[source] ApiError),

    /// Network or connection error
    #[error("network error connecting to {url}")]
    Network { url: String, source: reqwest::Error },

    /// Unexpected response from API
    #[error("unexpected response (status {status}): {message}")]
    UnexpectedResponse { status: u16, message: String },
}

/// Response from a successful revision restore operation.
#[derive(Debug, serde::Deserialize)]
pub struct RestoreResponse {
    /// Total number of files restored
    pub total_files: i32,
}

/// Errors that can occur when restoring a revision.
#[derive(Debug, thiserror::Error)]
pub enum RestoreError {
    /// Invalid path parameters (400, INVALID_PATH_PARAMETERS)
    ///
    /// The location ID in the URL path is invalid.
    #[error("invalid path parameters")]
    InvalidPath(#[source] ApiError),

    /// Revision not found (404, REVISION_NOT_FOUND)
    ///
    /// No revision exists with the specified location ID.
    #[error("revision not found")]
    RevisionNotFound(#[source] ApiError),

    /// Failed to get revision (500, GET_REVISION_BY_LOCATION_ID_ERROR)
    ///
    /// The database query to get the revision failed.
    #[error("failed to get revision")]
    GetRevision(#[source] ApiError),

    /// Failed to register files (500, REGISTER_FILES_ERROR)
    ///
    /// Re-registering the revision's files from object storage failed.
    #[error("failed to register revision files")]
    RegisterFiles(#[source] ApiError),

    /// Network or connection error
    #[error("network error connecting to {url}")]
    Network { url: String, source: reqwest::Error },

    /// Unexpected response from API
    #[error("unexpected response (status {status}): {message}")]
    UnexpectedResponse { status: u16, message: String },
}

//! Manifest content-addressable storage API client.
//!
//! Provides methods for interacting with the `/manifests` endpoints of the admin API.

use datasets_common::hash::Hash;
use monitoring::logging;

use super::{
    Client,
    error::{ApiError, ErrorResponse},
};

/// Build URL path for listing all manifests.
///
/// GET `/manifests`
fn manifests_list() -> &'static str {
    "manifests"
}

/// Build URL path for registering a manifest.
///
/// POST `/manifests`
fn manifest_register() -> &'static str {
    "manifests"
}

/// Build URL path for getting a manifest by hash.
///
/// GET `/manifests/{hash}`
fn manifest_get_by_id(hash: &Hash) -> String {
    format!("manifests/{hash}")
}

/// Build URL path for deleting a manifest by hash.
///
/// DELETE `/manifests/{hash}`
fn manifest_delete_by_id(hash: &Hash) -> String {
    format!("manifests/{hash}")
}

/// Build URL path for pruning orphaned manifests.
///
/// DELETE `/manifests`
fn manifest_prune() -> &'static str {
    "manifests"
}

/// Client for manifest-related API operations.
///
/// Created via [`Client::manifests`](crate::client::Client::manifests).
#[derive(Debug)]
pub struct ManifestsClient<'a> {
    client: &'a Client,
}

impl<'a> ManifestsClient<'a> {
    /// Create a new manifests client.
    pub(crate) fn new(client: &'a Client) -> Self {
        Self { client }
    }

    /// Register a manifest with content-addressable storage.
    ///
    /// POSTs to `/manifests` endpoint with the manifest JSON content.
    ///
    /// # Errors
    ///
    /// Returns [`RegisterError`] for network errors, API errors (400/500),
    /// or unexpected responses.
    #[tracing::instrument(skip(self, manifest))]
    pub async fn register(&self, manifest: &str) -> Result<Hash, RegisterError> {
        let url = self
            .client
            .base_url()
            .join(manifest_register())
            .expect("valid URL");

        tracing::debug!("Sending manifest registration request");

        // Parse manifest JSON to send as JSON value
        let manifest_json: serde_json::Value =
            serde_json::from_str(manifest).map_err(RegisterError::InvalidJson)?;

        let response = self
            .client
            .http()
            .post(url.as_str())
            .json(&manifest_json)
            .send()
            .await
            .map_err(|err| RegisterError::Network {
                url: url.to_string(),
                source: err,
            })?;

        let status = response.status();
        tracing::debug!(status = %status, "Received API response");

        match status.as_u16() {
            201 => {
                let register_response =
                    response
                        .json::<RegisterManifestResponse>()
                        .await
                        .map_err(|err| {
                            tracing::error!(error = %err, error_source = logging::error_source(&err), "Failed to parse success response");
                            RegisterError::UnexpectedResponse {
                                status: status.as_u16(),
                                message: format!("Failed to parse response: {}", err),
                            }
                        })?;
                Ok(register_response.hash)
            }
            400 | 500 => {
                let text = response.text().await.map_err(|err| {
                    tracing::error!(status = %status, error = %err, error_source = logging::error_source(&err), "Failed to read error response");
                    RegisterError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: format!("Failed to read error response: {}", err),
                    }
                })?;

                let error_response: ErrorResponse = serde_json::from_str(&text).map_err(|err| {
                    tracing::error!(status = %status, error = %err, error_source = logging::error_source(&err), "Failed to parse error response");
                    RegisterError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: text.clone(),
                    }
                })?;

                match error_response.error_code.as_str() {
                    "INVALID_PAYLOAD_FORMAT" => {
                        Err(RegisterError::InvalidPayloadFormat(error_response.into()))
                    }
                    "INVALID_MANIFEST" => {
                        Err(RegisterError::InvalidManifest(error_response.into()))
                    }
                    "DEPENDENCY_VALIDATION_ERROR" => Err(RegisterError::DependencyValidationError(
                        error_response.into(),
                    )),
                    "UNSUPPORTED_DATASET_KIND" => {
                        Err(RegisterError::UnsupportedDatasetKind(error_response.into()))
                    }
                    "MANIFEST_STORAGE_ERROR" => {
                        Err(RegisterError::ManifestStorageError(error_response.into()))
                    }
                    "MANIFEST_REGISTRATION_ERROR" => Err(RegisterError::ManifestRegistrationError(
                        error_response.into(),
                    )),
                    "MANIFEST_TRANSACTION_COMMIT_ERROR" => {
                        Err(RegisterError::TransactionCommitError(error_response.into()))
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

    /// Get a manifest by its content-addressable hash.
    ///
    /// GETs from `/manifests/{hash}` endpoint.
    ///
    /// Returns `None` if the manifest does not exist (404).
    ///
    /// # Errors
    ///
    /// Returns [`GetError`] for network errors, API errors (400/500),
    /// or unexpected responses.
    #[tracing::instrument(skip(self), fields(hash = %hash))]
    pub async fn get(&self, hash: &Hash) -> Result<Option<serde_json::Value>, GetError> {
        let url = self
            .client
            .base_url()
            .join(&manifest_get_by_id(hash))
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
                let manifest_response =
                    response.json::<ManifestResponse>().await.map_err(|err| {
                        tracing::error!(error = %err, error_source = logging::error_source(&err), "Failed to parse manifest response");
                        GetError::UnexpectedResponse {
                            status: status.as_u16(),
                            message: format!("Failed to parse response: {}", err),
                        }
                    })?;

                Ok(Some(manifest_response.manifest))
            }
            404 => {
                tracing::debug!("Manifest not found");
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
                    "INVALID_HASH" => Err(GetError::InvalidHash(error_response.into())),
                    "MANIFEST_METADATA_DB_ERROR" => {
                        Err(GetError::MetadataDbError(error_response.into()))
                    }
                    "MANIFEST_OBJECT_STORE_ERROR" => {
                        Err(GetError::ObjectStoreError(error_response.into()))
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

    /// Delete a manifest by its content-addressable hash.
    ///
    /// DELETEs to `/manifests/{hash}` endpoint.
    ///
    /// **Note**: Manifests linked to datasets cannot be deleted (returns 409 Conflict).
    ///
    /// # Errors
    ///
    /// Returns [`DeleteError`] for network errors, API errors (400/409/500),
    /// or unexpected responses.
    #[tracing::instrument(skip(self), fields(hash = %hash))]
    pub async fn delete(&self, hash: &Hash) -> Result<(), DeleteError> {
        let url = self
            .client
            .base_url()
            .join(&manifest_delete_by_id(hash))
            .expect("valid URL");

        tracing::debug!("Sending DELETE request");

        let response = self
            .client
            .http()
            .delete(url.as_str())
            .send()
            .await
            .map_err(|err| DeleteError::Network {
                url: url.to_string(),
                source: err,
            })?;

        let status = response.status();
        tracing::debug!(status = %status, "Received API response");

        match status.as_u16() {
            204 => {
                tracing::info!("Manifest deleted successfully");
                Ok(())
            }
            400 | 409 | 500 => {
                let text = response.text().await.map_err(|err| {
                    tracing::error!(status = %status, error = %err, error_source = logging::error_source(&err), "Failed to read error response");
                    DeleteError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: format!("Failed to read error response: {}", err),
                    }
                })?;

                let error_response: ErrorResponse = serde_json::from_str(&text).map_err(|err| {
                    tracing::error!(status = %status, error = %err, error_source = logging::error_source(&err), "Failed to parse error response");
                    DeleteError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: text.clone(),
                    }
                })?;

                match error_response.error_code.as_str() {
                    "INVALID_HASH" => Err(DeleteError::InvalidHash(error_response.into())),
                    "MANIFEST_LINKED" => Err(DeleteError::ManifestLinked(error_response.into())),
                    "MANIFEST_DELETE_TRANSACTION_BEGIN_ERROR" => {
                        Err(DeleteError::TransactionBeginError(error_response.into()))
                    }
                    "MANIFEST_DELETE_CHECK_LINKS_ERROR" => {
                        Err(DeleteError::CheckLinksError(error_response.into()))
                    }
                    "MANIFEST_DELETE_METADATA_DB_ERROR" => {
                        Err(DeleteError::MetadataDbDeleteError(error_response.into()))
                    }
                    "MANIFEST_DELETE_OBJECT_STORE_ERROR" => {
                        Err(DeleteError::ObjectStoreDeleteError(error_response.into()))
                    }
                    "MANIFEST_DELETE_TRANSACTION_COMMIT_ERROR" => {
                        Err(DeleteError::TransactionCommitError(error_response.into()))
                    }
                    _ => Err(DeleteError::UnexpectedResponse {
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
                Err(DeleteError::UnexpectedResponse {
                    status: status.as_u16(),
                    message: text,
                })
            }
        }
    }

    /// List all registered manifests.
    ///
    /// GETs from `/manifests` endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`ListAllError`] for network errors, API errors (500),
    /// or unexpected responses.
    #[tracing::instrument(skip(self))]
    pub async fn list_all(&self) -> Result<ManifestsResponse, ListAllError> {
        let url = self
            .client
            .base_url()
            .join(manifests_list())
            .expect("valid URL");

        tracing::debug!("Sending list all manifests request");

        let response = self
            .client
            .http()
            .get(url.as_str())
            .send()
            .await
            .map_err(|err| ListAllError::Network {
                url: url.to_string(),
                source: err,
            })?;

        let status = response.status();
        tracing::debug!(status = %status, "Received API response");

        match status.as_u16() {
            200 => {
                let manifests_response =
                    response.json::<ManifestsResponse>().await.map_err(|err| {
                        tracing::error!(error = %err, error_source = logging::error_source(&err), "Failed to parse manifests response");
                        ListAllError::UnexpectedResponse {
                            status: status.as_u16(),
                            message: format!("Failed to parse response: {}", err),
                        }
                    })?;
                Ok(manifests_response)
            }
            500 => {
                let text = response.text().await.map_err(|err| {
                    tracing::error!(status = %status, error = %err, error_source = logging::error_source(&err), "Failed to read error response");
                    ListAllError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: format!("Failed to read error response: {}", err),
                    }
                })?;

                let error_response: ErrorResponse = serde_json::from_str(&text).map_err(|err| {
                    tracing::error!(status = %status, error = %err, error_source = logging::error_source(&err), "Failed to parse error response");
                    ListAllError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: text.clone(),
                    }
                })?;

                match error_response.error_code.as_str() {
                    "LIST_ALL_MANIFESTS_ERROR" => {
                        Err(ListAllError::ListAllManifestsError(error_response.into()))
                    }
                    _ => Err(ListAllError::UnexpectedResponse {
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
                Err(ListAllError::UnexpectedResponse {
                    status: status.as_u16(),
                    message: text,
                })
            }
        }
    }

    /// Prune all orphaned manifests (not linked to any datasets).
    ///
    /// DELETEs to `/manifests` endpoint.
    ///
    /// Returns count of deleted manifests.
    ///
    /// # Errors
    ///
    /// Returns [`PruneError`] for network errors, API errors (500),
    /// or unexpected responses.
    #[tracing::instrument(skip(self))]
    pub async fn prune(&self) -> Result<PruneResponse, PruneError> {
        let url = self
            .client
            .base_url()
            .join(manifest_prune())
            .expect("valid URL");

        tracing::debug!("Sending prune request");

        let response = self
            .client
            .http()
            .delete(url.as_str())
            .send()
            .await
            .map_err(|err| PruneError::Network {
                url: url.to_string(),
                source: err,
            })?;

        let status = response.status();
        tracing::debug!(status = %status, "Received API response");

        match status.as_u16() {
            200 => {
                let prune_response = response.json::<PruneResponse>().await.map_err(|err| {
                    tracing::error!(error = %err, error_source = logging::error_source(&err), "Failed to parse prune response");
                    PruneError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: format!("Failed to parse response: {}", err),
                    }
                })?;
                Ok(prune_response)
            }
            500 => {
                let text = response.text().await.map_err(|err| {
                    tracing::error!(status = %status, error = %err, error_source = logging::error_source(&err), "Failed to read error response");
                    PruneError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: format!("Failed to read error response: {}", err),
                    }
                })?;

                let error_response: ErrorResponse = serde_json::from_str(&text).map_err(|err| {
                    tracing::error!(status = %status, error = %err, error_source = logging::error_source(&err), "Failed to parse error response");
                    PruneError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: text.clone(),
                    }
                })?;

                match error_response.error_code.as_str() {
                    "LIST_ORPHANED_MANIFESTS_ERROR" => Err(PruneError::ListOrphanedManifestsError(
                        error_response.into(),
                    )),
                    _ => Err(PruneError::UnexpectedResponse {
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
                Err(PruneError::UnexpectedResponse {
                    status: status.as_u16(),
                    message: text,
                })
            }
        }
    }
}

/// Response body for POST /manifests endpoint (201 success).
#[derive(Debug, serde::Deserialize)]
struct RegisterManifestResponse {
    hash: Hash,
}

/// Response body for GET /manifests/{hash} endpoint (200 success).
#[derive(Debug, serde::Deserialize)]
struct ManifestResponse {
    #[serde(flatten)]
    manifest: serde_json::Value,
}

/// Errors that can occur when registering a manifest.
#[derive(Debug, thiserror::Error)]
pub enum RegisterError {
    /// Invalid manifest JSON
    #[error("invalid manifest JSON")]
    InvalidJson(#[source] serde_json::Error),

    /// Invalid request format (400, INVALID_PAYLOAD_FORMAT)
    ///
    /// This occurs when:
    /// - Request JSON is malformed or invalid
    /// - Request body cannot be parsed as valid JSON
    /// - Required fields are missing or have wrong types
    #[error("invalid payload format")]
    InvalidPayloadFormat(#[source] ApiError),

    /// Invalid manifest content or structure (400, INVALID_MANIFEST)
    ///
    /// This occurs when:
    /// - Manifest JSON is malformed or invalid
    /// - Manifest structure doesn't match expected schema for the given kind
    /// - Required manifest fields (name, kind, version, etc.) are missing or invalid
    /// - JSON serialization/deserialization fails during canonicalization
    #[error("invalid manifest")]
    InvalidManifest(#[source] ApiError),

    /// Dependency validation error for derived datasets (400, DEPENDENCY_VALIDATION_ERROR)
    ///
    /// This occurs when:
    /// - SQL queries in derived datasets are syntactically invalid
    /// - SQL queries reference datasets not declared in the dependencies section
    /// - Dependency resolution fails during validation
    #[error("dependency validation error")]
    DependencyValidationError(#[source] ApiError),

    /// Unsupported dataset kind (400, UNSUPPORTED_DATASET_KIND)
    ///
    /// This occurs when:
    /// - Dataset kind is not one of the supported types (manifest, evm-rpc, firehose, eth-beacon)
    /// - The 'kind' field in the manifest contains an unrecognized value
    #[error("unsupported dataset kind")]
    UnsupportedDatasetKind(#[source] ApiError),

    /// Failed to write manifest to object store (500, MANIFEST_STORAGE_ERROR)
    ///
    /// This occurs when:
    /// - Object store is not accessible or connection fails
    /// - Write permissions are insufficient
    /// - Storage quota is exceeded
    /// - Network errors prevent writing to remote storage
    #[error("manifest storage error")]
    ManifestStorageError(#[source] ApiError),

    /// Failed to register manifest in metadata database (500, MANIFEST_REGISTRATION_ERROR)
    ///
    /// This occurs when:
    /// - Database connection is lost
    /// - SQL insertion or update query fails
    /// - Database constraints are violated
    /// - Schema inconsistencies prevent registration
    #[error("manifest registration error")]
    ManifestRegistrationError(#[source] ApiError),

    /// Failed to commit transaction after successful database operations (500, MANIFEST_TRANSACTION_COMMIT_ERROR)
    ///
    /// This occurs when:
    /// - Database transaction commit fails after all operations succeed
    /// - Connection is lost during commit
    /// - Database lock conflicts prevent commit
    /// - Transaction timeout occurs
    #[error("manifest transaction commit error")]
    TransactionCommitError(#[source] ApiError),

    /// Network or connection error
    #[error("network error connecting to {url}")]
    Network { url: String, source: reqwest::Error },

    /// Unexpected response from API
    #[error("unexpected response (status {status}): {message}")]
    UnexpectedResponse { status: u16, message: String },
}

/// Errors that can occur when getting a manifest.
#[derive(Debug, thiserror::Error)]
pub enum GetError {
    /// The manifest hash is invalid (400, INVALID_HASH)
    ///
    /// This occurs when:
    /// - The hash contains invalid characters or doesn't follow hash format conventions
    /// - The hash is empty or malformed
    /// - Path parameter extraction fails for the hash (deserialization error)
    #[error("invalid hash")]
    InvalidHash(#[source] ApiError),

    /// Failed to query manifest path from metadata database (500, MANIFEST_METADATA_DB_ERROR)
    ///
    /// This occurs when:
    /// - Database query to resolve hash to file path fails
    /// - Database connection is lost
    /// - SQL execution errors occur during path lookup
    #[error("manifest metadata DB error")]
    MetadataDbError(#[source] ApiError),

    /// Failed to retrieve manifest from object store (500, MANIFEST_OBJECT_STORE_ERROR)
    ///
    /// This occurs when:
    /// - Object store is not accessible or connection fails
    /// - Read permissions are insufficient
    /// - Network errors prevent reading from remote storage
    /// - File path exists in metadata DB but not in object store
    #[error("manifest object store error")]
    ObjectStoreError(#[source] ApiError),

    /// Network or connection error
    #[error("network error connecting to {url}")]
    Network { url: String, source: reqwest::Error },

    /// Unexpected response from API
    #[error("unexpected response (status {status}): {message}")]
    UnexpectedResponse { status: u16, message: String },
}

/// Errors that can occur when deleting a manifest.
#[derive(Debug, thiserror::Error)]
pub enum DeleteError {
    /// The manifest hash in the URL path is invalid (400, INVALID_HASH)
    ///
    /// This occurs when:
    /// - The hash contains invalid characters or doesn't follow hash format conventions
    /// - The hash is empty or malformed
    /// - Path parameter extraction fails for the hash (deserialization error)
    #[error("invalid hash")]
    InvalidHash(#[source] ApiError),

    /// The manifest is linked to one or more datasets (409, MANIFEST_LINKED)
    ///
    /// This occurs when:
    /// - The manifest is currently referenced by one or more dataset versions
    /// - Attempting to delete a manifest that has active dataset dependencies
    /// - Manifests must be unlinked from all datasets before deletion is allowed
    #[error("manifest is linked")]
    ManifestLinked(#[source] ApiError),

    /// Failed to begin transaction (500, MANIFEST_DELETE_TRANSACTION_BEGIN_ERROR)
    ///
    /// This occurs when:
    /// - Database connection is not available or lost
    /// - Transaction isolation level cannot be established
    /// - Database resource limits prevent new transactions
    #[error("transaction begin error")]
    TransactionBeginError(#[source] ApiError),

    /// Failed to check if manifest is linked to datasets (500, MANIFEST_DELETE_CHECK_LINKS_ERROR)
    ///
    /// This occurs when:
    /// - Database query to check manifest references fails
    /// - SQL execution errors during link validation
    /// - Database connection is lost during the check
    #[error("check links error")]
    CheckLinksError(#[source] ApiError),

    /// Failed to delete manifest from metadata database (500, MANIFEST_DELETE_METADATA_DB_ERROR)
    ///
    /// This occurs when:
    /// - Database delete operation fails
    /// - Database constraints prevent deletion
    /// - Connection is lost during the delete operation
    #[error("metadata DB delete error")]
    MetadataDbDeleteError(#[source] ApiError),

    /// Failed to delete manifest from object store (500, MANIFEST_DELETE_OBJECT_STORE_ERROR)
    ///
    /// This occurs when:
    /// - Object store is not accessible or connection fails
    /// - Delete permissions are insufficient
    /// - Network errors prevent deletion from remote storage
    /// - File does not exist in object store (non-critical if DB delete succeeded)
    #[error("object store delete error")]
    ObjectStoreDeleteError(#[source] ApiError),

    /// Failed to commit transaction (500, MANIFEST_DELETE_TRANSACTION_COMMIT_ERROR)
    ///
    /// This occurs when:
    /// - Database transaction commit fails after delete operations
    /// - Connection is lost during commit
    /// - Database lock conflicts prevent commit
    /// - Transaction timeout occurs
    #[error("transaction commit error")]
    TransactionCommitError(#[source] ApiError),

    /// Network or connection error
    #[error("network error connecting to {url}")]
    Network {
        url: String,
        #[source]
        source: reqwest::Error,
    },

    /// Unexpected response from API
    #[error("unexpected response (status {status}): {message}")]
    UnexpectedResponse { status: u16, message: String },
}

/// Response body for DELETE /manifests endpoint (200 success).
#[derive(Debug, serde::Deserialize)]
pub struct PruneResponse {
    /// Number of orphaned manifests deleted
    pub deleted_count: usize,
}

/// Response from GET /manifests endpoint.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ManifestsResponse {
    pub manifests: Vec<ManifestSummary>,
}

/// Summary information for a single manifest.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ManifestSummary {
    pub hash: Hash,
    pub dataset_count: u64,
}

/// Errors that can occur when listing all manifests.
#[derive(Debug, thiserror::Error)]
pub enum ListAllError {
    /// Failed to list all manifests (500, LIST_ALL_MANIFESTS_ERROR)
    ///
    /// This occurs when:
    /// - Failed to query manifests from the metadata database
    /// - Database connection issues
    /// - Internal database errors
    #[error("list all manifests error")]
    ListAllManifestsError(#[source] ApiError),

    /// Network or connection error
    #[error("network error connecting to {url}")]
    Network { url: String, source: reqwest::Error },

    /// Unexpected response from API
    #[error("unexpected response (status {status}): {message}")]
    UnexpectedResponse { status: u16, message: String },
}

/// Errors that can occur when pruning orphaned manifests.
#[derive(Debug, thiserror::Error)]
pub enum PruneError {
    /// Failed to list orphaned manifests (500, LIST_ORPHANED_MANIFESTS_ERROR)
    ///
    /// This occurs when:
    /// - Database connection is lost
    /// - Failed to query orphaned manifests
    /// - Database errors during query
    #[error("list orphaned manifests error")]
    ListOrphanedManifestsError(#[source] ApiError),

    /// Network or connection error
    #[error("network error connecting to {url}")]
    Network { url: String, source: reqwest::Error },

    /// Unexpected response from API
    #[error("unexpected response (status {status}): {message}")]
    UnexpectedResponse { status: u16, message: String },
}

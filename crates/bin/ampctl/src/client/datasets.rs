//! Dataset management API client.
//!
//! Provides methods for interacting with the `/datasets` endpoints of the admin API.

use datasets_common::{name::Name, namespace::Namespace, reference::Reference, revision::Revision};
use dump::EndBlock;
use worker::JobId;

use super::{
    Client,
    error::{ApiError, ErrorResponse},
};

/// Build URL path for registering a dataset.
///
/// POST `/datasets`
fn dataset_register() -> &'static str {
    "/datasets"
}

/// Build URL path for deploying a dataset version.
///
/// POST `/datasets/{namespace}/{name}/versions/{version}/deploy`
fn dataset_deploy(namespace: &Namespace, name: &Name, version: &Revision) -> String {
    format!("/datasets/{namespace}/{name}/versions/{version}/deploy")
}

/// Client for dataset-related API operations.
///
/// Created via [`Client::datasets`](crate::client::Client::datasets).
#[derive(Debug)]
pub struct DatasetsClient<'a> {
    client: &'a Client,
}

impl<'a> DatasetsClient<'a> {
    /// Create a new datasets client
    pub(crate) fn new(client: &'a Client) -> Self {
        Self { client }
    }

    /// Register a dataset manifest.
    ///
    /// POSTs to `/datasets` endpoint with the dataset reference and manifest content.
    ///
    /// # Errors
    ///
    /// Returns [`RegisterError`] for network errors, API errors (400/409/500),
    /// or unexpected responses.
    #[tracing::instrument(skip(self, manifest), fields(dataset_ref = %dataset_ref))]
    pub async fn register(
        &self,
        dataset_ref: &Reference,
        manifest: &str,
    ) -> Result<(), RegisterError> {
        let url = self
            .client
            .base_url()
            .join(dataset_register())
            .expect("valid URL");

        tracing::debug!("Sending dataset registration request");

        let request_body = RegisterRequest {
            namespace: dataset_ref.namespace(),
            name: dataset_ref.name(),
            version: dataset_ref.revision(),
            manifest,
        };

        let response = self
            .client
            .http()
            .post(url.as_str())
            .json(&request_body)
            .send()
            .await
            .map_err(|err| RegisterError::Network {
                url: url.to_string(),
                source: err,
            })?;

        let status = response.status();
        tracing::debug!(status = %status, "Received API response");

        match status.as_u16() {
            201 => Ok(()),
            400 | 409 | 500 => {
                let text = response.text().await.map_err(|err| {
                    tracing::error!(status = %status, error = %err, "Failed to read error response");
                    RegisterError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: format!("Failed to read error response: {}", err),
                    }
                })?;

                let error_response: ErrorResponse = serde_json::from_str(&text).map_err(|err| {
                    tracing::error!(status = %status, error = %err, "Failed to parse error response");
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
                    "MANIFEST_REGISTRATION_ERROR" => Err(RegisterError::ManifestRegistrationError(
                        error_response.into(),
                    )),
                    "VERSION_TAGGING_ERROR" => {
                        Err(RegisterError::VersionTaggingError(error_response.into()))
                    }
                    "STORE_ERROR" => Err(RegisterError::StoreError(error_response.into())),
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

    /// Deploy a dataset version.
    ///
    /// POSTs to `/datasets/{namespace}/{name}/versions/{version}/deploy` endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`DeployError`] for network errors, API errors (400/404/500),
    /// or unexpected responses.
    #[tracing::instrument(skip(self), fields(dataset_ref = %dataset_ref, ?end_block, parallelism))]
    pub async fn deploy(
        &self,
        dataset_ref: &Reference,
        end_block: Option<EndBlock>,
        parallelism: u16,
    ) -> Result<JobId, DeployError> {
        let namespace = dataset_ref.namespace();
        let name = dataset_ref.name();
        let version = dataset_ref.revision();

        let url = self
            .client
            .base_url()
            .join(&dataset_deploy(namespace, name, version))
            .expect("valid URL");

        tracing::debug!("Sending dataset deployment request");

        let request_body = DeployRequest {
            end_block,
            parallelism,
        };

        let response = self
            .client
            .http()
            .post(url.as_str())
            .json(&request_body)
            .send()
            .await
            .map_err(|err| DeployError::Network {
                url: url.to_string(),
                source: err,
            })?;

        let status = response.status();
        tracing::debug!(status = %status, "Received API response");

        match status.as_u16() {
            200 | 202 => {
                let deploy_response = response.json::<DeployResponse>().await.map_err(|err| {
                    tracing::error!(status = %status, error = %err, "Failed to parse success response");
                    DeployError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: format!("Failed to parse response: {}", err),
                    }
                })?;

                tracing::info!(job_id = %deploy_response.job_id, "Dataset deployment job scheduled");
                Ok(deploy_response.job_id)
            }
            400 | 404 | 500 => {
                let text = response.text().await.map_err(|err| {
                    tracing::error!(status = %status, error = %err, "Failed to read error response");
                    DeployError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: format!("Failed to read error response: {}", err),
                    }
                })?;

                let error_response: ErrorResponse = serde_json::from_str(&text).map_err(|err| {
                    tracing::error!(status = %status, error = %err, "Failed to parse error response");
                    DeployError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: text.clone(),
                    }
                })?;

                match error_response.error_code.as_str() {
                    "INVALID_PATH" => Err(DeployError::InvalidPath(error_response.into())),
                    "INVALID_BODY" => Err(DeployError::InvalidBody(error_response.into())),
                    "DATASET_NOT_FOUND" => Err(DeployError::NotFound(error_response.into())),
                    "LIST_VERSION_TAGS_ERROR" => {
                        Err(DeployError::ListVersionTagsError(error_response.into()))
                    }
                    "RESOLVE_REVISION_ERROR" => {
                        Err(DeployError::ResolveRevisionError(error_response.into()))
                    }
                    "GET_DATASET_ERROR" => Err(DeployError::GetDatasetError(error_response.into())),
                    "SCHEDULER_ERROR" => Err(DeployError::SchedulerError(error_response.into())),
                    _ => Err(DeployError::UnexpectedResponse {
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
                Err(DeployError::UnexpectedResponse {
                    status: status.as_u16(),
                    message: text,
                })
            }
        }
    }
}

/// Request body for POST /datasets endpoint.
#[derive(Debug, serde::Serialize)]
struct RegisterRequest<'a> {
    namespace: &'a Namespace,
    name: &'a Name,
    version: &'a Revision,
    manifest: &'a str,
}

/// Request body for POST /datasets/{namespace}/{name}/versions/{version}/deploy endpoint.
#[derive(Debug, serde::Serialize)]
struct DeployRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    end_block: Option<EndBlock>,
    parallelism: u16,
}

/// Response from the deploy endpoint.
#[derive(Debug, serde::Deserialize)]
struct DeployResponse {
    job_id: JobId,
}

/// Errors that can occur when registering a dataset.
#[derive(Debug, thiserror::Error)]
pub enum RegisterError {
    /// Invalid request format (400, INVALID_PAYLOAD_FORMAT)
    ///
    /// This occurs when:
    /// - Request JSON is malformed or invalid
    /// - Required fields are missing or have wrong types
    /// - Dataset name or version format is invalid
    #[error("invalid payload format")]
    InvalidPayloadFormat(#[source] ApiError),

    /// Invalid derived dataset manifest content or structure (400, INVALID_MANIFEST)
    ///
    /// This occurs when:
    /// - Manifest JSON is malformed or invalid
    /// - Manifest structure doesn't match expected schema
    /// - Required manifest fields are missing or invalid
    #[error("invalid manifest")]
    InvalidManifest(#[source] ApiError),

    /// Dependency validation error (400, DEPENDENCY_VALIDATION_ERROR)
    ///
    /// This occurs when:
    /// - SQL queries are invalid
    /// - SQL queries reference datasets not declared in dependencies
    #[error("dependency validation error")]
    DependencyValidationError(#[source] ApiError),

    /// Unsupported dataset kind (400, UNSUPPORTED_DATASET_KIND)
    ///
    /// This occurs when:
    /// - Dataset kind is not one of the supported types (manifest, evm-rpc, firehose, eth-beacon)
    #[error("unsupported dataset kind")]
    UnsupportedDatasetKind(#[source] ApiError),

    /// Failed to register manifest in the system (500, MANIFEST_REGISTRATION_ERROR)
    ///
    /// This occurs when:
    /// - Error during manifest processing or storage
    /// - Registry information extraction failed
    /// - System-level registration errors
    #[error("manifest registration error")]
    ManifestRegistrationError(#[source] ApiError),

    /// Failed to tag version for the dataset (500, VERSION_TAGGING_ERROR)
    ///
    /// This occurs when:
    /// - Error during version tagging in metadata database
    /// - Invalid semantic version format
    /// - Error updating latest tag
    #[error("version tagging error")]
    VersionTaggingError(#[source] ApiError),

    /// Dataset store error (500, STORE_ERROR)
    ///
    /// This occurs when:
    /// - Failed to load dataset from store
    /// - Dataset store configuration errors
    /// - Dataset store connectivity issues
    #[error("store error")]
    StoreError(#[source] ApiError),

    /// Network or connection error
    #[error("network error connecting to {url}")]
    Network { url: String, source: reqwest::Error },

    /// Unexpected response from API
    #[error("unexpected response (status {status}): {message}")]
    UnexpectedResponse { status: u16, message: String },
}

/// Errors that can occur when deploying a dataset.
#[derive(Debug, thiserror::Error)]
pub enum DeployError {
    /// Invalid path parameters (400, INVALID_PATH)
    ///
    /// This occurs when:
    /// - The namespace, name, or revision in the URL path is invalid
    /// - Path parameter parsing fails
    #[error("invalid path")]
    InvalidPath(#[source] ApiError),

    /// Invalid request body (400, INVALID_BODY)
    ///
    /// This occurs when:
    /// - The request body is not valid JSON
    /// - The JSON structure doesn't match the expected schema
    /// - Required fields are missing or have invalid types
    #[error("invalid body")]
    InvalidBody(#[source] ApiError),

    /// Dataset or revision not found (404, DATASET_NOT_FOUND)
    ///
    /// This occurs when:
    /// - The specified dataset name doesn't exist in the namespace
    /// - The specified revision doesn't exist for this dataset
    /// - The revision resolves to a manifest that doesn't exist
    #[error("dataset not found")]
    NotFound(#[source] ApiError),

    /// Dataset store operation error when listing version tags (500, LIST_VERSION_TAGS_ERROR)
    ///
    /// This occurs when:
    /// - Failed to query version tags from the dataset store
    /// - Database connection issues
    /// - Internal database errors
    #[error("list version tags error")]
    ListVersionTagsError(#[source] ApiError),

    /// Dataset store operation error when resolving revision (500, RESOLVE_REVISION_ERROR)
    ///
    /// This occurs when:
    /// - Failed to resolve revision to manifest hash
    /// - Database connection issues
    /// - Internal database errors
    #[error("resolve revision error")]
    ResolveRevisionError(#[source] ApiError),

    /// Dataset store operation error when loading dataset (500, GET_DATASET_ERROR)
    ///
    /// This occurs when:
    /// - Failed to load dataset configuration from manifest
    /// - Manifest parsing errors
    /// - Invalid dataset structure
    #[error("get dataset error")]
    GetDatasetError(#[source] ApiError),

    /// Scheduler error (500, SCHEDULER_ERROR)
    ///
    /// This occurs when:
    /// - Failed to create or schedule extraction job
    /// - Scheduler is unavailable or overloaded
    /// - Invalid job configuration
    #[error("scheduler error")]
    SchedulerError(#[source] ApiError),

    /// Network or connection error
    #[error("network error connecting to {url}")]
    Network { url: String, source: reqwest::Error },

    /// Unexpected response from API
    #[error("unexpected response (status {status}): {message}")]
    UnexpectedResponse { status: u16, message: String },
}

//! Dataset management API client.
//!
//! Provides methods for interacting with the `/datasets` endpoints of the admin API.

use datasets_common::{
    fqn::FullyQualifiedName, hash::Hash, name::Name, namespace::Namespace, reference::Reference,
    revision::Revision, version::Version,
};
use dump::EndBlock;
use monitoring::logging;
use serde_json::value::RawValue;
use worker::{job::JobId, node_id::NodeId};

use super::{
    Client,
    error::{ApiError, ErrorResponse},
};

/// Build URL path for listing all datasets.
///
/// GET `/datasets`
fn datasets_list() -> &'static str {
    "datasets"
}

/// Build URL path for registering a dataset.
///
/// POST `/datasets`
fn dataset_register() -> &'static str {
    "datasets"
}

/// Build URL path for getting a dataset by reference.
///
/// GET `/datasets/{namespace}/{name}/versions/{revision}`
fn dataset_get(reference: &Reference) -> String {
    format!(
        "datasets/{}/{}/versions/{}",
        reference.namespace(),
        reference.name(),
        reference.revision()
    )
}

/// Build URL path for deleting a dataset by FQN.
///
/// DELETE `/datasets/{namespace}/{name}`
fn dataset_delete(fqn: &FullyQualifiedName) -> String {
    format!("datasets/{}/{}", fqn.namespace(), fqn.name())
}

/// Build URL path for listing versions of a dataset.
///
/// GET `/datasets/{namespace}/{name}/versions`
fn dataset_list_versions(fqn: &FullyQualifiedName) -> String {
    format!("datasets/{}/{}/versions", fqn.namespace(), fqn.name())
}

/// Build URL path for deleting a version of a dataset.
///
/// DELETE `/datasets/{namespace}/{name}/versions/{version}`
fn dataset_delete_version(fqn: &FullyQualifiedName, version: &Version) -> String {
    format!(
        "datasets/{}/{}/versions/{}",
        fqn.namespace(),
        fqn.name(),
        version
    )
}

/// Build URL path for getting a manifest by reference.
///
/// GET `/datasets/{namespace}/{name}/versions/{revision}/manifest`
fn dataset_get_manifest(reference: &Reference) -> String {
    format!(
        "datasets/{}/{}/versions/{}/manifest",
        reference.namespace(),
        reference.name(),
        reference.revision()
    )
}

/// Build URL path for deploying a dataset version.
///
/// POST `/datasets/{namespace}/{name}/versions/{version}/deploy`
fn dataset_deploy(namespace: &Namespace, name: &Name, version: &Revision) -> String {
    format!("datasets/{namespace}/{name}/versions/{version}/deploy")
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
    /// POSTs to `/datasets` endpoint with the dataset FQN, optional version, and manifest input.
    /// If no version is provided, the server will update the "dev" tag.
    ///
    /// The `manifest` parameter can be either:
    /// - A manifest hash (to link to an existing manifest)
    /// - Full manifest content (to register a new manifest)
    ///
    /// # Errors
    ///
    /// Returns [`RegisterError`] for network errors, API errors (400/409/500),
    /// or unexpected responses.
    #[tracing::instrument(skip(self, manifest), fields(fqn = %fqn, version = ?version))]
    pub async fn register(
        &self,
        fqn: &FullyQualifiedName,
        version: Option<&Version>,
        manifest: impl Into<HashOrManifestJson>,
    ) -> Result<(), RegisterError> {
        let url = self
            .client
            .base_url()
            .join(dataset_register())
            .expect("valid URL");

        tracing::debug!("Sending dataset registration request");

        let request_body = RegisterRequest {
            namespace: fqn.namespace(),
            name: fqn.name(),
            version,
            manifest: manifest.into(),
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
    #[tracing::instrument(skip(self), fields(dataset_ref = %dataset_ref, ?end_block, parallelism, ?worker_id))]
    pub async fn deploy(
        &self,
        dataset_ref: &Reference,
        end_block: Option<EndBlock>,
        parallelism: u16,
        worker_id: Option<NodeId>,
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
            worker_id,
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
                    tracing::error!(status = %status, error = %err, error_source = logging::error_source(&err), "Failed to parse success response");
                    DeployError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: format!("Failed to parse response: {}", err),
                    }
                })?;

                tracing::debug!(job_id = %deploy_response.job_id, "Dataset deployment job scheduled");
                Ok(deploy_response.job_id)
            }
            400 | 404 | 500 => {
                let text = response.text().await.map_err(|err| {
                    tracing::error!(status = %status, error = %err, error_source = logging::error_source(&err), "Failed to read error response");
                    DeployError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: format!("Failed to read error response: {}", err),
                    }
                })?;

                let error_response: ErrorResponse = serde_json::from_str(&text).map_err(|err| {
                    tracing::error!(status = %status, error = %err, error_source = logging::error_source(&err), "Failed to parse error response");
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
                    "WORKER_NOT_AVAILABLE" => {
                        Err(DeployError::WorkerNotAvailable(error_response.into()))
                    }
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

    /// List all registered datasets.
    ///
    /// GETs from `/datasets` endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`ListAllError`] for network errors, API errors (500),
    /// or unexpected responses.
    #[tracing::instrument(skip(self))]
    pub async fn list_all(&self) -> Result<DatasetsResponse, ListAllError> {
        let url = self
            .client
            .base_url()
            .join(datasets_list())
            .expect("valid URL");

        tracing::debug!("Sending list all datasets request");

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
                let datasets_response =
                    response.json::<DatasetsResponse>().await.map_err(|err| {
                        tracing::error!(error = %err, error_source = logging::error_source(&err), "Failed to parse datasets response");
                        ListAllError::UnexpectedResponse {
                            status: status.as_u16(),
                            message: format!("Failed to parse response: {}", err),
                        }
                    })?;
                Ok(datasets_response)
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
                    "LIST_ALL_DATASETS_ERROR" => {
                        Err(ListAllError::ListAllDatasetsError(error_response.into()))
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

    /// Get a dataset by reference.
    ///
    /// GETs from `/datasets/{namespace}/{name}/versions/{revision}` endpoint.
    ///
    /// Returns `None` if the dataset does not exist (404).
    ///
    /// # Errors
    ///
    /// Returns [`GetError`] for network errors, API errors (400/500),
    /// or unexpected responses.
    #[tracing::instrument(skip(self), fields(reference = %reference))]
    pub async fn get(&self, reference: &Reference) -> Result<Option<DatasetInfo>, GetError> {
        let url = self
            .client
            .base_url()
            .join(&dataset_get(reference))
            .expect("valid URL");

        tracing::debug!("Sending get dataset request");

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
                let dataset_info: DatasetInfo = response.json().await.map_err(|err| {
                    tracing::error!(error = %err, error_source = logging::error_source(&err), "Failed to parse dataset response");
                    GetError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: format!("Failed to parse response: {}", err),
                    }
                })?;
                Ok(Some(dataset_info))
            }
            404 => {
                tracing::debug!("Dataset not found");
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
                    "INVALID_PATH" => Err(GetError::InvalidPath(error_response.into())),
                    "DATASET_NOT_FOUND" => Err(GetError::DatasetNotFound(error_response.into())),
                    "RESOLVE_REVISION_ERROR" => {
                        Err(GetError::ResolveRevisionError(error_response.into()))
                    }
                    "GET_MANIFEST_PATH_ERROR" => {
                        Err(GetError::GetManifestPathError(error_response.into()))
                    }
                    "READ_MANIFEST_ERROR" => {
                        Err(GetError::ReadManifestError(error_response.into()))
                    }
                    "PARSE_MANIFEST_ERROR" => {
                        Err(GetError::ParseManifestError(error_response.into()))
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

    /// Delete a dataset by fully qualified name.
    ///
    /// DELETEs at `/datasets/{namespace}/{name}` endpoint.
    ///
    /// This is idempotent - succeeds even if the dataset doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns [`DeleteError`] for network errors, API errors (400/500),
    /// or unexpected responses.
    #[tracing::instrument(skip(self), fields(fqn = %fqn))]
    pub async fn delete(&self, fqn: &FullyQualifiedName) -> Result<(), DeleteError> {
        let url = self
            .client
            .base_url()
            .join(&dataset_delete(fqn))
            .expect("valid URL");

        tracing::debug!("Sending delete dataset request");

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
                tracing::debug!("Dataset deleted successfully");
                Ok(())
            }
            400 | 500 => {
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
                    "INVALID_PATH" => Err(DeleteError::InvalidPath(error_response.into())),
                    "UNLINK_DATASET_MANIFESTS_ERROR" => Err(
                        DeleteError::UnlinkDatasetManifestsError(error_response.into()),
                    ),
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

    /// List all versions of a dataset.
    ///
    /// GETs from `/datasets/{namespace}/{name}/versions` endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`ListVersionsError`] for network errors, API errors (400/500),
    /// or unexpected responses.
    #[tracing::instrument(skip(self), fields(fqn = %fqn))]
    pub async fn list_versions(
        &self,
        fqn: &FullyQualifiedName,
    ) -> Result<VersionsResponse, ListVersionsError> {
        let url = self
            .client
            .base_url()
            .join(&dataset_list_versions(fqn))
            .expect("valid URL");

        tracing::debug!("Sending list versions request");

        let response = self
            .client
            .http()
            .get(url.as_str())
            .send()
            .await
            .map_err(|err| ListVersionsError::Network {
                url: url.to_string(),
                source: err,
            })?;

        let status = response.status();
        tracing::debug!(status = %status, "Received API response");

        match status.as_u16() {
            200 => {
                let versions_response =
                    response.json::<VersionsResponse>().await.map_err(|err| {
                        tracing::error!(error = %err, error_source = logging::error_source(&err), "Failed to parse versions response");
                        ListVersionsError::UnexpectedResponse {
                            status: status.as_u16(),
                            message: format!("Failed to parse response: {}", err),
                        }
                    })?;
                Ok(versions_response)
            }
            400 | 500 => {
                let text = response.text().await.map_err(|err| {
                    tracing::error!(status = %status, error = %err, error_source = logging::error_source(&err), "Failed to read error response");
                    ListVersionsError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: format!("Failed to read error response: {}", err),
                    }
                })?;

                let error_response: ErrorResponse = serde_json::from_str(&text).map_err(|err| {
                    tracing::error!(status = %status, error = %err, error_source = logging::error_source(&err), "Failed to parse error response");
                    ListVersionsError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: text.clone(),
                    }
                })?;

                match error_response.error_code.as_str() {
                    "INVALID_PATH" => Err(ListVersionsError::InvalidPath(error_response.into())),
                    "LIST_VERSION_TAGS_ERROR" => Err(ListVersionsError::ListVersionTagsError(
                        error_response.into(),
                    )),
                    "RESOLVE_REVISION_ERROR" => Err(ListVersionsError::ResolveRevisionError(
                        error_response.into(),
                    )),
                    _ => Err(ListVersionsError::UnexpectedResponse {
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
                Err(ListVersionsError::UnexpectedResponse {
                    status: status.as_u16(),
                    message: text,
                })
            }
        }
    }

    /// Delete a specific version of a dataset.
    ///
    /// DELETEs at `/datasets/{namespace}/{name}/versions/{version}` endpoint.
    ///
    /// This is idempotent - succeeds even if the version doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns [`DeleteVersionError`] for network errors, API errors (400/500),
    /// or unexpected responses.
    #[tracing::instrument(skip(self), fields(fqn = %fqn, version = %version))]
    pub async fn delete_version(
        &self,
        fqn: &FullyQualifiedName,
        version: &Version,
    ) -> Result<(), DeleteVersionError> {
        let url = self
            .client
            .base_url()
            .join(&dataset_delete_version(fqn, version))
            .expect("valid URL");

        tracing::debug!("Sending delete version request");

        let response = self
            .client
            .http()
            .delete(url.as_str())
            .send()
            .await
            .map_err(|err| DeleteVersionError::Network {
                url: url.to_string(),
                source: err,
            })?;

        let status = response.status();
        tracing::debug!(status = %status, "Received API response");

        match status.as_u16() {
            204 => {
                tracing::debug!("Version deleted successfully");
                Ok(())
            }
            400 | 500 => {
                let text = response.text().await.map_err(|err| {
                    tracing::error!(status = %status, error = %err, error_source = logging::error_source(&err), "Failed to read error response");
                    DeleteVersionError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: format!("Failed to read error response: {}", err),
                    }
                })?;

                let error_response: ErrorResponse = serde_json::from_str(&text).map_err(|err| {
                    tracing::error!(status = %status, error = %err, error_source = logging::error_source(&err), "Failed to parse error response");
                    DeleteVersionError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: text.clone(),
                    }
                })?;

                match error_response.error_code.as_str() {
                    "INVALID_PATH" => Err(DeleteVersionError::InvalidPath(error_response.into())),
                    "CANNOT_DELETE_LATEST_VERSION" => Err(
                        DeleteVersionError::CannotDeleteLatestVersion(error_response.into()),
                    ),
                    "RESOLVE_LATEST_REVISION_ERROR" => Err(
                        DeleteVersionError::ResolveLatestRevisionError(error_response.into()),
                    ),
                    "RESOLVE_VERSION_REVISION_ERROR" => Err(
                        DeleteVersionError::ResolveVersionRevisionError(error_response.into()),
                    ),
                    "DELETE_VERSION_TAG_ERROR" => Err(DeleteVersionError::DeleteVersionTagError(
                        error_response.into(),
                    )),
                    _ => Err(DeleteVersionError::UnexpectedResponse {
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
                Err(DeleteVersionError::UnexpectedResponse {
                    status: status.as_u16(),
                    message: text,
                })
            }
        }
    }

    /// Get the manifest JSON for a dataset revision.
    ///
    /// GETs from `/datasets/{namespace}/{name}/versions/{revision}/manifest` endpoint.
    ///
    /// Returns `None` if the dataset or manifest does not exist (404).
    ///
    /// # Errors
    ///
    /// Returns [`GetManifestError`] for network errors, API errors (500),
    /// or unexpected responses.
    #[tracing::instrument(skip(self), fields(reference = %reference))]
    pub async fn get_manifest(
        &self,
        reference: &Reference,
    ) -> Result<Option<serde_json::Value>, GetManifestError> {
        let url = self
            .client
            .base_url()
            .join(&dataset_get_manifest(reference))
            .expect("valid URL");

        tracing::debug!("Sending get manifest request");

        let response = self
            .client
            .http()
            .get(url.as_str())
            .send()
            .await
            .map_err(|err| GetManifestError::Network {
                url: url.to_string(),
                source: err,
            })?;

        let status = response.status();
        tracing::debug!(status = %status, "Received API response");

        match status.as_u16() {
            200 => {
                let manifest: serde_json::Value = response.json().await.map_err(|err| {
                    tracing::error!(error = %err, error_source = logging::error_source(&err), "Failed to parse manifest JSON");
                    GetManifestError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: format!("Failed to parse response: {}", err),
                    }
                })?;
                Ok(Some(manifest))
            }
            404 => {
                tracing::debug!("Manifest not found");
                Ok(None)
            }
            500 => {
                let text = response.text().await.map_err(|err| {
                    tracing::error!(status = %status, error = %err, error_source = logging::error_source(&err), "Failed to read error response");
                    GetManifestError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: format!("Failed to read error response: {}", err),
                    }
                })?;

                let error_response: ErrorResponse = serde_json::from_str(&text).map_err(|err| {
                    tracing::error!(status = %status, error = %err, error_source = logging::error_source(&err), "Failed to parse error response");
                    GetManifestError::UnexpectedResponse {
                        status: status.as_u16(),
                        message: text.clone(),
                    }
                })?;

                match error_response.error_code.as_str() {
                    "INVALID_PATH" => Err(GetManifestError::InvalidPath(error_response.into())),
                    "DATASET_NOT_FOUND" => {
                        Err(GetManifestError::DatasetNotFound(error_response.into()))
                    }
                    "MANIFEST_NOT_FOUND" => {
                        Err(GetManifestError::ManifestNotFound(error_response.into()))
                    }
                    "RESOLVE_REVISION_ERROR" => Err(GetManifestError::ResolveRevisionError(
                        error_response.into(),
                    )),
                    "GET_MANIFEST_PATH_ERROR" => Err(GetManifestError::GetManifestPathError(
                        error_response.into(),
                    )),
                    "READ_MANIFEST_ERROR" => {
                        Err(GetManifestError::ReadManifestError(error_response.into()))
                    }
                    "PARSE_MANIFEST_ERROR" => {
                        Err(GetManifestError::ParseManifestError(error_response.into()))
                    }
                    _ => Err(GetManifestError::UnexpectedResponse {
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
                Err(GetManifestError::UnexpectedResponse {
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
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<&'a Version>,
    manifest: HashOrManifestJson,
}

/// Request body for POST /datasets/{namespace}/{name}/versions/{version}/deploy endpoint.
#[derive(Debug, serde::Serialize)]
struct DeployRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    end_block: Option<EndBlock>,
    parallelism: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    worker_id: Option<NodeId>,
}

/// Input type for dataset registration manifest parameter.
///
/// This enum allows callers to provide either:
/// - A manifest hash (64-character SHA-256 hex string) to link to an existing manifest
/// - A full manifest content as raw JSON to register a new manifest
///
/// This mirrors the server's `ManifestInput` enum, providing type safety at the client API level.
#[derive(Debug, Clone)]
pub enum HashOrManifestJson {
    /// A reference to an existing manifest by its SHA-256 hash.
    ///
    /// The hash must be exactly 64 hexadecimal characters.
    /// When this variant is used, the manifest must already exist in the system.
    Hash(Hash),

    /// Full manifest content as raw JSON.
    ///
    /// The manifest will be validated, canonicalized, and stored during registration.
    ManifestJson(Box<RawValue>),
}

impl serde::Serialize for HashOrManifestJson {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            // Serialize hash as a string
            HashOrManifestJson::Hash(hash) => hash.serialize(serializer),
            // Delegate to RawValue's serialize to preserve raw JSON pass-through
            HashOrManifestJson::ManifestJson(raw) => raw.serialize(serializer),
        }
    }
}

impl From<Hash> for HashOrManifestJson {
    fn from(value: Hash) -> Self {
        Self::Hash(value)
    }
}

impl From<Box<RawValue>> for HashOrManifestJson {
    fn from(value: Box<RawValue>) -> Self {
        Self::ManifestJson(value)
    }
}

impl std::str::FromStr for HashOrManifestJson {
    type Err = HashOrManifestParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Try to parse as Hash first
        if let Ok(hash) = s.parse::<Hash>() {
            return Ok(Self::Hash(hash));
        }

        // If that fails, try as manifest JSON
        serde_json::from_str::<Box<RawValue>>(s)
            .map(Into::into)
            .map_err(HashOrManifestParseError)
    }
}

/// Error type for parsing `HashOrManifest` from a string.
///
/// This error occurs when the input string cannot be parsed as either:
/// - A valid manifest hash (64-character hexadecimal string)
/// - Valid manifest JSON content
#[derive(Debug, thiserror::Error)]
#[error("failed to parse manifest input: not a valid hash or JSON manifest")]
pub struct HashOrManifestParseError(#[source] serde_json::Error);

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

    /// Invalid manifest JSON format (client-side parsing error)
    ///
    /// This occurs when:
    /// - The manifest string provided to the client is not valid JSON
    /// - The manifest cannot be parsed before sending to server
    #[error("invalid manifest JSON: {0}")]
    InvalidManifestJson(#[source] serde_json::Error),

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

    /// Specified worker not found or inactive (400, WORKER_NOT_AVAILABLE)
    ///
    /// This occurs when:
    /// - The specified worker ID doesn't exist in the system
    /// - The specified worker hasn't sent heartbeats recently (inactive)
    #[error("worker not available")]
    WorkerNotAvailable(#[source] ApiError),

    /// Network or connection error
    #[error("network error connecting to {url}")]
    Network { url: String, source: reqwest::Error },

    /// Unexpected response from API
    #[error("unexpected response (status {status}): {message}")]
    UnexpectedResponse { status: u16, message: String },
}

/// Response from GET /datasets endpoint.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct DatasetsResponse {
    pub datasets: Vec<DatasetSummary>,
}

/// Summary information for a single dataset.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct DatasetSummary {
    pub namespace: Namespace,
    pub name: Name,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_version: Option<Version>,
    pub versions: Vec<Version>,
}

/// Detailed information for a dataset at a specific revision.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct DatasetInfo {
    pub namespace: Namespace,
    pub name: Name,
    pub revision: Revision,
    pub manifest_hash: String,
    pub kind: String,
}

/// Response from GET /datasets/{namespace}/{name}/versions endpoint.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct VersionsResponse {
    pub namespace: Namespace,
    pub name: Name,
    pub versions: Vec<VersionInfo>,
    pub special_tags: SpecialTags,
}

/// Information about a specific version.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct VersionInfo {
    pub version: Version,
    pub manifest_hash: Hash,
    pub created_at: String,
    pub updated_at: String,
}

/// Special tags for a dataset (latest and dev).
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct SpecialTags {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest: Option<Version>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dev: Option<Hash>,
}

/// Errors that can occur when listing all datasets.
#[derive(Debug, thiserror::Error)]
pub enum ListAllError {
    /// Failed to list all datasets (500, LIST_ALL_DATASETS_ERROR)
    ///
    /// This occurs when:
    /// - Failed to query datasets from the dataset store
    /// - Database connection issues
    /// - Internal database errors
    #[error("list all datasets error")]
    ListAllDatasetsError(#[source] ApiError),

    /// Network or connection error
    #[error("network error connecting to {url}")]
    Network { url: String, source: reqwest::Error },

    /// Unexpected response from API
    #[error("unexpected response (status {status}): {message}")]
    UnexpectedResponse { status: u16, message: String },
}

/// Errors that can occur when getting a dataset.
#[derive(Debug, thiserror::Error)]
pub enum GetError {
    /// Invalid path parameters (400, INVALID_PATH)
    ///
    /// This occurs when:
    /// - The namespace, name, or revision in the URL path is invalid
    /// - Path parameter parsing fails
    #[error("invalid path")]
    InvalidPath(#[source] ApiError),

    /// Dataset or revision not found (404, DATASET_NOT_FOUND)
    ///
    /// This occurs when:
    /// - The specified dataset doesn't exist
    /// - The specified revision doesn't exist for this dataset
    #[error("dataset not found")]
    DatasetNotFound(#[source] ApiError),

    /// Failed to resolve revision (500, RESOLVE_REVISION_ERROR)
    ///
    /// This occurs when:
    /// - Failed to resolve revision to manifest hash
    /// - Database connection issues
    /// - Internal database errors
    #[error("resolve revision error")]
    ResolveRevisionError(#[source] ApiError),

    /// Failed to get manifest path (500, GET_MANIFEST_PATH_ERROR)
    ///
    /// This occurs when:
    /// - Failed to query manifest path from database
    /// - Database connection issues
    #[error("get manifest path error")]
    GetManifestPathError(#[source] ApiError),

    /// Failed to read manifest (500, READ_MANIFEST_ERROR)
    ///
    /// This occurs when:
    /// - Failed to read manifest file from storage
    /// - Storage connectivity issues
    #[error("read manifest error")]
    ReadManifestError(#[source] ApiError),

    /// Failed to parse manifest (500, PARSE_MANIFEST_ERROR)
    ///
    /// This occurs when:
    /// - Manifest JSON is invalid or malformed
    /// - Manifest doesn't match expected schema
    #[error("parse manifest error")]
    ParseManifestError(#[source] ApiError),

    /// Network or connection error
    #[error("network error connecting to {url}")]
    Network { url: String, source: reqwest::Error },

    /// Unexpected response from API
    #[error("unexpected response (status {status}): {message}")]
    UnexpectedResponse { status: u16, message: String },
}

/// Errors that can occur when deleting a dataset.
#[derive(Debug, thiserror::Error)]
pub enum DeleteError {
    /// Invalid path parameters (400, INVALID_PATH)
    ///
    /// This occurs when:
    /// - The namespace or name in the URL path is invalid
    /// - Path parameter parsing fails
    #[error("invalid path")]
    InvalidPath(#[source] ApiError),

    /// Failed to unlink dataset manifests (500, UNLINK_DATASET_MANIFESTS_ERROR)
    ///
    /// This occurs when:
    /// - Failed to remove manifest links from database
    /// - Database connection issues
    /// - Internal database errors
    #[error("unlink dataset manifests error")]
    UnlinkDatasetManifestsError(#[source] ApiError),

    /// Network or connection error
    #[error("network error connecting to {url}")]
    Network { url: String, source: reqwest::Error },

    /// Unexpected response from API
    #[error("unexpected response (status {status}): {message}")]
    UnexpectedResponse { status: u16, message: String },
}

/// Errors that can occur when listing dataset versions.
#[derive(Debug, thiserror::Error)]
pub enum ListVersionsError {
    /// Invalid path parameters (400, INVALID_PATH)
    ///
    /// This occurs when:
    /// - The namespace or name in the URL path is invalid
    /// - Path parameter parsing fails
    #[error("invalid path")]
    InvalidPath(#[source] ApiError),

    /// Failed to list version tags (500, LIST_VERSION_TAGS_ERROR)
    ///
    /// This occurs when:
    /// - Failed to query version tags from database
    /// - Database connection issues
    /// - Internal database errors
    #[error("list version tags error")]
    ListVersionTagsError(#[source] ApiError),

    /// Failed to resolve revision (500, RESOLVE_REVISION_ERROR)
    ///
    /// This occurs when:
    /// - Failed to resolve dev tag
    /// - Database connection issues
    #[error("resolve revision error")]
    ResolveRevisionError(#[source] ApiError),

    /// Network or connection error
    #[error("network error connecting to {url}")]
    Network { url: String, source: reqwest::Error },

    /// Unexpected response from API
    #[error("unexpected response (status {status}): {message}")]
    UnexpectedResponse { status: u16, message: String },
}

/// Errors that can occur when deleting a dataset version.
#[derive(Debug, thiserror::Error)]
pub enum DeleteVersionError {
    /// Invalid path parameters (400, INVALID_PATH)
    ///
    /// This occurs when:
    /// - The namespace, name, or version in the URL path is invalid
    /// - Path parameter parsing fails
    #[error("invalid path")]
    InvalidPath(#[source] ApiError),

    /// Cannot delete latest version (400, CANNOT_DELETE_LATEST_VERSION)
    ///
    /// This occurs when:
    /// - Attempting to delete a version that is tagged as "latest"
    #[error("cannot delete latest version")]
    CannotDeleteLatestVersion(#[source] ApiError),

    /// Failed to resolve latest revision (500, RESOLVE_LATEST_REVISION_ERROR)
    ///
    /// This occurs when:
    /// - Failed to resolve the "latest" tag
    /// - Database connection issues
    #[error("resolve latest revision error")]
    ResolveLatestRevisionError(#[source] ApiError),

    /// Failed to resolve version revision (500, RESOLVE_VERSION_REVISION_ERROR)
    ///
    /// This occurs when:
    /// - Failed to resolve the version to delete
    /// - Database connection issues
    #[error("resolve version revision error")]
    ResolveVersionRevisionError(#[source] ApiError),

    /// Failed to delete version tag (500, DELETE_VERSION_TAG_ERROR)
    ///
    /// This occurs when:
    /// - Failed to remove version tag from database
    /// - Database connection issues
    /// - Internal database errors
    #[error("delete version tag error")]
    DeleteVersionTagError(#[source] ApiError),

    /// Network or connection error
    #[error("network error connecting to {url}")]
    Network { url: String, source: reqwest::Error },

    /// Unexpected response from API
    #[error("unexpected response (status {status}): {message}")]
    UnexpectedResponse { status: u16, message: String },
}

/// Errors that can occur when getting a dataset manifest.
#[derive(Debug, thiserror::Error)]
pub enum GetManifestError {
    /// Invalid path parameters (400, INVALID_PATH)
    ///
    /// This occurs when:
    /// - The namespace, name, or revision in the URL path is invalid
    /// - Path parameter parsing fails
    #[error("invalid path")]
    InvalidPath(#[source] ApiError),

    /// Dataset or revision not found (404, DATASET_NOT_FOUND)
    ///
    /// This occurs when:
    /// - The specified dataset doesn't exist
    /// - The specified revision doesn't exist for this dataset
    #[error("dataset not found")]
    DatasetNotFound(#[source] ApiError),

    /// Manifest file not found (404, MANIFEST_NOT_FOUND)
    ///
    /// This occurs when:
    /// - The manifest file doesn't exist in storage
    #[error("manifest not found")]
    ManifestNotFound(#[source] ApiError),

    /// Failed to resolve revision (500, RESOLVE_REVISION_ERROR)
    ///
    /// This occurs when:
    /// - Failed to resolve revision to manifest hash
    /// - Database connection issues
    #[error("resolve revision error")]
    ResolveRevisionError(#[source] ApiError),

    /// Failed to get manifest path (500, GET_MANIFEST_PATH_ERROR)
    ///
    /// This occurs when:
    /// - Failed to query manifest path from database
    /// - Database connection issues
    #[error("get manifest path error")]
    GetManifestPathError(#[source] ApiError),

    /// Failed to read manifest (500, READ_MANIFEST_ERROR)
    ///
    /// This occurs when:
    /// - Failed to read manifest file from storage
    /// - Storage connectivity issues
    #[error("read manifest error")]
    ReadManifestError(#[source] ApiError),

    /// Failed to parse manifest (500, PARSE_MANIFEST_ERROR)
    ///
    /// This occurs when:
    /// - Manifest JSON is invalid or malformed
    /// - Manifest doesn't match expected schema
    #[error("parse manifest error")]
    ParseManifestError(#[source] ApiError),

    /// Network or connection error
    #[error("network error connecting to {url}")]
    Network { url: String, source: reqwest::Error },

    /// Unexpected response from API
    #[error("unexpected response (status {status}): {message}")]
    UnexpectedResponse { status: u16, message: String },
}

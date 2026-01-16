use amp_datasets_registry::error::ListDatasetsUsingManifestError;
use axum::{
    Json,
    extract::{Path, State, rejection::PathRejection},
    http::StatusCode,
};
use datasets_common::{hash::Hash, name::Name, namespace::Namespace, version::Version};
use monitoring::logging;

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
};

/// Handler for the `GET /manifests/{hash}/datasets` endpoint
///
/// Lists all datasets that reference a specific manifest hash.
///
/// ## Path Parameters
/// - `hash`: Manifest content hash (validated hash format)
///
/// ## Response
/// - **200 OK**: Successfully retrieved datasets using manifest
/// - **400 Bad Request**: Invalid manifest hash format
/// - **404 Not Found**: Manifest with the given hash does not exist
/// - **500 Internal Server Error**: Database query error
///
/// ## Error Codes
/// - `INVALID_HASH`: The provided hash is not valid (invalid hash format or parsing error)
/// - `MANIFEST_NOT_FOUND`: No manifest exists with the given hash
/// - `QUERY_MANIFEST_PATH_ERROR`: Failed to query manifest path from metadata database
/// - `LIST_DATASET_TAGS_ERROR`: Failed to list dataset tags from metadata database
///
/// ## Behavior
/// This handler queries the dataset store to find all datasets using a manifest:
/// - Validates and extracts the manifest hash from the URL path
/// - Queries dataset store for all dataset tags referencing this manifest
/// - Returns 404 if the manifest doesn't exist
/// - Returns list of datasets with their namespace, name, and version
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        get,
        path = "/manifests/{hash}/datasets",
        tag = "manifests",
        operation_id = "list_manifest_datasets",
        params(
            ("hash" = String, Path, description = "Manifest hash (64-char hex)")
        ),
        responses(
            (status = 200, description = "Successfully retrieved datasets using manifest", body = ManifestDatasetsResponse),
            (status = 400, description = "Invalid manifest hash", body = ErrorResponse),
            (status = 404, description = "Manifest not found", body = ErrorResponse),
            (status = 500, description = "Internal server error", body = ErrorResponse)
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    path: Result<Path<Hash>, PathRejection>,
) -> Result<Json<ManifestDatasetsResponse>, ErrorResponse> {
    let hash = match path {
        Ok(Path(hash)) => hash,
        Err(err) => {
            tracing::debug!(error = %err, error_source = logging::error_source(&err), "invalid manifest hash in path");
            return Err(Error::InvalidHash(err).into());
        }
    };

    let Some(tags) = ctx
        .datasets_registry
        .list_manifest_linked_datasets(&hash)
        .await
        .map_err(Error::ListDatasetTags)?
    else {
        return Err(Error::ManifestNotFound { hash }.into());
    };

    Ok(Json(ManifestDatasetsResponse {
        hash,
        datasets: tags.into_iter().map(Into::into).collect(),
    }))
}

/// Response for listing datasets using a manifest
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct ManifestDatasetsResponse {
    /// Manifest hash
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub hash: Hash,
    /// List of datasets using this manifest
    pub datasets: Vec<Dataset>,
}

/// Dataset information
///
/// Represents a dataset tag with its namespace, name, and version.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct Dataset {
    /// Dataset namespace
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub namespace: Namespace,
    /// Dataset name
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub name: Name,
    /// Version tag
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub version: Version,
}

impl From<amp_datasets_registry::DatasetTag> for Dataset {
    fn from(tag: amp_datasets_registry::DatasetTag) -> Self {
        Self {
            namespace: tag.namespace,
            name: tag.name,
            version: tag.version,
        }
    }
}

/// Errors that can occur when listing datasets using a manifest
///
/// This enum represents all possible error conditions when handling
/// a request to list datasets that reference a specific manifest hash.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The manifest hash is invalid
    ///
    /// This occurs when:
    /// - The hash contains invalid characters or doesn't follow hash format conventions
    /// - The hash is empty or malformed
    /// - Path parameter extraction fails for the hash (deserialization error)
    #[error("invalid manifest hash: {0}")]
    InvalidHash(PathRejection),

    /// Manifest not found
    ///
    /// This occurs when:
    /// - No manifest exists with the given hash in the content-addressable store
    /// - The manifest has been deleted or moved
    /// - Manifest is not registered in the metadata database
    #[error("manifest '{hash}' not found")]
    ManifestNotFound { hash: Hash },

    /// Failed to list dataset tags
    ///
    /// This occurs when:
    /// - Database connection is lost
    /// - SQL query to retrieve dataset tags fails
    /// - Database schema inconsistencies prevent tag retrieval
    #[error("failed to list dataset tags: {0}")]
    ListDatasetTags(#[source] ListDatasetsUsingManifestError),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidHash(_) => "INVALID_HASH",
            Error::ManifestNotFound { .. } => "MANIFEST_NOT_FOUND",
            Error::ListDatasetTags(err) => match err {
                ListDatasetsUsingManifestError::MetadataDbQueryPath(_) => {
                    "QUERY_MANIFEST_PATH_ERROR"
                }
                ListDatasetsUsingManifestError::MetadataDbListTags(_) => "LIST_DATASET_TAGS_ERROR",
            },
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidHash(_) => StatusCode::BAD_REQUEST,
            Error::ManifestNotFound { .. } => StatusCode::NOT_FOUND,
            Error::ListDatasetTags(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

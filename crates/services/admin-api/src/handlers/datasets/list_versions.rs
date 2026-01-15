use amp_datasets_registry::error::{ListVersionTagsError, ResolveRevisionError};
use axum::{
    Json,
    extract::{Path, State, rejection::PathRejection},
    http::StatusCode,
};
use datasets_common::{name::Name, namespace::Namespace, version::Version};
use monitoring::logging;

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
};

/// Handler for the `GET /datasets/{namespace}/{name}/versions` endpoint
///
/// Returns all versions for a dataset with their metadata.
///
/// ## Response
/// - **200 OK**: Successfully retrieved version list
/// - **400 Bad Request**: Invalid path parameters
/// - **500 Internal Server Error**: Database query error
///
/// ## Error Codes
/// - `INVALID_PATH`: Invalid namespace or name in path parameters
/// - `LIST_VERSION_TAGS_ERROR`: Failed to list version tags from dataset store
/// - `RESOLVE_REVISION_ERROR`: Failed to resolve dev tag revision
///
/// ## Behavior
/// This endpoint returns comprehensive version information for a dataset:
/// - All semantic versions sorted in descending order (newest first)
/// - For each version: manifest hash, creation time, and last update time
/// - Special tags: "latest" (if any semantic versions exist) and "dev" (if set)
///
/// The "latest" tag is automatically managed and always points to the highest
/// semantic version. The "dev" tag is explicitly managed via the registration
/// endpoint and may point to any manifest hash.
///
/// Returns an empty list if the dataset has no registered versions.
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        get,
        path = "/datasets/{namespace}/{name}/versions",
        tag = "datasets",
        operation_id = "list_dataset_versions",
        params(
            ("namespace" = String, Path, description = "Dataset namespace"),
            ("name" = String, Path, description = "Dataset name")
        ),
        responses(
            (status = 200, description = "Successfully retrieved versions", body = VersionsResponse),
            (status = 400, description = "Invalid path parameters", body = ErrorResponse),
            (status = 500, description = "Internal server error", body = ErrorResponse)
        )
    )
)]
pub async fn handler(
    path: Result<Path<(Namespace, Name)>, PathRejection>,
    State(ctx): State<Ctx>,
) -> Result<Json<VersionsResponse>, ErrorResponse> {
    let (namespace, name) = match path {
        Ok(Path((namespace, name))) => (namespace, name),
        Err(err) => {
            tracing::debug!(error = %err, error_source = logging::error_source(&err), "invalid path parameters");
            return Err(Error::InvalidPath(err).into());
        }
    };

    tracing::debug!(
        dataset_namespace = %namespace,
        dataset_name = %name,
        "listing dataset versions"
    );

    // Query version tags with full details
    let version_tags = ctx
        .dataset_store
        .list_dataset_version_tags(&namespace, &name)
        .await
        .map_err(Error::ListVersionTags)?;

    // Convert tags to VersionInfo
    let versions: Vec<VersionInfo> = version_tags
        .into_iter()
        .filter_map(|tag| {
            let version_str = tag.version.into_inner();
            let hash_str = tag.hash.to_string();

            // Parse version string
            version_str
                .parse::<Version>()
                .ok()
                .map(|version| VersionInfo {
                    version,
                    manifest_hash: hash_str,
                    created_at: tag.created_at.to_rfc3339(),
                    updated_at: tag.updated_at.to_rfc3339(),
                })
        })
        .collect();

    // Get latest tag (should match the highest version if exists)
    let latest_version = versions.first().map(|v| v.version.clone());

    // Get dev tag hash
    let dev_hash = ctx
        .dataset_store
        .resolve_dev_version_hash(&namespace, &name)
        .await
        .map_err(Error::ResolveRevision)?
        .map(|hash| hash.into_inner());

    Ok(Json(VersionsResponse {
        namespace,
        name,
        versions,
        special_tags: SpecialTags {
            latest: latest_version,
            dev: dev_hash,
        },
    }))
}

/// Response for listing dataset versions
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct VersionsResponse {
    /// Dataset namespace
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub namespace: Namespace,
    /// Dataset name
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub name: Name,
    /// List of semantic versions (sorted descending)
    pub versions: Vec<VersionInfo>,
    /// Special tags (latest and dev)
    pub special_tags: SpecialTags,
}

/// Version information
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct VersionInfo {
    /// Semantic version
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub version: Version,
    /// Manifest hash for this version
    pub manifest_hash: String,
    /// When this version was created
    pub created_at: String,
    /// When this version was last updated
    pub updated_at: String,
}

/// Special tags pointing to versions or hashes
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct SpecialTags {
    /// Latest semantic version (if any)
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "utoipa", schema(value_type = Option<String>))]
    pub latest: Option<Version>,
    /// Dev tag pointing to manifest hash (if any)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dev: Option<String>,
}

/// Errors that can occur when listing versions
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid path parameters
    ///
    /// This occurs when:
    /// - The namespace or name in the URL path is invalid
    /// - Path parameter parsing fails
    #[error("Invalid path parameters: {0}")]
    InvalidPath(#[source] PathRejection),

    /// Dataset store operation error when listing version tags
    ///
    /// This occurs when:
    /// - Failed to query version tags from the dataset store
    /// - Database connection issues
    /// - Internal database errors
    #[error("Failed to list version tags: {0}")]
    ListVersionTags(#[source] ListVersionTagsError),

    /// Dataset store operation error when resolving revision
    ///
    /// This occurs when:
    /// - Failed to resolve dev tag revision
    /// - Database connection issues
    /// - Internal database errors
    #[error("Failed to resolve revision: {0}")]
    ResolveRevision(#[source] ResolveRevisionError),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidPath(_) => "INVALID_PATH",
            Error::ListVersionTags(_) => "LIST_VERSION_TAGS_ERROR",
            Error::ResolveRevision(_) => "RESOLVE_REVISION_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidPath(_) => StatusCode::BAD_REQUEST,
            Error::ListVersionTags(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::ResolveRevision(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

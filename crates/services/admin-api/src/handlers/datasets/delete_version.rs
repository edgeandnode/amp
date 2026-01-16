use amp_datasets_registry::error::{DeleteVersionTagError, ResolveRevisionError};
use axum::{
    extract::{Path, State, rejection::PathRejection},
    http::StatusCode,
};
use datasets_common::{name::Name, namespace::Namespace, version::Version};
use monitoring::logging;

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
};

/// Handler for the `DELETE /datasets/{namespace}/{name}/versions/{version}` endpoint
///
/// Removes a semantic version tag from a dataset.
///
/// ## Response
/// - **204 No Content**: Version successfully deleted (or didn't exist)
/// - **400 Bad Request**: Invalid path parameters or attempting to delete the "latest" version
/// - **500 Internal Server Error**: Database operation error
///
/// ## Error Codes
/// - `INVALID_PATH`: Invalid namespace, name, or version in path parameters
/// - `CANNOT_DELETE_LATEST_VERSION`: Cannot delete the version currently tagged as "latest"
/// - `RESOLVE_LATEST_REVISION_ERROR`: Failed to resolve the "latest" tag to its manifest hash
/// - `RESOLVE_VERSION_REVISION_ERROR`: Failed to resolve the requested version to its manifest hash
/// - `DELETE_VERSION_TAG_ERROR`: Failed to delete version tag from dataset store
///
/// ## Behavior
/// This endpoint removes a semantic version tag from a dataset. The deletion follows this flow:
///
/// 1. **Check version existence**: Resolves the requested version to its manifest hash.
///    If the version doesn't exist, returns 204 immediately (idempotent).
///
/// 2. **Check "latest" protection**: Resolves the "latest" tag to its manifest hash and compares
///    with the requested version's hash. If they point to the same manifest, deletion is rejected
///    with a 400 error. You must create a newer version first to update the "latest" tag.
///
/// 3. **Delete version tag**: Removes only the version tag from the database. The underlying
///    manifest file is never deleted (manifests are content-addressable and may be referenced
///    by other versions or datasets).
///
/// This operation is fully idempotent - it returns 204 even if the version doesn't exist.
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        delete,
        path = "/datasets/{namespace}/{name}/versions/{version}",
        tag = "datasets",
        operation_id = "delete_dataset_version",
        params(
            ("namespace" = String, Path, description = "Dataset namespace"),
            ("name" = String, Path, description = "Dataset name"),
            ("version" = String, Path, description = "Semantic version (e.g., 1.2.3)")
        ),
        responses(
            (status = 204, description = "Version successfully deleted"),
            (status = 400, description = "Invalid path parameters", body = ErrorResponse),
            (status = 500, description = "Internal server error", body = ErrorResponse)
        )
    )
)]
pub async fn handler(
    path: Result<Path<(Namespace, Name, Version)>, PathRejection>,
    State(ctx): State<Ctx>,
) -> Result<StatusCode, ErrorResponse> {
    let (namespace, name, version) = match path {
        Ok(Path((namespace, name, version))) => (namespace, name, version),
        Err(err) => {
            tracing::debug!(error = %err, error_source = logging::error_source(&err), "invalid path parameters");
            return Err(Error::InvalidPath(err).into());
        }
    };

    tracing::debug!(
        namespace=%namespace,
        name=%name,
        version=%version,
        "deleting dataset version"
    );

    // First check if this version exists
    let version_hash = ctx
        .datasets_registry
        .resolve_version_hash(&namespace, &name, &version)
        .await
        .map_err(Error::ResolveVersionRevision)?;

    // If version doesn't exist, return success (idempotent delete)
    let Some(ver_hash) = version_hash else {
        tracing::debug!(
            namespace=%namespace,
            name=%name,
            version=%version,
            "version does not exist, nothing to delete"
        );
        return Ok(StatusCode::NO_CONTENT);
    };

    // Check if this version is the current "latest" version
    let latest_hash = ctx
        .datasets_registry
        .resolve_latest_version_hash(&namespace, &name)
        .await
        .map_err(Error::ResolveLatestRevision)?;

    // If latest exists and points to the same manifest, reject deletion
    if let Some(latest_hash) = latest_hash
        && latest_hash == ver_hash
    {
        return Err(Error::CannotDeleteLatestVersion.into());
    }

    ctx.datasets_registry
        .delete_dataset_version_tag(&namespace, &name, &version)
        .await
        .map_err(Error::DeleteVersionTag)?;

    Ok(StatusCode::NO_CONTENT)
}

/// Errors that can occur when deleting a version
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid path parameters
    ///
    /// This occurs when:
    /// - The namespace, name, or version in the URL path is invalid
    /// - Path parameter parsing fails
    #[error("Invalid path parameters: {0}")]
    InvalidPath(#[source] PathRejection),
    /// Failed to resolve the "latest" revision
    ///
    /// This occurs when:
    /// - Failed to resolve which manifest the "latest" tag points to
    /// - Database connection issues
    /// - Internal database errors during resolution
    #[error("Failed to resolve latest revision: {0}")]
    ResolveLatestRevision(#[source] ResolveRevisionError),
    /// Failed to resolve the version revision
    ///
    /// This occurs when:
    /// - Failed to resolve which manifest the requested version points to
    /// - Database connection issues
    /// - Internal database errors during resolution
    #[error("Failed to resolve version revision: {0}")]
    ResolveVersionRevision(#[source] ResolveRevisionError),
    /// Cannot delete the version currently tagged as "latest"
    ///
    /// This occurs when:
    /// - Attempting to delete the version that is currently tagged as "latest"
    /// - Create a newer version first to update the "latest" tag
    #[error("Cannot delete version tagged as 'latest' - create a newer version first")]
    CannotDeleteLatestVersion,
    /// Dataset store operation error when deleting version tag
    ///
    /// This occurs when:
    /// - Failed to delete version tag from the dataset store
    /// - Database connection issues
    /// - Internal database errors during deletion
    #[error("Failed to delete version tag: {0}")]
    DeleteVersionTag(#[source] DeleteVersionTagError),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidPath(_) => "INVALID_PATH",
            Error::CannotDeleteLatestVersion => "CANNOT_DELETE_LATEST_VERSION",
            Error::ResolveLatestRevision(_) => "RESOLVE_LATEST_REVISION_ERROR",
            Error::ResolveVersionRevision(_) => "RESOLVE_VERSION_REVISION_ERROR",
            Error::DeleteVersionTag(_) => "DELETE_VERSION_TAG_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidPath(_) => StatusCode::BAD_REQUEST,
            Error::CannotDeleteLatestVersion => StatusCode::BAD_REQUEST,
            Error::ResolveLatestRevision(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::ResolveVersionRevision(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::DeleteVersionTag(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

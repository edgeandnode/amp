use axum::{
    extract::{Path, State, rejection::PathRejection},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use datasets_common::{name::Name, version_tag::VersionTag};

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
};

/// Handler for the `GET /datasets/{name}/versions/{version}/manifest` endpoint
///
/// Retrieves the raw manifest JSON for a specific version of a dataset.
///
/// ## Path Parameters
/// - `name`: Dataset name (validated identifier)
/// - `version`: Dataset version using semantic versioning (e.g., "1.0.0")
///
/// ## Response
/// - **200 OK**: Returns the raw manifest JSON
/// - **400 Bad Request**: Invalid dataset name or version format
/// - **404 Not Found**: Dataset with the given name/version does not exist
/// - **500 Internal Server Error**: Dataset store error
///
/// ## Error Codes
/// - `INVALID_SELECTOR`: The provided dataset name or version is not valid (invalid name format, malformed version, or parsing error)
/// - `MANIFEST_NOT_FOUND`: No manifest exists with the given name/version
/// - `MANIFEST_RETRIEVAL_ERROR`: Failed to retrieve manifest from the dataset manifests store
///
/// This handler:
/// - Validates and extracts the dataset name and version from the URL path
/// - Retrieves the raw manifest JSON directly from the dataset manifests store
/// - Returns the manifest as a JSON response with proper Content-Type header
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        get,
        path = "/datasets/{name}/versions/{version}/manifest",
        tag = "datasets",
        operation_id = "datasets_get_manifest",
        params(
            ("name" = String, Path, description = "Dataset name"),
            ("version" = String, Path, description = "Dataset version")
        ),
        responses(
            (status = 200, description = "Successfully retrieved manifest JSON", content_type = "application/json", body = String),
            (status = 400, description = "Invalid dataset name or version", body = crate::handlers::error::ErrorResponse),
            (status = 404, description = "Manifest not found", body = crate::handlers::error::ErrorResponse),
            (status = 500, description = "Internal server error", body = crate::handlers::error::ErrorResponse)
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    path: Result<Path<(Name, VersionTag)>, PathRejection>,
) -> Result<Response, ErrorResponse> {
    let (name, version) = match path {
        Ok(Path((name, version))) => (name, version),
        Err(err) => {
            tracing::debug!(error=?err, "invalid dataset path parameters");
            return Err(Error::InvalidSelector(err).into());
        }
    };

    tracing::debug!(
        dataset_name=%name,
        dataset_version=%version,
        "retrieving manifest from store"
    );

    // Get the raw manifest JSON from the dataset manifests store
    let manifest_content = match ctx.dataset_manifests_store.get(&name, &version).await {
        Ok(Some(content)) => content,
        Ok(None) => {
            tracing::debug!(
                dataset_name=%name,
                dataset_version=%version,
                "manifest not found"
            );
            return Err(Error::NotFound {
                name: name.clone(),
                version: version.clone(),
            }
            .into());
        }
        Err(err) => {
            tracing::debug!(
                dataset_name=%name,
                dataset_version=%version,
                error=?err,
                "failed to retrieve manifest"
            );
            return Err(Error::ManifestRetrievalError(err).into());
        }
    };

    tracing::debug!(
        dataset_name=%name,
        dataset_version=%version,
        "manifest retrieved successfully"
    );

    // Return the raw JSON with proper Content-Type header
    Ok((
        [(header::CONTENT_TYPE, "application/json")],
        manifest_content.into_json_string(),
    )
        .into_response())
}

/// Errors that can occur during manifest retrieval
///
/// This enum represents all possible error conditions when handling
/// a request to retrieve the manifest for a specific dataset by name and version.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The dataset selector is invalid
    ///
    /// This occurs when:
    /// - The dataset name contains invalid characters or doesn't follow naming conventions
    /// - The dataset name is empty or malformed
    /// - The version syntax is invalid (e.g., malformed semver)
    /// - Path parameter extraction fails for dataset selection
    #[error("invalid dataset selector: {0}")]
    InvalidSelector(PathRejection),

    /// Manifest not found
    ///
    /// This occurs when:
    /// - No manifest exists with the given name/version
    /// - The dataset has been deleted or moved
    /// - Dataset configuration is missing
    #[error("manifest '{name}' version '{version}' not found")]
    NotFound { name: Name, version: VersionTag },

    /// Manifest retrieval error from the dataset manifests store
    ///
    /// This occurs when:
    /// - The dataset manifests store is not accessible
    /// - There's a configuration error in the store
    /// - I/O errors while reading manifest files
    /// - Unsupported manifest file format
    #[error("manifest retrieval error: {0}")]
    ManifestRetrievalError(#[source] dataset_store::manifests::GetError),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidSelector(_) => "INVALID_SELECTOR",
            Error::NotFound { .. } => "MANIFEST_NOT_FOUND",
            Error::ManifestRetrievalError(_) => "MANIFEST_RETRIEVAL_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidSelector(_) => StatusCode::BAD_REQUEST,
            Error::NotFound { .. } => StatusCode::NOT_FOUND,
            Error::ManifestRetrievalError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

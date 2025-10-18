use axum::{
    extract::{Path, State, rejection::PathRejection},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use datasets_common::{name::Name, namespace::Namespace, version::Version};

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
};

/// Handler for the `GET /datasets/{name}/versions/{version}/manifest` endpoint
///
/// Retrieves the raw manifest JSON for a specific version of a dataset using a
/// two-step resolution process: first resolving the manifest hash from the metadata
/// database, then fetching the manifest content from the object store.
///
/// ## Path Parameters
/// - `name`: Dataset name (validated identifier)
/// - `version`: Dataset version using semantic versioning (e.g., "1.0.0")
///
/// ## Response
/// - **200 OK**: Returns the raw manifest JSON
/// - **400 Bad Request**: Invalid dataset name or version format
/// - **404 Not Found**: Dataset with the given name/version does not exist
/// - **500 Internal Server Error**: Database or object store error
///
/// ## Error Codes
/// - `INVALID_SELECTOR`: The provided dataset name or version is not valid (invalid name format, malformed version, or parsing error)
/// - `MANIFEST_NOT_FOUND`: No manifest exists with the given name/version
/// - `HASH_RESOLUTION_ERROR`: Failed to resolve manifest hash from the metadata database
/// - `MANIFEST_RETRIEVAL_ERROR`: Failed to retrieve manifest content from the object store
///
/// ## Resolution Flow
/// 1. Validates and extracts the dataset name and version from the URL path
/// 2. Resolves the manifest hash from the metadata database using namespace+name+version
/// 3. Fetches the manifest content from the object store using the resolved hash
/// 4. Returns the manifest as a JSON response with proper Content-Type header
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
    path: Result<Path<(Name, Version)>, PathRejection>,
) -> Result<Response, ErrorResponse> {
    let (name, version) = match path {
        Ok(Path((name, version))) => (name, version),
        Err(err) => {
            tracing::debug!(error=?err, "invalid dataset path parameters");
            return Err(Error::InvalidSelector(err).into());
        }
    };

    // TODO: Pass the actual namespace instead of using a placeholder
    let namespace = "_"
        .parse::<Namespace>()
        .expect("'_' should be a valid namespace");

    tracing::debug!(
        dataset_namespace=%namespace,
        dataset_name=%name,
        dataset_version=%version,
        "retrieving manifest from store"
    );

    // Resolve the manifest hash from namespace+name+version
    let manifest_hash = match ctx
        .metadata_db
        .resolve_tag_hash(&namespace, &name, &version)
        .await
    {
        Ok(Some(hash)) => hash.into(),
        Ok(None) => {
            tracing::debug!(
                dataset_namespace=%namespace,
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
                dataset_namespace=%namespace,
                dataset_name=%name,
                dataset_version=%version,
                error=?err,
                "failed to resolve manifest hash"
            );
            return Err(Error::HashResolutionError(err).into());
        }
    };

    // Fetch the manifest content by hash
    let manifest_content = match ctx.dataset_manifests_store.get(&manifest_hash).await {
        Ok(Some(content)) => content,
        Ok(None) => {
            tracing::debug!(
                dataset_namespace=%namespace,
                dataset_name=%name,
                dataset_version=%version,
                manifest_hash=%manifest_hash,
                "manifest content not found by hash"
            );
            return Err(Error::NotFound {
                name: name.clone(),
                version: version.clone(),
            }
            .into());
        }
        Err(err) => {
            tracing::debug!(
                dataset_namespace=%namespace,
                dataset_name=%name,
                dataset_version=%version,
                manifest_hash=%manifest_hash,
                error=?err,
                "failed to retrieve manifest content"
            );
            return Err(Error::ManifestRetrievalError(err).into());
        }
    };

    tracing::debug!(
        dataset_namespace=%namespace,
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
    NotFound { name: Name, version: Version },

    /// Hash resolution error from the metadata database
    ///
    /// This occurs when:
    /// - The metadata database is not accessible
    /// - There's a database query error
    #[error("hash resolution error: {0}")]
    HashResolutionError(#[source] metadata_db::Error),

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
            Error::HashResolutionError(_) => "HASH_RESOLUTION_ERROR",
            Error::ManifestRetrievalError(_) => "MANIFEST_RETRIEVAL_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidSelector(_) => StatusCode::BAD_REQUEST,
            Error::NotFound { .. } => StatusCode::NOT_FOUND,
            Error::HashResolutionError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::ManifestRetrievalError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

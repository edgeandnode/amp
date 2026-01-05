use axum::{
    Json,
    extract::{Path, State, rejection::PathRejection},
    http::StatusCode,
};
use datasets_common::{name::Name, namespace::Namespace, reference::Reference, revision::Revision};
use monitoring::logging;
use serde_json::Value as JsonValue;

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
};

/// Handler for the `GET /datasets/{namespace}/{name}/versions/{revision}/manifest` endpoint
///
/// Retrieves the raw manifest JSON for the specified dataset revision.
///
/// ## Response
/// - **200 OK**: Successfully retrieved manifest
/// - **404 Not Found**: Dataset, revision, or manifest not found
/// - **500 Internal Server Error**: Database or object store error
///
/// ## Error Codes
/// - `INVALID_PATH`: Invalid namespace, name, or revision in path parameters
/// - `DATASET_NOT_FOUND`: The specified dataset or revision does not exist
/// - `MANIFEST_NOT_FOUND`: The manifest file was not found in object storage
/// - `RESOLVE_REVISION_ERROR`: Failed to resolve revision to manifest hash
/// - `GET_MANIFEST_PATH_ERROR`: Failed to query manifest path from metadata database
/// - `READ_MANIFEST_ERROR`: Failed to read manifest file from object store
/// - `PARSE_MANIFEST_ERROR`: Failed to parse manifest JSON
///
/// ## Behavior
/// This endpoint returns the raw manifest JSON document for a dataset revision.
/// The revision parameter supports four types:
/// - Semantic version (e.g., "1.2.3")
/// - Manifest hash (SHA256 hash)
/// - "latest" - resolves to the highest semantic version
/// - "dev" - resolves to the development version
///
/// The endpoint first resolves the revision to a manifest hash, then retrieves
/// the manifest JSON from object storage. Manifests are immutable and
/// content-addressable, identified by their SHA256 hash.
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        get,
        path = "/datasets/{namespace}/{name}/versions/{revision}/manifest",
        tag = "datasets",
        operation_id = "get_dataset_manifest",
        params(
            ("namespace" = String, Path, description = "Dataset namespace"),
            ("name" = String, Path, description = "Dataset name"),
            ("revision" = String, Path, description = "Revision (version, hash, latest, or dev)")
        ),
        responses(
            (status = 200, description = "Successfully retrieved manifest", body = JsonValue),
            (status = 404, description = "Dataset or revision not found", body = ErrorResponse),
            (status = 500, description = "Internal server error", body = ErrorResponse)
        )
    )
)]
pub async fn handler(
    path: Result<Path<(Namespace, Name, Revision)>, PathRejection>,
    State(ctx): State<Ctx>,
) -> Result<Json<JsonValue>, ErrorResponse> {
    let reference = match path {
        Ok(Path((namespace, name, revision))) => Reference::new(namespace, name, revision),
        Err(err) => {
            tracing::debug!(error = %err, error_source = logging::error_source(&err), "invalid path parameters");
            return Err(Error::InvalidPath(err).into());
        }
    };

    tracing::debug!(dataset_reference = %reference, "retrieving dataset manifest");

    let namespace = reference.namespace().clone();
    let name = reference.name().clone();
    let revision = reference.revision().clone();

    // Resolve the revision to a manifest hash
    let reference = ctx
        .dataset_store
        .resolve_revision(&reference)
        .await
        .map_err(Error::ResolveRevision)?
        .ok_or_else(|| Error::NotFound {
            namespace: namespace.clone(),
            name: name.clone(),
            revision: revision.clone(),
        })?;

    // Load manifest content
    let manifest_content = ctx
        .dataset_store
        .get_manifest(reference.hash())
        .await
        .map_err(|err| match err {
            amp_dataset_store::GetManifestError::MetadataDbQueryPath(_) => {
                Error::GetManifestPath(err)
            }
            amp_dataset_store::GetManifestError::ObjectStoreError(_) => Error::ReadManifest(err),
        })?
        .ok_or_else(|| Error::ManifestNotFound {
            hash: reference.hash().to_string(),
        })?;

    // Parse manifest JSON and return it
    let manifest_json: JsonValue = manifest_content
        .try_into_manifest()
        .map_err(Error::ParseManifest)?;

    Ok(Json(manifest_json))
}

/// Errors that can occur when getting a manifest
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid path parameters
    ///
    /// This occurs when:
    /// - The namespace, name, or revision in the URL path is invalid
    /// - Path parameter parsing fails
    #[error("Invalid path parameters: {0}")]
    InvalidPath(#[source] PathRejection),
    /// Dataset or revision not found
    ///
    /// This occurs when:
    /// - The specified dataset name doesn't exist in the namespace
    /// - The specified revision doesn't exist for this dataset
    /// - The revision resolves to a manifest that doesn't exist
    #[error("Dataset '{namespace}/{name}' at revision '{revision}' not found")]
    NotFound {
        namespace: Namespace,
        name: Name,
        revision: Revision,
    },
    /// Manifest not found
    ///
    /// This occurs when:
    /// - The manifest file doesn't exist in object storage
    /// - The manifest was registered but never uploaded
    /// - The manifest hash is invalid or corrupted
    #[error("Manifest with hash '{hash}' not found")]
    ManifestNotFound { hash: String },
    /// Failed to resolve revision to manifest hash
    ///
    /// This occurs when:
    /// - Failed to query metadata database for revision information
    /// - Database connection issues
    /// - Internal database errors during revision resolution
    #[error("Failed to resolve revision: {0}")]
    ResolveRevision(#[source] amp_dataset_store::ResolveRevisionError),
    /// Failed to query manifest path from metadata database
    ///
    /// This occurs when:
    /// - Failed to query manifest file path from metadata database
    /// - Database connection issues
    /// - Internal database errors
    #[error("Failed to query manifest path: {0}")]
    GetManifestPath(#[source] amp_dataset_store::GetManifestError),
    /// Failed to read manifest from object store
    ///
    /// This occurs when:
    /// - Failed to read manifest file from object storage
    /// - Object store connection issues
    /// - Permissions issues accessing object store
    /// - Network errors
    #[error("Failed to read manifest from object store: {0}")]
    ReadManifest(#[source] amp_dataset_store::GetManifestError),
    /// Failed to parse manifest JSON
    ///
    /// This occurs when:
    /// - Manifest file contains invalid JSON
    /// - Manifest structure doesn't match expected schema
    /// - Required fields are missing
    #[error("Failed to parse manifest: {0}")]
    ParseManifest(#[source] amp_dataset_store::ManifestParseError),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidPath(_) => "INVALID_PATH",
            Error::NotFound { .. } => "DATASET_NOT_FOUND",
            Error::ManifestNotFound { .. } => "MANIFEST_NOT_FOUND",
            Error::ResolveRevision(_) => "RESOLVE_REVISION_ERROR",
            Error::GetManifestPath(_) => "GET_MANIFEST_PATH_ERROR",
            Error::ReadManifest(_) => "READ_MANIFEST_ERROR",
            Error::ParseManifest(_) => "PARSE_MANIFEST_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidPath(_) => StatusCode::BAD_REQUEST,
            Error::NotFound { .. } => StatusCode::NOT_FOUND,
            Error::ManifestNotFound { .. } => StatusCode::NOT_FOUND,
            Error::ResolveRevision(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::GetManifestPath(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::ReadManifest(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::ParseManifest(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

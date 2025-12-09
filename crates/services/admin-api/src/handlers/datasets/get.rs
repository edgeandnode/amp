use axum::{
    Json,
    extract::{Path, State, rejection::PathRejection},
    http::StatusCode,
};
use datasets_common::{name::Name, namespace::Namespace, reference::Reference, revision::Revision};
use monitoring::logging;

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
};

/// Handler for the `GET /datasets/{namespace}/{name}/versions/{revision}` endpoint
///
/// Returns detailed dataset information for the specified revision.
///
/// ## Response
/// - **200 OK**: Successfully retrieved dataset information
/// - **400 Bad Request**: Invalid path parameters
/// - **404 Not Found**: Dataset or revision not found
/// - **500 Internal Server Error**: Database or dataset store error
///
/// ## Error Codes
/// - `INVALID_PATH`: Invalid namespace, name, or revision in path parameters
/// - `DATASET_NOT_FOUND`: The specified dataset or revision does not exist
/// - `RESOLVE_REVISION_ERROR`: Failed to resolve revision to manifest hash
/// - `GET_MANIFEST_PATH_ERROR`: Failed to query manifest path from metadata database
/// - `READ_MANIFEST_ERROR`: Failed to read manifest file from object store
/// - `PARSE_MANIFEST_ERROR`: Failed to parse manifest JSON
///
/// ## Behavior
/// This endpoint retrieves detailed information about a specific dataset revision.
/// The revision parameter supports four types:
/// - Semantic version (e.g., "1.2.3")
/// - Manifest hash (SHA256 hash)
/// - "latest" - resolves to the highest semantic version
/// - "dev" - resolves to the development version
///
/// The endpoint first resolves the revision to a manifest hash, then returns
/// basic dataset information including namespace, name, revision, manifest hash, and kind.
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        get,
        path = "/datasets/{namespace}/{name}/versions/{revision}",
        tag = "datasets",
        operation_id = "get_dataset_by_revision",
        params(
            ("namespace" = String, Path, description = "Dataset namespace"),
            ("name" = String, Path, description = "Dataset name"),
            ("revision" = String, Path, description = "Revision (version, hash, latest, or dev)")
        ),
        responses(
            (status = 200, description = "Successfully retrieved dataset", body = DatasetInfo),
            (status = 400, description = "Invalid path parameters", body = ErrorResponse),
            (status = 404, description = "Dataset or revision not found", body = ErrorResponse),
            (status = 500, description = "Internal server error", body = ErrorResponse)
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    path: Result<Path<(Namespace, Name, Revision)>, PathRejection>,
) -> Result<Json<DatasetInfo>, ErrorResponse> {
    let reference = match path {
        Ok(Path((namespace, name, revision))) => Reference::new(namespace, name, revision),
        Err(err) => {
            tracing::debug!(error = %err, error_source = logging::error_source(&err), "invalid path parameters");
            return Err(Error::InvalidPath(err).into());
        }
    };

    tracing::debug!(dataset_reference = %reference, "retrieving dataset information");

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
            dataset_store::GetManifestError::MetadataDbQueryPath(_) => Error::GetManifestPath(err),
            dataset_store::GetManifestError::ObjectStoreError(_) => Error::ReadManifest(err),
        })?
        .ok_or_else(|| Error::NotFound {
            namespace: namespace.clone(),
            name: name.clone(),
            revision: revision.clone(),
        })?;

    // Parse manifest to determine kind
    let manifest: datasets_common::manifest::Manifest = manifest_content
        .try_into_manifest()
        .map_err(Error::ParseManifest)?;

    Ok(Json(DatasetInfo {
        namespace,
        name,
        revision,
        manifest_hash: reference.hash().to_string(),
        kind: manifest.kind,
    }))
}

/// Detailed dataset information
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct DatasetInfo {
    /// Dataset namespace
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub namespace: Namespace,
    /// Dataset name
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub name: Name,
    /// Revision requested
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub revision: Revision,
    /// Manifest hash
    pub manifest_hash: String,
    /// Dataset kind
    pub kind: String,
    // TODO: Add tables and other dataset details
}

/// Errors that can occur when getting a dataset
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
    /// Failed to resolve revision to manifest hash
    ///
    /// This occurs when:
    /// - Failed to query metadata database for revision information
    /// - Database connection issues
    /// - Internal database errors during revision resolution
    #[error("Failed to resolve revision: {0}")]
    ResolveRevision(#[source] dataset_store::ResolveRevisionError),
    /// Failed to query manifest path from metadata database
    ///
    /// This occurs when:
    /// - Failed to query manifest file path from metadata database
    /// - Database connection issues
    /// - Internal database errors
    #[error("Failed to query manifest path: {0}")]
    GetManifestPath(#[source] dataset_store::GetManifestError),
    /// Failed to read manifest from object store
    ///
    /// This occurs when:
    /// - Failed to read manifest file from object storage
    /// - Object store connection issues
    /// - Permissions issues accessing object store
    /// - Network errors
    #[error("Failed to read manifest from object store: {0}")]
    ReadManifest(#[source] dataset_store::GetManifestError),
    /// Failed to parse manifest JSON
    ///
    /// This occurs when:
    /// - Manifest file contains invalid JSON
    /// - Manifest structure doesn't match expected schema
    /// - Required fields are missing
    #[error("Failed to parse manifest: {0}")]
    ParseManifest(#[source] dataset_store::ManifestParseError),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidPath(_) => "INVALID_PATH",
            Error::NotFound { .. } => "DATASET_NOT_FOUND",
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
            Error::ResolveRevision(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::GetManifestPath(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::ReadManifest(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::ParseManifest(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

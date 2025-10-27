//! Manifests get by ID handler

use axum::{
    Json,
    extract::{Path, State, rejection::PathRejection},
    http::StatusCode,
};
use dataset_store::manifests::ManifestContent;
use datasets_common::hash::Hash;
use serde_json::value::RawValue as JsonRawValue;

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
};

/// Handler for the `GET /manifests/{hash}` endpoint
///
/// Retrieves the raw manifest JSON for a specific manifest hash.
///
/// ## Path Parameters
/// - `hash`: Manifest content hash (validated hash format)
///
/// ## Response
/// - **200 OK**: Returns the raw manifest JSON
/// - **400 Bad Request**: Invalid manifest hash format
/// - **404 Not Found**: Manifest with the given hash does not exist
/// - **500 Internal Server Error**: Manifest retrieval error
///
/// ## Error Codes
/// - `INVALID_HASH`: The provided hash is not valid (invalid hash format or parsing error)
/// - `MANIFEST_NOT_FOUND`: No manifest exists with the given hash
/// - `MANIFEST_RETRIEVAL_ERROR`: Failed to retrieve manifest from the dataset manifests store
///
/// ## Retrieval Process
/// This handler retrieves manifests from content-addressable storage:
/// - The dataset manifests store queries the metadata database internally to resolve the hash to a file path
/// - Then fetches the manifest content from the object store
///
/// This handler:
/// - Validates and extracts the manifest hash from the URL path
/// - Retrieves the raw manifest JSON from the dataset manifests store using the hash
/// - Returns the manifest as a JSON response with proper Content-Type header
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        get,
        path = "/manifests/{hash}",
        tag = "manifests",
        operation_id = "manifests_get_by_hash",
        params(
            ("hash" = String, Path, description = "Manifest content hash")
        ),
        responses(
            (status = 200, description = "Successfully retrieved manifest JSON (schema varies by manifest kind)", body = ManifestResponse),
            (status = 400, description = "Invalid manifest hash", body = crate::handlers::error::ErrorResponse),
            (status = 404, description = "Manifest not found", body = crate::handlers::error::ErrorResponse),
            (status = 500, description = "Manifest retrieval error", body = crate::handlers::error::ErrorResponse)
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    path: Result<Path<Hash>, PathRejection>,
) -> Result<Json<ManifestResponse>, ErrorResponse> {
    let hash = match path {
        Ok(Path(hash)) => hash,
        Err(err) => {
            tracing::debug!(error=?err, "invalid manifest hash in path");
            return Err(Error::InvalidHash(err).into());
        }
    };

    tracing::debug!(
        manifest_hash=%hash,
        "retrieving manifest from store"
    );

    // Get the raw manifest JSON using the dataset store
    let manifest_content = match ctx.dataset_store.get_manifest(&hash).await {
        Ok(Some(content)) => content,
        Ok(None) => {
            tracing::debug!(
                manifest_hash=%hash,
                "manifest not found"
            );
            return Err(Error::NotFound { hash }.into());
        }
        Err(dataset_store::GetManifestError::MetadataDbQueryPath(err)) => {
            tracing::error!(
                manifest_hash=%hash,
                error=?err,
                "failed to query manifest path from metadata database"
            );
            return Err(Error::MetadataDbQueryError(err).into());
        }
        Err(dataset_store::GetManifestError::ObjectStoreError(err)) => {
            tracing::error!(
                manifest_hash=%hash,
                error=?err,
                "failed to retrieve manifest from object store"
            );
            return Err(Error::ObjectStoreReadError(err).into());
        }
    };

    tracing::debug!(
        manifest_hash=%hash,
        "manifest retrieved successfully"
    );

    // Return the raw JSON with proper Content-Type header
    Ok(Json(manifest_content.into()))
}

/// Response wrapper for manifest content
#[derive(Debug, serde::Serialize)]
#[serde(transparent)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "utoipa", schema(value_type = Object))]
pub struct ManifestResponse(Box<JsonRawValue>);

impl From<ManifestContent> for ManifestResponse {
    fn from(value: ManifestContent) -> Self {
        let json_str = value.into_json_string();
        let raw_value = JsonRawValue::from_string(json_str)
            .expect("ManifestContent should always contain valid JSON");
        Self(raw_value)
    }
}

/// Errors that can occur during manifest retrieval
///
/// This enum represents all possible error conditions when handling
/// a request to retrieve a manifest by its content hash.
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
    /// - Manifest is registered in metadata DB but file is missing from object store
    #[error("manifest '{hash}' not found")]
    NotFound { hash: Hash },

    /// Failed to query manifest path from metadata database
    ///
    /// This occurs when:
    /// - Database connection is lost
    /// - SQL query to resolve manifest hash to file path fails
    /// - Database schema inconsistencies prevent path lookup
    #[error("failed to query manifest path from metadata database: {0}")]
    MetadataDbQueryError(#[source] metadata_db::Error),

    /// Failed to read manifest from object store
    ///
    /// This occurs when:
    /// - Object store is not accessible or connection fails
    /// - Read permissions are insufficient
    /// - Network errors prevent reading from remote storage
    /// - File corruption or invalid data in the object store
    #[error("failed to read manifest from object store: {0}")]
    ObjectStoreReadError(#[source] dataset_store::manifests::GetError),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidHash(_) => "INVALID_HASH",
            Error::NotFound { .. } => "MANIFEST_NOT_FOUND",
            Error::MetadataDbQueryError(_) => "MANIFEST_METADATA_DB_ERROR",
            Error::ObjectStoreReadError(_) => "MANIFEST_OBJECT_STORE_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidHash(_) => StatusCode::BAD_REQUEST,
            Error::NotFound { .. } => StatusCode::NOT_FOUND,
            Error::MetadataDbQueryError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::ObjectStoreReadError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

use axum::{Json, extract::State, http::StatusCode};
use datasets_common::hash::Hash;

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
};

/// Handler for the `GET /manifests` endpoint
///
/// Returns all registered manifests in the system.
///
/// ## Response
/// - **200 OK**: Successfully retrieved all manifests
/// - **500 Internal Server Error**: Database query error
///
/// ## Error Codes
/// - `LIST_ALL_MANIFESTS_ERROR`: Failed to list all manifests from metadata database
///
/// ## Behavior
/// This handler returns a comprehensive list of all manifests registered in the system.
/// For each manifest, it includes:
/// - The content-addressable hash (SHA-256)
/// - The object store path where the manifest is stored
///
/// Results are ordered by hash (lexicographical).
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        get,
        path = "/manifests",
        tag = "manifests",
        operation_id = "list_all_manifests",
        responses(
            (status = 200, description = "Successfully retrieved all manifests", body = ManifestsResponse),
            (status = 500, description = "Internal server error", body = crate::handlers::error::ErrorResponse)
        )
    )
)]
pub async fn handler(State(ctx): State<Ctx>) -> Result<Json<ManifestsResponse>, ErrorResponse> {
    // Query all manifests from dataset store
    let manifests_data = ctx
        .dataset_store
        .list_all_manifests()
        .await
        .map_err(Error::ListAllManifests)?;

    // Convert metadata-db types to response types
    let manifests = manifests_data
        .into_iter()
        .map(|summary| ManifestInfo {
            hash: Hash::from(summary.hash),
            dataset_count: summary.dataset_count as u64,
        })
        .collect();

    Ok(Json(ManifestsResponse { manifests }))
}

/// Response for listing all manifests
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct ManifestsResponse {
    /// List of all manifests in the system
    pub manifests: Vec<ManifestInfo>,
}

/// Summary information for a single manifest
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct ManifestInfo {
    /// Content-addressable hash (SHA-256)
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub hash: Hash,
    /// Number of datasets using this manifest
    pub dataset_count: u64,
}

/// Errors that can occur when listing manifests
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Metadata database operation error when listing all manifests
    ///
    /// This occurs when:
    /// - Failed to query all manifests from the metadata database
    /// - Database connection issues
    /// - Internal database errors
    #[error("Failed to list all manifests: {0}")]
    ListAllManifests(#[source] amp_datasets_registry::error::ListAllManifestsError),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::ListAllManifests(_) => "LIST_ALL_MANIFESTS_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::ListAllManifests(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

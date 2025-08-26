use std::sync::Arc;

use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use common::manifest::Manifest;
use dataset_store::DatasetStore;
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::ServiceState;

#[derive(Debug, Deserialize, Serialize)]
pub struct RegisterRequest {
    pub manifest: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterResponse {
    pub success: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum RegisterManifestError {
    #[error("Dataset already exists: {0} version {1}")]
    DatasetAlreadyExists(String, String),
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),
    #[error("Dataset store error: {0}")]
    DatasetStoreError(String),
    #[error("Database error: {0}")]
    DatabaseError(#[from] metadata_db::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum RegisterError {
    #[error("Invalid manifest: {0}")]
    InvalidManifest(#[from] serde_json::Error),
    #[error("Registration failed: {0}")]
    RegistrationFailed(#[from] RegisterManifestError),
}

impl IntoResponse for RegisterError {
    fn into_response(self) -> axum::response::Response {
        let status_code = match &self {
            RegisterError::InvalidManifest(_) => StatusCode::BAD_REQUEST,
            RegisterError::RegistrationFailed(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let body = serde_json::json!({
            "error": self.to_string(),
        });
        (status_code, axum::Json(body)).into_response()
    }
}

#[instrument(skip_all, err)]
pub async fn register_handler(
    State(state): State<Arc<ServiceState>>,
    Json(payload): Json<RegisterRequest>,
) -> Result<Json<RegisterResponse>, RegisterError> {
    let manifest: Manifest = serde_json::from_str(&payload.manifest)?;
    register_manifest(&state.dataset_store, &manifest).await?;
    Ok(Json(RegisterResponse { success: true }))
}

#[instrument(skip_all)]
pub async fn register_manifest(
    dataset_store: &Arc<DatasetStore>,
    manifest: &Manifest,
) -> Result<(), RegisterManifestError> {
    let dataset_name = manifest.name.clone();
    let version = manifest.version.0.to_string();

    // Check if the dataset with the given name and version already exists in the registry.
    if dataset_store
        .metadata_db
        .dataset_exists(&dataset_name, &version)
        .await?
    {
        return Err(RegisterManifestError::DatasetAlreadyExists(
            dataset_name,
            version,
        ));
    }
    let registry_info = manifest.extract_registry_info();
    let manifest_json = serde_json::to_string(&manifest)?;
    let dataset_defs_store = dataset_store.dataset_defs_store();
    let manifest_path = object_store::path::Path::from(registry_info.manifest.clone());
    dataset_defs_store
        .prefixed_store()
        .put(&manifest_path, manifest_json.into())
        .await
        .map_err(|e| RegisterManifestError::DatasetStoreError(e.to_string()))?;
    dataset_store
        .metadata_db
        .register_dataset(registry_info)
        .await?;
    tracing::info!(
        "Successfully registered manifest '{}' version '{}'",
        dataset_name,
        version
    );
    Ok(())
}

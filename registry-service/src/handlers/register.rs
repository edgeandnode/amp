use axum::{Json, extract::State};
use common::manifest::Manifest;
use dataset_store::DatasetStore;
use http_common::BoxRequestError;
use metadata_db::MetadataDb;
use serde::{Deserialize, Serialize};
use tracing::instrument;

use super::error::Error;
use crate::ServiceState;

#[derive(Debug, Deserialize, Serialize)]
pub struct RegisterRequest {
    pub manifest: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterResponse {
    pub success: bool,
}

#[instrument(skip_all, err)]
pub async fn register_handler(
    State(ctx): State<ServiceState>,
    Json(payload): Json<RegisterRequest>,
) -> Result<Json<RegisterResponse>, BoxRequestError> {
    let manifest: Manifest =
        serde_json::from_str(&payload.manifest).map_err(|e| Error::InvalidManifest {
            source: Box::new(e),
        })?;
    register_manifest(&ctx.dataset_store, &ctx.metadata_db, &manifest).await?;
    Ok(Json(RegisterResponse { success: true }))
}

#[instrument(skip_all)]
pub async fn register_manifest(
    dataset_store: &DatasetStore,
    metadata_db: &MetadataDb,
    manifest: &Manifest,
) -> Result<(), Error> {
    let dataset_name = manifest.name.clone();
    let version = manifest.version.0.to_string();

    // Check if the dataset with the given name and version already exists in the registry.
    if metadata_db.dataset_exists(&dataset_name, &version).await? {
        return Err(Error::DatasetAlreadyExists {
            name: dataset_name,
            version,
        });
    }
    let registry_info = manifest.extract_registry_info();
    let manifest_json = serde_json::to_string(&manifest)?;
    let dataset_defs_store = dataset_store.dataset_defs_store();
    let manifest_path = object_store::path::Path::from(registry_info.manifest.clone());
    dataset_defs_store
        .prefixed_store()
        .put(&manifest_path, manifest_json.into())
        .await
        .map_err(|e| Error::DatasetStoreError(e.to_string()))?;
    metadata_db.register_dataset(registry_info).await?;
    tracing::info!(
        "Successfully registered manifest '{}' version '{}'",
        dataset_name,
        version
    );
    Ok(())
}

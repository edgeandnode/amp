//! Dataset deploy handler
use std::str::FromStr;

use axum::{Json, extract::State, http::StatusCode};
use common::manifest::{self, Manifest, Version};
use dataset_store::DatasetDefsCommon;
use http_common::BoxRequestError;
use object_store::path::Path;
use registry_service::handlers::register::register_manifest;

use super::error::Error;
use crate::{ctx::Ctx, handlers::common::validate_dataset_name};

/// Handler for the `POST /datasets` and `POST /deploy` endpoints
///
/// Deploys a new dataset configuration to the system. Accepts a JSON payload
/// containing the dataset deployment configuration.
///
/// ## Request Body
/// - `dataset_name`: Name of the dataset to be deployed (must be valid dataset name)
/// - `version`: Version of the dataset to deploy (must be valid version string)
/// - `manifest`: Optional JSON string representation of the dataset manifest
///
/// ## Response
/// - **200 OK**: Dataset successfully deployed
/// - **400 Bad Request**: Invalid dataset name, version, or manifest format
/// - **409 Conflict**: Dataset already exists with provided manifest, or manifest required but not provided
/// - **500 Internal Server Error**: Database, scheduler, or object store error
///
/// ## Error Codes
/// - `INVALID_REQUEST`: Invalid dataset name, version format, or request structure
/// - `INVALID_MANIFEST`: Manifest JSON parsing or structure error
/// - `MANIFEST_VALIDATION_ERROR`: Manifest name/version doesn't match request parameters
/// - `MANIFEST_REGISTRATION_ERROR`: Failed to register manifest in system
/// - `MANIFEST_REQUIRED`: No existing dataset found and no manifest provided
/// - `DATASET_ALREADY_EXISTS`: Dataset exists and manifest provided (conflict)
/// - `SCHEDULER_ERROR`: Failed to schedule dataset dump job
/// - `DATASET_DEF_STORE_ERROR`: Failed to store dataset definition
/// - `STORE_ERROR`: Failed to load or access dataset store
///
/// ## Behavior
/// This handler supports multiple deployment scenarios:
/// 1. **Existing dataset without manifest**: Schedules dump for existing dataset
/// 2. **New manifest dataset**: Registers manifest then schedules dump
/// 3. **New dataset definition**: Stores definition then schedules dump
/// 4. **Conflict cases**: Returns appropriate errors for invalid combinations
///
/// The handler:
/// - Validates dataset name and version format
/// - Attempts to load existing dataset from store
/// - Handles manifest registration for new datasets
/// - Schedules dataset dump job via scheduler
/// - Returns appropriate status codes and error messages
#[tracing::instrument(skip_all, err)]
pub async fn handler(
    State(ctx): State<Ctx>,
    Json(payload): Json<DeployRequest>,
) -> Result<(StatusCode, &'static str), BoxRequestError> {
    handler_inner(ctx, payload)
        .await
        .map_err(BoxRequestError::from)
}

/// Handler for the `/deploy` endpoint
///
/// A dump job is scheduled on a worker node by means of a DB notification.
/// If a manifest is provided, it will be registered before deployment.
#[tracing::instrument(skip_all, err)]
pub async fn handler_inner(
    ctx: Ctx,
    payload: DeployRequest,
) -> Result<(StatusCode, &'static str), Error> {
    validate_dataset_name(&payload.dataset_name)
        .map_err(|e| Error::InvalidRequest(format!("invalid dataset name: {e}").into()))?;
    let dataset_version = Version::from_str(&payload.version)
        .map_err(|e| Error::InvalidRequest(format!("invalid dataset version: {e}").into()))?;
    let dataset = ctx
        .store
        .try_load_dataset(&payload.dataset_name, Some(&dataset_version))
        .await?;

    match (dataset, payload.manifest) {
        (Some(dataset), None) => {
            tracing::info!(
                "Deploying existing dataset '{}' version '{}'",
                payload.dataset_name,
                payload.version
            );
            ctx.scheduler
                .schedule_dataset_dump(dataset, None)
                .await
                .map_err(Error::SchedulerError)?;
        }
        (None, Some(manifest_str)) => {
            let common: DatasetDefsCommon =
                serde_json::from_str::<DatasetDefsCommon>(&manifest_str)
                    .map_err(|e| Error::InvalidManifest(e.to_string()))?;

            match common.kind.as_str() {
                "manifest" => {
                    let manifest: Manifest = serde_json::from_str(&manifest_str)
                        .map_err(|e| Error::InvalidManifest(e.to_string()))?;
                    if manifest.name != payload.dataset_name
                        || manifest.version.0.to_string() != payload.version
                    {
                        return Err(Error::ManifestValidationError(
                            manifest.name,
                            manifest.version.0.to_string(),
                        ));
                    }
                    register_manifest(&ctx.store, &manifest)
                        .await
                        .map_err(|e| Error::ManifestRegistrationError(e.to_string()))?;
                    tracing::info!(
                        "Registered manifest for dataset '{}' version '{}'",
                        payload.dataset_name,
                        payload.version
                    );

                    let dataset = manifest::dataset(manifest)
                        .map_err(|e| Error::InvalidManifest(e.to_string()))?;
                    ctx.scheduler.schedule_dataset_dump(dataset, None).await?;
                }
                _ => {
                    let format_extension = if manifest_str.starts_with('{') {
                        "json"
                    } else {
                        "toml"
                    };
                    let path = Path::from(format!("{}.{}", payload.dataset_name, format_extension));
                    ctx.config
                        .dataset_defs_store
                        .prefixed_store()
                        .put(&path, manifest_str.into())
                        .await
                        .map_err(Error::DatasetDefStoreError)?;

                    let dataset = ctx.store.load_dataset(&payload.dataset_name, None).await?;

                    ctx.scheduler.schedule_dataset_dump(dataset, None).await?;
                }
            }
        }
        (None, None) => {
            return Err(Error::ManifestRequired(
                payload.dataset_name,
                payload.version,
            ));
        }
        (Some(_), Some(_)) => {
            return Err(Error::DatasetAlreadyExists(
                payload.dataset_name,
                payload.version,
            ));
        }
    }

    Ok((StatusCode::OK, "DEPLOYMENT_SUCCESSFUL"))
}

/// Request payload for dataset deployment
///
/// Contains the dataset name, version, and optionally a manifest.
/// If manifest is provided, it will be registered before deployment.
#[derive(serde::Deserialize, serde::Serialize)]
pub struct DeployRequest {
    /// Name of the dataset to be deployed
    pub dataset_name: String,
    /// Version of the dataset to deploy
    pub version: String,
    /// Optional JSON string representation of the dataset manifest
    /// If provided, it will be registered before deployment
    #[serde(default)]
    pub manifest: Option<String>,
}

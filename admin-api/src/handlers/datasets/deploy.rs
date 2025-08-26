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

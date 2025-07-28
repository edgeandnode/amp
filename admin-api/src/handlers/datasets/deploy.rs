//! Dataset deploy handler
use axum::{Json, extract::State, http::StatusCode};
use common::manifest::Manifest;
use http_common::BoxRequestError;
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
    let dataset = ctx
        .metadata_db
        .get_manifest_from_registry(&payload.dataset_name, &payload.version)
        .await
        .map_err(|e| Error::InvalidRequest(e.to_string().into()))?;

    match (dataset, payload.manifest) {
        (Some((dataset_name, version)), None) => {
            tracing::info!(
                "Deploying existing dataset '{}' version '{}'",
                dataset_name,
                version
            );
            let dataset = ctx
                .store
                .load_dataset_with_version(&dataset_name, &version)
                .await?;
            ctx.scheduler
                .schedule_dataset_dump(dataset, None)
                .await
                .map_err(Error::SchedulerError)?;
        }
        (None, Some(manifest_str)) => {
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
            let manifest = register_manifest(&ctx.store, manifest)
                .await
                .map_err(|e| Error::ManifestRegistrationError(e.to_string()))?;
            tracing::info!(
                "Registered manifest for dataset '{}' version '{}'",
                payload.dataset_name,
                payload.version
            );
            ctx.scheduler
                .schedule_dataset_dump(manifest.into(), None)
                .await
                .map_err(Error::SchedulerError)?;
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
#[derive(serde::Deserialize)]
pub struct DeployRequest {
    /// Name of the dataset to be deployed
    dataset_name: String,
    /// Version of the dataset to deploy
    version: String,
    /// Optional JSON string representation of the dataset manifest
    /// If provided, it will be registered before deployment
    #[serde(default)]
    manifest: Option<String>,
}

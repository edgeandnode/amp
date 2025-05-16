//! Dataset deploy handler

use axum::{extract::State, http::StatusCode, Json};
use common::manifest::Manifest;
use http_common::BoxRequestError;
use object_store::path::Path;
use serde::de::Error as _;

use super::error::Error;
use crate::{ctx::Ctx, handlers::common::validate_dataset_name};

/// Handler for the `/deploy` endpoint
///
/// A dump job is scheduled on a worker node by means of a DB notification.
#[tracing::instrument(skip_all, err)]
pub async fn handler(
    State(ctx): State<Ctx>,
    Json(payload): Json<DeployRequest>,
) -> Result<(StatusCode, &'static str), BoxRequestError> {
    // Parse and validate the manifest
    let manifest: Manifest =
        serde_json::from_str(&payload.manifest).map_err(Error::InvalidManifest)?;

    if let Err(err) = validate_dataset_name(&payload.dataset_name) {
        let err = Error::InvalidManifest(serde_json::Error::custom(format!(
            "invalid dataset name: {err}"
        )));
        tracing::error!(name=%payload.dataset_name, error=?err, "invalid dataset name");
        return Err(err.into());
    }

    // Write the manifest to the dataset def store
    let path = Path::from(payload.dataset_name.clone() + ".json");
    ctx.config
        .dataset_defs_store
        .prefixed_store()
        .put(&path, payload.manifest.into())
        .await
        .map_err(|err| {
            tracing::error!(path=%path, error=?err, "failed to write manifest to dataset definition store");
            Error::DatasetDefStoreError(err)
        })?;

    ctx.scheduler
        .schedule_dataset_dump(manifest)
        .await
        .map_err(|err| {
            tracing::error!(error=?err, "failed to schedule dataset dump");
            Error::SchedulerError(err)
        })?;

    Ok((StatusCode::OK, "DEPLOYMENT_SUCCESSFUL"))
}

/// Request payload for dataset deployment
///
/// Contains the dataset name and manifest JSON string that will be validated
/// and stored in the dataset definitions store.
#[derive(serde::Deserialize)]
pub struct DeployRequest {
    /// Name of the dataset to be deployed
    dataset_name: String,
    /// JSON string representation of the dataset manifest
    manifest: String,
}

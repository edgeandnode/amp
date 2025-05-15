//! Dataset deploy handler

use axum::{extract::State, http::StatusCode, Json};
use common::{manifest::Manifest, BoxError};
use http_common::{BoxRequestError, RequestError};

use crate::ctx::Ctx;

/// Handler for the `/deploy` endpoint
///
/// A dump job is scheduled on a worker node by means of a DB notification.
#[tracing::instrument(skip_all, err)]
pub async fn handler(
    State(ctx): State<Ctx>,
    Json(payload): Json<DeployRequest>,
) -> Result<(StatusCode, &'static str), BoxRequestError> {
    let manifest: Manifest =
        serde_json::from_str(&payload.manifest).map_err(DeployError::ManifestParseError)?;

    // Write the manifest to the dataset def store
    let path = payload.dataset_name.clone() + ".json";
    ctx.config
        .dataset_defs_store
        .prefixed_store()
        .put(&path.into(), payload.manifest.into())
        .await
        .map_err(|_| DeployError::DatasetDefStoreError)?;

    ctx.scheduler
        .schedule_dataset_dump(manifest)
        .await
        .map_err(DeployError::SchedulerError)?;

    Ok((StatusCode::OK, "Deployment successful"))
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

/// Errors that can occur during the deployment process
#[derive(Debug, thiserror::Error)]
enum DeployError {
    /// Manifest parsing error
    #[error("failed to parse manifest: {0}")]
    ManifestParseError(serde_json::Error),

    /// Dataset dump scheduling error
    #[error("scheduler error: {0}")]
    SchedulerError(BoxError),

    /// Dataset definition store write error
    #[error("failed to write manifest to internal store")]
    DatasetDefStoreError,
}

impl RequestError for DeployError {
    fn status_code(&self) -> StatusCode {
        match self {
            DeployError::ManifestParseError(_) => StatusCode::BAD_REQUEST,
            DeployError::SchedulerError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            DeployError::DatasetDefStoreError => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_code(&self) -> &'static str {
        match self {
            DeployError::ManifestParseError(_) => "MANIFEST_PARSE_ERROR",
            DeployError::SchedulerError(_) => "SCHEDULER_ERROR",
            DeployError::DatasetDefStoreError => "DATASET_DEF_STORE_ERROR",
        }
    }
}

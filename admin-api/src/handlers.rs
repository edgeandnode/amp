use axum::http::StatusCode;
use axum::{extract::State, Json};
use common::{manifest::Manifest, BoxError};
use http_common::{BoxRequestError, RequestError};
use serde::Deserialize;
use thiserror::Error;
use tracing::instrument;

use crate::ServiceState;

// Define the request payload structure
#[derive(Deserialize)]
pub struct DeployRequest {
    dataset_name: String,
    manifest: String,
}

#[derive(Debug, Error)]
enum DeployError {
    #[error("failed to parse manifest: {0}")]
    ManifestParseError(serde_json::Error),
    #[error("scheduler error: {0}")]
    SchedulerError(BoxError),
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

/// Handler for the /deploy endpoint
///
/// This handler has very different behaviour depending on whether a metadata DB is configured.
///
/// # No Metadata DB
///
/// If no metadata DB is configured, the job run is spawned on this node. The job will not be resumed
/// on restart. This is for local usage that wants to avoid the operational overhead of Postgres.
///
/// # Metadata DB
///
/// If a metadata DB is configured, the job is scheduled on a worker node by means of a DB
/// notification.
#[instrument(skip_all, err)]
pub async fn deploy_handler(
    State(state): State<ServiceState>,
    Json(payload): Json<DeployRequest>,
) -> Result<(StatusCode, &'static str), BoxRequestError> {
    use DeployError::*;

    let ServiceState {
        config,
        job_scheduler,
    } = state;

    // Validate the manifest
    let manifest: Manifest = serde_json::from_str(&payload.manifest).map_err(ManifestParseError)?;

    // Write the manifest to the dataset def store
    let path = payload.dataset_name.clone() + ".json";
    config
        .dataset_defs_store
        .prefixed_store()
        .put(&path.into(), payload.manifest.clone().into())
        .await
        .map_err(|_| DatasetDefStoreError)?;

    job_scheduler
        .clone()
        .schedule_dataset_dump(manifest)
        .await
        .map_err(SchedulerError)?;

    Ok((axum::http::StatusCode::OK, "Deployment successful"))
}

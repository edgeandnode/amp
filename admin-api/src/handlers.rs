use std::sync::Arc;

use axum::http::StatusCode;
use axum::{extract::State, Json};
use common::{manifest::Manifest, BoxError};
use dataset_store::DatasetStore;
use dump::dump_dataset;
use http_common::{BoxRequestError, RequestError};
use serde::Deserialize;
use thiserror::Error;
use tokio::{select, task::JoinError};
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
    #[error("Failed to parse manifest: {0}")]
    ManifestParseError(serde_json::Error),
    #[error("Failed to sync dataset: {0}")]
    SyncError(BoxError),
    #[error("Failed to join dataset sync task: {0}")]
    JoinError(JoinError),
    #[error("Failed to write manifest to internal store")]
    DatasetDefStoreError,
}

impl RequestError for DeployError {
    fn status_code(&self) -> StatusCode {
        match self {
            DeployError::ManifestParseError(_) => StatusCode::BAD_REQUEST,
            DeployError::SyncError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            DeployError::JoinError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            DeployError::DatasetDefStoreError => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_code(&self) -> &'static str {
        match self {
            DeployError::ManifestParseError(_) => "MANIFEST_PARSE_ERROR",
            DeployError::SyncError(_) => "DATASET_SYNC_ERROR",
            DeployError::JoinError(_) => "JOIN_ERROR",
            DeployError::DatasetDefStoreError => "DATASET_DEF_STORE_ERROR",
        }
    }
}

// Handler for the /deploy endpoint
#[instrument(skip_all, err)]
pub async fn deploy_handler(
    State(state): State<Arc<ServiceState>>,
    Json(payload): Json<DeployRequest>,
) -> Result<(StatusCode, &'static str), BoxRequestError> {
    use DeployError::*;

    // Validate the manifest
    let _: Manifest =
        serde_json::from_str(&payload.manifest).map_err(DeployError::ManifestParseError)?;

    // Write the manifest to the dataset def store
    let path = payload.dataset_name.clone() + ".json";
    state
        .config
        .dataset_defs_store
        .prefixed_store()
        .put(&path.into(), payload.manifest.clone().into())
        .await
        .map_err(|_| DatasetDefStoreError)?;

    let dataset_store = DatasetStore::new(state.config.clone(), state.metadata_db.clone());

    let dump_task = tokio::spawn(async move {
        dump_dataset(
            &payload.dataset_name,
            &dataset_store,
            &state.config,
            state.metadata_db.as_ref(),
            1,
            dump::default_partition_size(),
            &dump::default_parquet_opts(),
            0,
            None,
        )
        .await
        .map_err(SyncError)
    });

    // Wait for a couple of seconds to see if the dump task errors
    select! {
        res = dump_task => {
            // The dump task completed in under a second, return the error.
            // Or even a success if it completed successfully that fast.
            let () = res.map_err(JoinError)??;
            Ok((axum::http::StatusCode::OK, "Deployment successful, sync done"))
        }
        _ = tokio::time::sleep(tokio::time::Duration::from_secs(2)) => {
            // The dump task did not complete within 1 second
            Ok((axum::http::StatusCode::OK, "Deployment successful, dataset being synced"))
        }
    }
}

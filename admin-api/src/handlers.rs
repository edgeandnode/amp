use std::sync::Arc;

use axum::{extract::State, Json};
use common::manifest::DatasetManifest;
use http_common::{BoxRequestError, RequestError};
use reqwest::StatusCode;
use serde::Deserialize;
use thiserror::Error;
use tracing::instrument;

use crate::ServiceState;

// Define the request payload structure
#[derive(Deserialize)]
pub struct DeployRequest {
    cid: String,
}

#[derive(Debug, Error)]
enum DeployError {
    #[error("Failed to fetch manifest from IPFS: {0}")]
    ManifestFetchError(reqwest::Error),
    #[error("Failed to parse manifest: {0}")]
    ManifestParseError(serde_json::Error),
}

impl RequestError for DeployError {
    fn status_code(&self) -> StatusCode {
        match self {
            DeployError::ManifestFetchError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            DeployError::ManifestParseError(_) => StatusCode::BAD_REQUEST,
        }
    }

    fn error_code(&self) -> &'static str {
        match self {
            DeployError::ManifestFetchError(_) => "MANIFEST_FETCH_ERROR",
            DeployError::ManifestParseError(_) => "MANIFEST_PARSE_ERROR",
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

    // Simulate processing the CID (content identifier)
    println!("Received CID: {}", payload.cid);

    let base = if !state.config.ipfs_url.ends_with('/') {
        state.config.ipfs_url.clone()
    } else {
        state.config.ipfs_url.clone() + "/"
    };

    // Fetch the manifest from IPFS
    let url = format!("{}/api/v0/cat?arg={}", base, payload.cid);
    let req = state.ipfs_client.post(url).send().await;
    let manifest_raw = req
        .map_err(ManifestFetchError)?
        .text()
        .await
        .map_err(ManifestFetchError)?;

    let manifest: DatasetManifest =
        serde_json::from_str(&manifest_raw).map_err(DeployError::ManifestParseError)?;

    // dump_dataset(
    //     "test",
    //     &dataset_store,
    //     &config,
    //     None,
    //     &env,
    //     1,
    //     1000000,
    //     &parquet_opts,
    //     0,
    //     Some(1000000),
    // )
    // .await?;

    // Respond with a success message
    Ok((axum::http::StatusCode::OK, "Deployment successful"))
}

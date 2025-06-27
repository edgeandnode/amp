//! Dataset deploy handler

use axum::{Json, extract::State, http::StatusCode};
use http_common::BoxRequestError;
use object_store::path::Path;

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
#[tracing::instrument(skip_all, err)]
pub async fn handler_inner(
    ctx: Ctx,
    payload: DeployRequest,
) -> Result<(StatusCode, &'static str), Error> {
    validate_dataset_name(&payload.dataset_name)
        .map_err(|e| Error::InvalidRequest(format!("invalid dataset name: {e}").into()))?;

    // Write the manifest to the dataset def store. Detect if JSON or TOML.
    let format_extension = if payload.manifest.trim_start().starts_with('{') {
        "json"
    } else {
        "toml"
    };
    let path = Path::from(format!("{}.{}", payload.dataset_name, format_extension));
    ctx.config
        .dataset_defs_store
        .prefixed_store()
        .put(&path, payload.manifest.into())
        .await
        .map_err(Error::DatasetDefStoreError)?;

    let dataset = ctx.store.load_dataset(&payload.dataset_name).await?;

    ctx.scheduler
        .schedule_dataset_dump(dataset, None)
        .await
        .map_err(Error::SchedulerError)?;

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

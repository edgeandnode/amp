//! Dataset deploy handler

use axum::{extract::State, http::StatusCode, Json};
use http_common::BoxRequestError;
use metadata_db::JobStatus;
use object_store::path::Path;
use tokio::time::{sleep, Duration};

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

    let job_id = ctx
        .scheduler
        .schedule_dataset_dump(dataset, payload.end_block)
        .await
        .map_err(Error::SchedulerError)?;

    if payload.wait_for_completion {
        if payload.end_block.is_none() {
            return Err(Error::InvalidRequest(
                format!("end_block must be specified for wait_for_completion",).into(),
            ));
        }
        // Poll the job status until it reaches a terminal state
        const POLL_INTERVAL: Duration = Duration::from_millis(500);

        loop {
            // Get the current job status
            let job =
                ctx.metadata_db.get_job(&job_id).await?.ok_or_else(|| {
                    Error::SchedulerError(format!("job {job_id} not found").into())
                })?;

            match job.status {
                JobStatus::Completed => break,
                JobStatus::Failed | JobStatus::Stopped | JobStatus::Unknown => {
                    return Err(Error::UnexpectedJobStatus(job.status).into());
                }
                JobStatus::Scheduled
                | JobStatus::Running
                | JobStatus::StopRequested
                | JobStatus::Stopping => sleep(POLL_INTERVAL).await,
            }
        }
    }

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

    end_block: Option<i64>,

    /// If true, the handler will wait for the dump job to complete before returning.
    #[serde(default)]
    wait_for_completion: bool,
}

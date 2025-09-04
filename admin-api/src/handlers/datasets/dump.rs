use std::time::Duration;

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use common::manifest::Version;
use http_common::BoxRequestError;
use metadata_db::JobStatus;
use tokio::time::sleep;

use super::error::Error;
use crate::ctx::Ctx;

/// Request options for dataset dump operations
///
/// Controls the behavior and scope of the data extraction job.
/// These options determine the range of blocks to extract and whether
/// the API call should wait for job completion.
#[derive(serde::Deserialize)]
pub struct DumpOptions {
    /// The dataset version to dump
    ///
    /// If not specified, the latest version of the dataset will be used
    #[serde(default)]
    version: Option<Version>,

    /// The last block number to extract (optional)
    ///
    /// If not specified, the extraction will continue indefinitely
    /// (until manually stopped or the blockchain tip). Required when
    /// using `wait_for_completion=true` to prevent infinite waiting.
    #[serde(default)]
    end_block: Option<i64>,

    /// If true, the handler will wait for the dump job to complete before returning
    ///
    /// When enabled, the API call will poll the job status every 500ms until
    /// the job reaches a terminal state. Requires `end_block` to be specified
    /// to prevent indefinite waiting. Returns success only if the job completes
    /// successfully, and returns error if the job fails or is stopped.
    #[serde(default)]
    wait_for_completion: bool,
}

/// Handler for the `POST /datasets/{id}/dump` endpoint
///
/// Triggers a data extraction job for the specified dataset. The `id` parameter
/// identifies the target dataset, and the request accepts a JSON payload with
/// dump options including end_block and wait_for_completion settings.
///
/// ## Path Parameters
/// - `id`: The unique identifier of the dataset to dump (dataset name)
///
/// ## Request Body
/// - `end_block`: (optional) The last block number to extract (if not specified, extracts indefinitely)
/// - `wait_for_completion`: (optional, default: false) If true, waits for job completion before returning
///
/// ## Response
/// - **200 OK**: Dump job scheduled successfully (or completed if wait_for_completion=true)
/// - **400 Bad Request**: Invalid dataset ID or invalid request parameters
/// - **404 Not Found**: Dataset with the given ID does not exist
/// - **500 Internal Server Error**: Scheduler, database, or store error
///
/// ## Error Codes
/// - `INVALID_REQUEST`: Invalid dataset name, missing end_block for wait_for_completion, or malformed request
/// - `DATASET_NOT_FOUND`: No dataset exists with the given ID
/// - `SCHEDULER_ERROR`: Failed to schedule the dump job
/// - `STORE_ERROR`: Failed to load dataset from store
/// - `METADATA_DB_ERROR`: Database error while polling job status
/// - `UNEXPECTED_JOB_STATUS`: Job reached unexpected terminal state (failed, stopped, unknown)
///
/// ## Behavior
/// This handler supports two modes:
/// 1. **Asynchronous mode** (default): Schedules job and returns immediately
/// 2. **Synchronous mode** (wait_for_completion=true): Waits for job completion
///
/// For synchronous mode:
/// - `end_block` must be specified (cannot wait indefinitely)
/// - Polls job status every 500ms until completion
/// - Returns success only when job completes successfully
/// - Returns error if job fails, stops, or reaches unknown state
///
/// The handler:
/// - Validates the dataset ID and loads dataset from store
/// - Schedules a dataset dump job via the scheduler
/// - Optionally waits for job completion with status polling
/// - Returns appropriate status codes and messages
#[tracing::instrument(skip_all, err)]
pub async fn handler(
    State(ctx): State<Ctx>,
    Path(id): Path<String>,
    Json(options): Json<DumpOptions>,
) -> Result<(StatusCode, &'static str), BoxRequestError> {
    let dataset = ctx
        .store
        .load_dataset(&id, options.version.as_ref())
        .await
        .map_err(Error::StoreError)?;

    let job_id = ctx
        .scheduler
        .schedule_dataset_dump(dataset, options.end_block)
        .await
        .map_err(|err| {
            tracing::error!(error=?err, "failed to schedule dataset dump");
            Error::SchedulerError(err)
        })?;

    if options.wait_for_completion {
        if options.end_block.is_none() {
            return Err(Error::InvalidRequest(
                "end_block must be specified for wait_for_completion"
                    .to_string()
                    .into(),
            )
            .into());
        }
        // Poll the job status until it reaches a terminal state
        const POLL_INTERVAL: Duration = Duration::from_millis(500);

        loop {
            // Get the current job status
            let job = ctx
                .metadata_db
                .get_job(&job_id)
                .await
                .map_err(|e| BoxRequestError::from(Error::from(e)))?
                .ok_or_else(|| Error::InvalidRequest(format!("job {job_id} not found").into()))?;

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
        Ok((StatusCode::OK, "Dump completed successfully"))
    } else {
        Ok((StatusCode::OK, "Dump job scheduled successfully"))
    }
}

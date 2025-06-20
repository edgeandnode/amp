use std::time::Duration;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use http_common::BoxRequestError;
use metadata_db::JobStatus;
use tokio::time::sleep;

use super::error::Error;
use crate::ctx::Ctx;

#[derive(serde::Deserialize)]
pub struct DumpOptions {
    #[serde(default)]
    end_block: Option<i64>,

    /// If true, the handler will wait for the dump job to complete before returning.
    #[serde(default)]
    wait_for_completion: bool,
}

#[tracing::instrument(skip_all, err)]
pub async fn handler(
    State(ctx): State<Ctx>,
    Path(id): Path<String>,
    Json(options): Json<DumpOptions>,
) -> Result<(StatusCode, &'static str), BoxRequestError> {
    let dataset = ctx
        .store
        .load_dataset(&id)
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
                format!("end_block must be specified for wait_for_completion",).into(),
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
                .ok_or_else(|| Error::SchedulerError(format!("job {job_id} not found").into()))?;

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

    todo!()
}

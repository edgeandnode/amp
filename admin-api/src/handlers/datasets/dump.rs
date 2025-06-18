use axum::{
    extract::{Path, State},
    http::StatusCode,
};
use http_common::BoxRequestError;

use super::error::Error;
use crate::ctx::Ctx;

#[tracing::instrument(skip_all, err)]
pub async fn handler(
    State(ctx): State<Ctx>,
    Path(id): Path<String>,
) -> Result<(StatusCode, &'static str), BoxRequestError> {
    let dataset = ctx
        .store
        .load_dataset(&id)
        .await
        .map_err(Error::StoreError)?;
    ctx.scheduler
        .schedule_dataset_dump(dataset)
        .await
        .map_err(|err| {
            tracing::error!(error=?err, "failed to schedule dataset dump");
            Error::SchedulerError(err)
        })?;

    todo!()
}

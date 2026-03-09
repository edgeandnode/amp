//! Handler for the `POST /datasets/{namespace}/{name}/versions/{revision}/rematerialize` endpoint
//!
//! Re-extracts a specified block range for a raw dataset by sending a rematerialize
//! notification to the active materialization job.
//!
//! # Requirements
//!
//! - The dataset must be a raw dataset (not derived)
//! - The dataset must have an active materialization job running
//!
//! # Current Limitations
//!
//! If no active job exists, the endpoint returns a `NO_ACTIVE_JOB` error. Users must
//! ensure the dataset has an active job (e.g., via deployment) before rematerializing.
//!
//! Future enhancement: Automatically create a temporary job when no active job exists.

use std::fmt::Debug;

use amp_datasets_registry::error::ResolveRevisionError;
use amp_worker_core::jobs::job_id::JobId;
use axum::{
    Json,
    extract::{
        Path, State,
        rejection::{JsonRejection, PathRejection},
    },
    http::StatusCode,
};
use common::datasets_cache::GetDatasetError;
use datasets_common::{
    hash_reference::HashReference, name::Name, namespace::Namespace, reference::Reference,
    revision::Revision,
};
use datasets_derived::DerivedDatasetKind;
use metadata_db::jobs;
use monitoring::logging;
use worker::job::JobNotification;

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
};

/// Handler for the `POST /datasets/{namespace}/{name}/versions/{revision}/rematerialize` endpoint
///
/// Re-extracts a specified block range for a raw dataset by notifying the active
/// materialization job.
///
/// ## Requirements
/// - Dataset must be raw (not derived)
/// - Dataset must have an active materialization job running
///
/// ## Path Parameters
/// - `namespace`: Dataset namespace
/// - `name`: Dataset name
/// - `revision`: Revision (version, hash, "latest", or "dev")
///
/// ## Request Body
/// - `start_block`: Start block of the range to rematerialize (inclusive)
/// - `end_block`: End block of the range to rematerialize (inclusive)
///
/// ## Response
/// - **202 Accepted**: Rematerialize request sent to active job
/// - **400 Bad Request**: Invalid parameters, derived dataset, invalid range, or no active job
/// - **404 Not Found**: Dataset not found
///
/// ## Error Codes
/// - `NO_ACTIVE_JOB`: No active materialization job found (deploy dataset first)
/// - `DERIVED_DATASET`: Cannot rematerialize derived datasets
/// - `INVALID_RANGE`: start_block > end_block or start_block < dataset.start_block
/// - **500 Internal Server Error**: Database or scheduler error
///
/// ## Error Codes
/// - `INVALID_PATH`: Invalid path parameters
/// - `INVALID_BODY`: Invalid request body
/// - `DATASET_NOT_RAW`: Cannot rematerialize derived datasets
/// - `DATASET_NOT_FOUND`: Dataset or revision not found
/// - `INVALID_BLOCK_RANGE`: start_block > end_block or out of bounds
/// - `RESOLVE_REVISION_ERROR`: Failed to resolve revision
/// - `GET_DATASET_ERROR`: Failed to load dataset
/// - `FIND_ACTIVE_JOB_ERROR`: Failed to find active job
/// - `NOTIFY_WORKER_ERROR`: Failed to send rematerialize notification
/// - `SCHEDULER_ERROR`: Failed to schedule temporary job
///
/// ## Behavior
/// This endpoint handles rematerialization in two ways:
///
/// 1. **Active job exists**: Sends a `Rematerialize` notification to the running job via
///    PostgreSQL NOTIFY. The job will process the range and resume normal operation.
///
/// 2. **No active job**: Schedules a new temporary job that only processes the specified
///    range and exits when complete.
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        post,
        path = "/datasets/{namespace}/{name}/versions/{revision}/rematerialize",
        tag = "datasets",
        operation_id = "rematerialize_dataset",
        params(
            ("namespace" = String, Path, description = "Dataset namespace"),
            ("name" = String, Path, description = "Dataset name"),
            ("revision" = String, Path, description = "Revision (version, hash, latest, or dev)")
        ),
        request_body = RematerializeRequest,
        responses(
            (status = 202, description = "Rematerialize request sent or job scheduled", body = RematerializeResponse),
            (status = 400, description = "Bad request (invalid parameters)", body = ErrorResponse),
            (status = 404, description = "Dataset or revision not found", body = ErrorResponse),
            (status = 500, description = "Internal server error", body = ErrorResponse)
        )
    )
)]
pub async fn handler(
    path: Result<Path<(Namespace, Name, Revision)>, PathRejection>,
    State(ctx): State<Ctx>,
    json: Result<Json<RematerializeRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<RematerializeResponse>), ErrorResponse> {
    let reference = match path {
        Ok(Path((namespace, name, revision))) => Reference::new(namespace, name, revision),
        Err(err) => {
            tracing::debug!(error = %err, error_source = logging::error_source(&err), "invalid path parameters");
            return Err(Error::InvalidPath(err).into());
        }
    };

    let RematerializeRequest {
        start_block,
        end_block,
    } = match json {
        Ok(Json(request)) => request,
        Err(err) => {
            tracing::debug!(error = %err, error_source = logging::error_source(&err), "invalid request body");
            return Err(Error::InvalidBody(err).into());
        }
    };

    // Validate block range
    if start_block > end_block {
        return Err(Error::InvalidBlockRange {
            start_block,
            end_block,
            reason: "start_block must be <= end_block".to_string(),
        }
        .into());
    }

    tracing::debug!(
        dataset_reference = %reference,
        start_block,
        end_block,
        "rematerializing dataset range"
    );

    let namespace = reference.namespace().clone();
    let name = reference.name().clone();
    let revision = reference.revision().clone();

    // Resolve reference to hash reference
    let reference = ctx
        .datasets_registry
        .resolve_revision(&reference)
        .await
        .map_err(Error::ResolveRevision)?
        .ok_or_else(|| Error::DatasetNotFound {
            namespace,
            name,
            revision,
        })?;

    // Load the full dataset object
    let dataset = ctx
        .datasets_cache
        .get_dataset(&reference)
        .await
        .map_err(Error::GetDataset)?;

    // Validate dataset is raw (not derived)
    if dataset.kind() == DerivedDatasetKind {
        return Err(Error::DatasetNotRaw {
            reference: reference.clone(),
        }
        .into());
    }

    // Validate block range against dataset bounds
    if let Some(dataset_start) = dataset.start_block()
        && start_block < dataset_start
    {
        return Err(Error::InvalidBlockRange {
            start_block,
            end_block,
            reason: format!(
                "start_block ({start_block}) is before dataset start block ({dataset_start})"
            ),
        }
        .into());
    }

    // Check for active job
    let active_jobs = jobs::get_by_dataset(&ctx.metadata_db, reference.hash())
        .await
        .map_err(Error::FindActiveJob)?;

    let active_job = active_jobs.into_iter().find(|job| {
        matches!(
            job.status.into(),
            amp_worker_core::jobs::status::JobStatus::Scheduled
                | amp_worker_core::jobs::status::JobStatus::Running
        )
    });

    let (job_id, new_job_created) = if let Some(job) = active_job {
        // Send rematerialize notification to the active job
        let job_id: JobId = job.id.into();

        tracing::info!(
            %job_id,
            dataset_reference = %reference,
            start_block,
            end_block,
            "sending rematerialize notification to active job"
        );

        ctx.metadata_db
            .send_job_notif(
                job.node_id,
                &JobNotification::rematerialize(job_id, start_block, end_block),
            )
            .await
            .map_err(Error::NotifyWorker)?;

        (job_id, false)
    } else {
        // TODO: Schedule a new temporary job for the rematerialize range
        // For now, return an error
        return Err(Error::NoActiveJob {
            reference: reference.clone(),
        }
        .into());
    };

    Ok((
        StatusCode::ACCEPTED,
        Json(RematerializeResponse {
            job_id,
            new_job_created,
        }),
    ))
}

/// Request for rematerializing a dataset range
#[derive(Debug, serde::Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct RematerializeRequest {
    /// Start block of the range to rematerialize (inclusive)
    pub start_block: u64,
    /// End block of the range to rematerialize (inclusive)
    pub end_block: u64,
}

/// Response for rematerialize operation
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct RematerializeResponse {
    /// The ID of the job processing the rematerialize request
    #[cfg_attr(feature = "utoipa", schema(value_type = i64))]
    pub job_id: JobId,
    /// Whether a new job was created (true) or existing job notified (false)
    pub new_job_created: bool,
}

/// Errors that can occur when rematerializing a dataset
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid path parameters
    #[error("Invalid path parameters: {0}")]
    InvalidPath(#[source] PathRejection),

    /// Invalid request body
    #[error("Invalid request body: {0}")]
    InvalidBody(#[source] JsonRejection),

    /// Dataset is not a raw dataset
    #[error("Cannot rematerialize derived dataset: {reference}")]
    DatasetNotRaw { reference: HashReference },

    /// Dataset or revision not found
    #[error("Dataset '{namespace}/{name}' at revision '{revision}' not found")]
    DatasetNotFound {
        namespace: Namespace,
        name: Name,
        revision: Revision,
    },

    /// Invalid block range
    #[error("Invalid block range {start_block}..{end_block}: {reason}")]
    InvalidBlockRange {
        start_block: u64,
        end_block: u64,
        reason: String,
    },

    /// Failed to resolve revision
    #[error("Failed to resolve revision: {0}")]
    ResolveRevision(#[source] ResolveRevisionError),

    /// Failed to load dataset
    #[error("Failed to load dataset: {0}")]
    GetDataset(#[source] GetDatasetError),

    /// Failed to find active job
    #[error("Failed to find active job: {0}")]
    FindActiveJob(#[source] metadata_db::Error),

    /// No active job found
    #[error(
        "No active materialization job found for dataset: {reference}. Deploy the dataset first to start an active job, then retry the rematerialize command."
    )]
    NoActiveJob { reference: HashReference },

    /// Failed to send rematerialize notification
    #[error("Failed to send rematerialize notification: {0}")]
    NotifyWorker(#[source] metadata_db::Error),

    /// Failed to schedule temporary job
    #[error("Failed to schedule temporary job: {0}")]
    SchedulerError(String),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidPath(_) => "INVALID_PATH",
            Error::InvalidBody(_) => "INVALID_BODY",
            Error::DatasetNotRaw { .. } => "DATASET_NOT_RAW",
            Error::DatasetNotFound { .. } => "DATASET_NOT_FOUND",
            Error::InvalidBlockRange { .. } => "INVALID_BLOCK_RANGE",
            Error::ResolveRevision(_) => "RESOLVE_REVISION_ERROR",
            Error::GetDataset(_) => "GET_DATASET_ERROR",
            Error::FindActiveJob(_) => "FIND_ACTIVE_JOB_ERROR",
            Error::NoActiveJob { .. } => "NO_ACTIVE_JOB",
            Error::NotifyWorker(_) => "NOTIFY_WORKER_ERROR",
            Error::SchedulerError(_) => "SCHEDULER_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidPath(_) => StatusCode::BAD_REQUEST,
            Error::InvalidBody(_) => StatusCode::BAD_REQUEST,
            Error::DatasetNotRaw { .. } => StatusCode::BAD_REQUEST,
            Error::DatasetNotFound { .. } => StatusCode::NOT_FOUND,
            Error::InvalidBlockRange { .. } => StatusCode::BAD_REQUEST,
            Error::ResolveRevision(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::GetDataset(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::FindActiveJob(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::NoActiveJob { .. } => StatusCode::NOT_FOUND,
            Error::NotifyWorker(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::SchedulerError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

use amp_datasets_registry::error::ResolveRevisionError;
use axum::{
    Json,
    extract::{
        Path, State,
        rejection::{JsonRejection, PathRejection},
    },
    http::StatusCode,
};
use datasets_common::{name::Name, namespace::Namespace, reference::Reference, revision::Revision};
use metadata_db::RedumpRequestId;
use monitoring::logging;
use worker::job::JobId;

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
    scheduler::{ListJobsByDatasetError, ScheduleJobError},
};

/// Handler for the `POST /datasets/{namespace}/{name}/versions/{revision}/redump` endpoint
///
/// Creates a redump request to re-extract blocks in the given range.
/// If no active dump job exists for the dataset, one is auto-started.
///
/// ## Path Parameters
/// - `namespace`: Dataset namespace
/// - `name`: Dataset name
/// - `revision`: Revision (version, hash, "latest", or "dev")
///
/// ## Request Body
/// - `start_block`: First block of the range to re-extract (inclusive)
/// - `end_block`: Last block of the range to re-extract (inclusive)
///
/// ## Response
/// - **202 Accepted**: Redump request created
/// - **400 Bad Request**: Invalid path parameters or request body
/// - **404 Not Found**: Dataset or revision not found
/// - **409 Conflict**: A redump request for this range already exists
/// - **500 Internal Server Error**: Database or scheduler error
///
/// ## Error Codes
/// - `INVALID_PATH`: Invalid path parameters (namespace, name, or revision)
/// - `INVALID_BODY`: Invalid request body (malformed JSON or missing required fields)
/// - `DATASET_NOT_FOUND`: The specified dataset or revision does not exist
/// - `RESOLVE_REVISION_ERROR`: Failed to resolve revision to manifest hash
/// - `DUPLICATE_REQUEST`: A redump request for this range already exists
/// - `LIST_JOBS_ERROR`: Failed to list jobs for dataset
/// - `SCHEDULER_ERROR`: Failed to schedule extraction job
/// - `DATABASE_ERROR`: Failed to insert redump request
///
/// ## Behavior
/// 1. Resolves the revision to find the corresponding dataset
/// 2. Checks if an active dump job exists for the dataset
/// 3. Inserts the redump request into the database
/// 4. If no active job, schedules a new dump job for the dataset
/// 5. Returns the request ID and job ID (if a new job was started)
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        post,
        path = "/datasets/{namespace}/{name}/versions/{revision}/redump",
        tag = "datasets",
        operation_id = "redump_dataset",
        params(
            ("namespace" = String, Path, description = "Dataset namespace"),
            ("name" = String, Path, description = "Dataset name"),
            ("revision" = String, Path, description = "Revision (version, hash, latest, or dev)")
        ),
        request_body = RedumpRequest,
        responses(
            (status = 202, description = "Redump request created", body = RedumpResponse),
            (status = 400, description = "Bad request (invalid parameters)", body = ErrorResponse),
            (status = 404, description = "Dataset or revision not found", body = ErrorResponse),
            (status = 409, description = "Duplicate redump request", body = ErrorResponse),
            (status = 500, description = "Internal server error", body = ErrorResponse)
        )
    )
)]
pub async fn handler(
    path: Result<Path<(Namespace, Name, Revision)>, PathRejection>,
    State(ctx): State<Ctx>,
    json: Result<Json<RedumpRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<RedumpResponse>), ErrorResponse> {
    let reference = match path {
        Ok(Path((namespace, name, revision))) => Reference::new(namespace, name, revision),
        Err(err) => {
            tracing::debug!(error = %err, error_source = logging::error_source(&err), "invalid path parameters");
            return Err(Error::InvalidPath(err).into());
        }
    };

    let RedumpRequest {
        start_block,
        end_block,
    } = match json {
        Ok(Json(request)) => request,
        Err(err) => {
            tracing::debug!(error = %err, error_source = logging::error_source(&err), "invalid request body");
            return Err(Error::InvalidBody(err).into());
        }
    };

    tracing::debug!(
        dataset_reference = %reference,
        start_block,
        end_block,
        "creating redump request"
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
        .ok_or_else(|| Error::NotFound {
            namespace: namespace.clone(),
            name: name.clone(),
            revision: revision.clone(),
        })?;

    // Check for active (non-terminal) jobs for the dataset
    let existing_jobs = ctx
        .scheduler
        .list_jobs_by_dataset(reference.namespace(), reference.name(), reference.hash())
        .await
        .map_err(Error::ListJobs)?;

    let has_active_job = existing_jobs.iter().any(|job| !job.status.is_terminal());

    // Insert the redump request
    let request_id = metadata_db::redump_requests::insert(
        &ctx.metadata_db,
        reference.namespace(),
        reference.name(),
        reference.hash(),
        start_block,
        end_block,
    )
    .await
    .map_err(|err| {
        // Unique constraint violation -> duplicate request
        if let metadata_db::Error::Database(ref db_err) = err
            && let Some(pg_err) = db_err.as_database_error()
        {
            // PostgreSQL unique violation code: 23505
            if pg_err.code().as_deref() == Some("23505") {
                return Error::DuplicateRequest {
                    start_block,
                    end_block,
                };
            }
        }
        Error::Database(err)
    })?;

    tracing::debug!(
        dataset_reference = %reference,
        request_id = %request_id,
        "redump request inserted"
    );

    // If no active job exists, schedule a new one
    let job_id = if !has_active_job {
        let dataset = ctx
            .dataset_store
            .get_dataset(&reference)
            .await
            .map_err(Error::GetDataset)?;

        let jid = ctx
            .scheduler
            .schedule_dataset_sync_job(
                reference.clone(),
                dataset.kind().clone(),
                datasets_common::end_block::EndBlock::default(),
                1,
                None,
            )
            .await
            .map_err(|err| {
                tracing::error!(
                    dataset_reference = %reference,
                    error = %err,
                    error_source = logging::error_source(&err),
                    "failed to schedule dump job for redump"
                );
                Error::Scheduler(err)
            })?;

        tracing::info!(
            dataset_reference = %reference,
            job_id = %jid,
            "scheduled new dump job for redump"
        );

        Some(jid)
    } else {
        None
    };

    Ok((
        StatusCode::ACCEPTED,
        Json(RedumpResponse { request_id, job_id }),
    ))
}

/// Request body for redump operation
#[derive(serde::Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct RedumpRequest {
    /// First block of the range to re-extract (inclusive)
    pub start_block: u64,
    /// Last block of the range to re-extract (inclusive)
    pub end_block: u64,
}

/// Response for redump operation
#[derive(serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct RedumpResponse {
    /// The ID of the created redump request
    #[cfg_attr(feature = "utoipa", schema(value_type = i64))]
    pub request_id: RedumpRequestId,
    /// The ID of the scheduled dump job, if a new job was started.
    /// `null` if an active job already exists for this dataset.
    #[cfg_attr(feature = "utoipa", schema(value_type = Option<i64>))]
    pub job_id: Option<JobId>,
}

/// Errors that can occur when creating a redump request
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid path parameters
    #[error("Invalid path parameters: {0}")]
    InvalidPath(#[source] PathRejection),

    /// Invalid request body
    #[error("Invalid request body: {0}")]
    InvalidBody(#[source] JsonRejection),

    /// Dataset or revision not found
    #[error("Dataset '{namespace}/{name}' at revision '{revision}' not found")]
    NotFound {
        namespace: Namespace,
        name: Name,
        revision: Revision,
    },

    /// Failed to resolve revision to manifest hash
    #[error("Failed to resolve revision: {0}")]
    ResolveRevision(#[source] ResolveRevisionError),

    /// A redump request for this block range already exists
    #[error("A redump request for blocks {start_block}..={end_block} already exists")]
    DuplicateRequest { start_block: u64, end_block: u64 },

    /// Failed to list jobs for dataset
    #[error("Failed to list jobs for dataset: {0}")]
    ListJobs(#[source] ListJobsByDatasetError),

    /// Failed to load dataset from store
    #[error("Failed to load dataset: {0}")]
    GetDataset(#[source] common::dataset_store::GetDatasetError),

    /// Failed to schedule dump job
    #[error("Failed to schedule job: {0}")]
    Scheduler(#[source] ScheduleJobError),

    /// Database error when inserting redump request
    #[error("Database error: {0}")]
    Database(#[source] metadata_db::Error),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidPath(_) => "INVALID_PATH",
            Error::InvalidBody(_) => "INVALID_BODY",
            Error::NotFound { .. } => "DATASET_NOT_FOUND",
            Error::ResolveRevision(_) => "RESOLVE_REVISION_ERROR",
            Error::DuplicateRequest { .. } => "DUPLICATE_REQUEST",
            Error::ListJobs(_) => "LIST_JOBS_ERROR",
            Error::GetDataset(_) => "GET_DATASET_ERROR",
            Error::Scheduler(err) => match err {
                ScheduleJobError::WorkerNotAvailable(_) => "WORKER_NOT_AVAILABLE",
                _ => "SCHEDULER_ERROR",
            },
            Error::Database(_) => "DATABASE_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidPath(_) => StatusCode::BAD_REQUEST,
            Error::InvalidBody(_) => StatusCode::BAD_REQUEST,
            Error::NotFound { .. } => StatusCode::NOT_FOUND,
            Error::ResolveRevision(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::DuplicateRequest { .. } => StatusCode::CONFLICT,
            Error::ListJobs(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::GetDataset(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::Scheduler(err) => match err {
                ScheduleJobError::WorkerNotAvailable(_) => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            Error::Database(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

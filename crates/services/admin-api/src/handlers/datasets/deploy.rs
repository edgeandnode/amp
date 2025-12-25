use std::fmt::Debug;

use axum::{
    Json,
    extract::{
        Path, State,
        rejection::{JsonRejection, PathRejection},
    },
    http::StatusCode,
};
use dataset_store::DatasetKind;
use datasets_common::{name::Name, namespace::Namespace, reference::Reference, revision::Revision};
use monitoring::logging;
use worker::job::JobId;

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
    scheduler::{NodeSelector, ScheduleJobError},
};

/// Handler for the `POST /datasets/{namespace}/{name}/versions/{revision}/deploy` endpoint
///
/// Schedules a data extraction job for the specified dataset revision.
///
/// ## Response
/// - **202 Accepted**: Job successfully scheduled
/// - **400 Bad Request**: Invalid path parameters or request body
/// - **404 Not Found**: Dataset or revision not found
/// - **500 Internal Server Error**: Database or scheduler error
///
/// ## Error Codes
/// - `INVALID_PATH`: Invalid path parameters (namespace, name, or revision)
/// - `INVALID_BODY`: Invalid request body (malformed JSON or missing required fields)
/// - `DATASET_NOT_FOUND`: The specified dataset or revision does not exist
/// - `LIST_VERSION_TAGS_ERROR`: Failed to list version tags from dataset store
/// - `RESOLVE_REVISION_ERROR`: Failed to resolve revision to manifest hash
/// - `GET_DATASET_ERROR`: Failed to load dataset from store
/// - `WORKER_NOT_AVAILABLE`: Specified worker not found or inactive
/// - `SCHEDULER_ERROR`: Failed to schedule extraction job
///
/// ## Behavior
/// This endpoint schedules a data extraction job for a dataset:
/// 1. Resolves the revision to find the corresponding version tag
/// 2. Loads the full dataset configuration from the dataset store
/// 3. Schedules an extraction job with the specified parameters
/// 4. Returns job ID for tracking
///
/// The revision parameter supports four types:
/// - Semantic version (e.g., "1.2.3") - uses that specific version
/// - "latest" - resolves to the highest semantic version
/// - "dev" - resolves to the development version tag
/// - Manifest hash (SHA256 hash) - finds the version that points to this hash
///
/// Jobs are executed asynchronously by worker nodes. Use the returned job ID
/// to track progress via the jobs endpoints.
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        post,
        path = "/datasets/{namespace}/{name}/versions/{revision}/deploy",
        tag = "datasets",
        operation_id = "deploy_dataset",
        params(
            ("namespace" = String, Path, description = "Dataset namespace"),
            ("name" = String, Path, description = "Dataset name"),
            ("revision" = String, Path, description = "Revision (version, hash, latest, or dev)")
        ),
        request_body = DeployRequest,
        responses(
            (status = 202, description = "Job successfully scheduled", body = DeployResponse),
            (status = 400, description = "Bad request (invalid parameters)", body = ErrorResponse),
            (status = 404, description = "Dataset or revision not found", body = ErrorResponse),
            (status = 500, description = "Internal server error", body = ErrorResponse)
        )
    )
)]
pub async fn handler(
    path: Result<Path<(Namespace, Name, Revision)>, PathRejection>,
    State(ctx): State<Ctx>,
    json: Result<Json<DeployRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<DeployResponse>), ErrorResponse> {
    let reference = match path {
        Ok(Path((namespace, name, revision))) => Reference::new(namespace, name, revision),
        Err(err) => {
            tracing::debug!(error = %err, error_source = logging::error_source(&err), "invalid path parameters");
            return Err(Error::InvalidPath(err).into());
        }
    };

    let DeployRequest {
        end_block,
        parallelism,
        worker_id,
    } = match json {
        Ok(Json(request)) => request,
        Err(err) => {
            tracing::debug!(error = %err, error_source = logging::error_source(&err), "invalid request body");
            return Err(Error::InvalidBody(err).into());
        }
    };

    tracing::debug!(
        dataset_reference = %reference,
        end_block=%end_block,
        parallelism=%parallelism,
        worker_id=?worker_id,
        "deploying dataset"
    );

    let namespace = reference.namespace().clone();
    let name = reference.name().clone();
    let revision = reference.revision().clone();

    // Resolve reference to hash reference
    let reference = ctx
        .dataset_store
        .resolve_revision(&reference)
        .await
        .map_err(Error::ResolveRevision)?
        .ok_or_else(|| Error::NotFound {
            namespace: namespace.clone(),
            name: name.clone(),
            revision: revision.clone(),
        })?;

    // Load the full dataset object using the resolved hash reference
    let dataset = ctx
        .dataset_store
        .get_dataset(&reference)
        .await
        .map_err(Error::GetDataset)?;

    // Parse dataset kind (must always succeed - panic if DB is in bad state)
    let dataset_kind: DatasetKind = dataset
        .kind
        .parse()
        .expect("dataset kind in database must be valid");

    // Schedule the extraction job using the scheduler
    let job_id = ctx
        .scheduler
        .schedule_dataset_sync_job(
            reference.clone(),
            dataset_kind,
            end_block.into(),
            parallelism,
            worker_id,
        )
        .await
        .map_err(|err| {
            tracing::error!(
                dataset_reference = %reference,
                error = %err,
                error_source = logging::error_source(&err),
                "failed to schedule dataset deployment"
            );
            Error::Scheduler(err)
        })?;

    Ok((StatusCode::ACCEPTED, Json(DeployResponse { job_id })))
}

/// End block configuration for API requests.
///
/// Determines when the dump process should stop extracting blocks.
/// Accepts the following values:
///
/// - `null` (or omitted): Continuous dumping - never stops, keeps extracting new blocks as they arrive
/// - `"latest"`: Stop at the latest available block at the time the dump starts
/// - A positive number as a string (e.g., `"1000000"`): Stop at the specified absolute block number
/// - A negative number as a string (e.g., `"-100"`): Stop at (latest block - N), useful for staying N blocks behind the chain tip
#[derive(Default, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "utoipa", schema(value_type = Option<String>))]
pub struct EndBlock(dump::EndBlock);

impl From<EndBlock> for dump::EndBlock {
    fn from(value: EndBlock) -> Self {
        value.0
    }
}

impl std::fmt::Display for EndBlock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

/// Request for deploying a dataset
#[derive(serde::Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct DeployRequest {
    /// The end block configuration for the deployment
    ///
    /// Supports multiple modes:
    /// - `null` or omitted: Continuous dumping (never stops)
    /// - `"latest"`: Stop at the latest available block
    /// - `<number>`: Stop at specific block number (e.g., `1000000`)
    /// - `<negative number>`: Stop N blocks before latest (e.g., `-100` means latest - 100)
    ///
    /// If not specified, defaults to continuous mode.
    #[serde(default)]
    pub end_block: EndBlock,

    /// Number of parallel workers to run
    ///
    /// Each worker will be responsible for an equal number of blocks.
    /// For example, if extracting blocks 0-10,000,000 with parallelism=10,
    /// each worker will handle a contiguous section of 1 million blocks.
    ///
    /// Only applicable to raw datasets (EVM RPC, Firehose, etc.).
    /// Derived datasets ignore this parameter.
    ///
    /// Defaults to 1 if not specified.
    #[serde(default = "default_parallelism")]
    pub parallelism: u16,

    /// Optional worker selector - either an exact worker ID or a glob pattern.
    ///
    /// Examples:
    /// - `"worker-node-0"` - assigns to the specific worker with ID "worker-node-0"
    /// - `"worker-eth-*"` - randomly selects from workers matching the pattern
    ///
    /// If not specified, a worker will be selected randomly from all available workers.
    /// If a glob pattern is provided, one matching worker will be randomly selected
    /// The worker must be active (has sent heartbeats recently) for the deployment to succeed.
    #[serde(default)]
    #[cfg_attr(feature = "utoipa", schema(value_type = Option<String>))]
    pub worker_id: Option<NodeSelector>,
}

fn default_parallelism() -> u16 {
    1
}

/// Response for deploy operation
#[derive(serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct DeployResponse {
    /// The ID of the scheduled dump job (64-bit integer)
    #[cfg_attr(feature = "utoipa", schema(value_type = i64))]
    pub job_id: JobId,
}

/// Errors that can occur when deploying a dataset
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid path parameters
    ///
    /// This occurs when:
    /// - The namespace, name, or revision in the URL path is invalid
    /// - Path parameter parsing fails
    #[error("Invalid path parameters: {0}")]
    InvalidPath(#[source] PathRejection),
    /// Invalid request body
    ///
    /// This occurs when:
    /// - The request body is not valid JSON
    /// - The JSON structure doesn't match the expected schema
    /// - Required fields are missing or have invalid types
    #[error("Invalid request body: {0}")]
    InvalidBody(#[source] JsonRejection),
    /// Dataset or revision not found
    ///
    /// This occurs when:
    /// - The specified dataset name doesn't exist in the namespace
    /// - The specified revision doesn't exist for this dataset
    /// - The revision resolves to a manifest that doesn't exist
    #[error("Dataset '{namespace}/{name}' at revision '{revision}' not found")]
    NotFound {
        namespace: Namespace,
        name: Name,
        revision: Revision,
    },
    /// Dataset store operation error when listing version tags
    ///
    /// This occurs when:
    /// - Failed to query version tags from the dataset store
    /// - Database connection issues
    /// - Internal database errors
    #[error("Failed to list version tags: {0}")]
    ListVersionTags(#[source] dataset_store::ListVersionTagsError),
    /// Dataset store operation error when resolving revision
    ///
    /// This occurs when:
    /// - Failed to resolve revision to manifest hash
    /// - Database connection issues
    /// - Internal database errors
    #[error("Failed to resolve revision: {0}")]
    ResolveRevision(#[source] dataset_store::ResolveRevisionError),
    /// Dataset store operation error when loading dataset
    ///
    /// This occurs when:
    /// - Failed to load dataset configuration from manifest
    /// - Manifest parsing errors
    /// - Invalid dataset structure
    #[error("Failed to load dataset: {0}")]
    GetDataset(#[source] dataset_store::GetDatasetError),
    /// Scheduler error
    ///
    /// This occurs when:
    /// - Failed to create or schedule extraction job
    /// - Scheduler is unavailable or overloaded
    /// - Invalid job configuration
    #[error("Failed to schedule job: {0}")]
    Scheduler(#[source] ScheduleJobError),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidPath(_) => "INVALID_PATH",
            Error::InvalidBody(_) => "INVALID_BODY",
            Error::NotFound { .. } => "DATASET_NOT_FOUND",
            Error::ListVersionTags(_) => "LIST_VERSION_TAGS_ERROR",
            Error::ResolveRevision(_) => "RESOLVE_REVISION_ERROR",
            Error::GetDataset(_) => "GET_DATASET_ERROR",
            Error::Scheduler(err) => match err {
                ScheduleJobError::WorkerNotAvailable(_) => "WORKER_NOT_AVAILABLE",
                _ => "SCHEDULER_ERROR",
            },
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidPath(_) => StatusCode::BAD_REQUEST,
            Error::InvalidBody(_) => StatusCode::BAD_REQUEST,
            Error::NotFound { .. } => StatusCode::NOT_FOUND,
            Error::ListVersionTags(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::ResolveRevision(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::GetDataset(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::Scheduler(err) => match err {
                ScheduleJobError::WorkerNotAvailable(_) => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
        }
    }
}

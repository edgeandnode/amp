use axum::{
    Json,
    extract::{Path, State, rejection::PathRejection},
    http::StatusCode,
};
use common::manifest::common::{Name, Version};
use http_common::{BoxRequestError, RequestError};
use metadata_db::JobId;

use super::tracing::display_selector_version;
use crate::{ctx::Ctx, scheduler::ScheduleJobError};

/// Handler for dataset dump endpoint without version
///
/// Triggers a data extraction job for the specified dataset using the latest version.
/// URL pattern: `POST /datasets/{name}/dump`
///
/// ## Path Parameters
/// - `name`: Dataset name
///
/// ## Request Body
/// - `end_block`: (optional) The last block number to extract (if not specified, extracts indefinitely)
///
/// ## Response
/// - **200 OK**: Returns the ID of the scheduled dump job
/// - **400 Bad Request**: Invalid dataset name or invalid request parameters
/// - **404 Not Found**: Dataset with the given name does not exist
/// - **500 Internal Server Error**: Scheduler, database, or store error
///
/// ## Error Codes
/// - `INVALID_SELECTOR`: The provided dataset name is not valid (invalid name format or parsing error)
/// - `DATASET_NOT_FOUND`: No dataset exists with the given name
/// - `DATASET_STORE_ERROR`: Failed to load dataset from store
/// - `SCHEDULER_ERROR`: Failed to schedule the dump job
#[tracing::instrument(skip_all, err)]
pub async fn handler(
    State(ctx): State<Ctx>,
    path: Result<Path<Name>, PathRejection>,
    Json(options): Json<DumpOptions>,
) -> Result<Json<DumpResponse>, BoxRequestError> {
    let name = match path {
        Ok(Path(name)) => name,
        Err(err) => {
            tracing::debug!(error=?err, "invalid dataset path parameters");
            return Err(Error::InvalidSelector(err).into());
        }
    };

    handler_inner(ctx, name, None, options).await
}

/// Handler for dataset dump endpoint with specific version
///
/// Triggers a data extraction job for the specified dataset version.
/// URL pattern: `POST /datasets/{name}/versions/{version}/dump`
///
/// ## Path Parameters
/// - `name`: Dataset name
/// - `version`: Specific dataset version
///
/// ## Request Body
/// - `end_block`: (optional) The last block number to extract (if not specified, extracts indefinitely)
///
/// ## Response
/// - **200 OK**: Returns the ID of the scheduled dump job
/// - **400 Bad Request**: Invalid dataset name/version or invalid request parameters
/// - **404 Not Found**: Dataset with the given name/version does not exist
/// - **500 Internal Server Error**: Scheduler, database, or store error
///
/// ## Error Codes
/// - `INVALID_SELECTOR`: The provided dataset name or version is not valid (invalid name format, malformed version, or parsing error)
/// - `DATASET_NOT_FOUND`: No dataset exists with the given name/version
/// - `DATASET_STORE_ERROR`: Failed to load dataset from store
/// - `SCHEDULER_ERROR`: Failed to schedule the dump job
#[tracing::instrument(skip_all, err)]
pub async fn handler_with_version(
    State(ctx): State<Ctx>,
    path: Result<Path<(Name, Version)>, PathRejection>,
    Json(options): Json<DumpOptions>,
) -> Result<Json<DumpResponse>, BoxRequestError> {
    let (name, version) = match path {
        Ok(Path((name, version))) => (name, version),
        Err(err) => {
            tracing::debug!(error=?err, "invalid dataset path parameters");
            return Err(Error::InvalidSelector(err).into());
        }
    };

    handler_inner(ctx, name, Some(version), options).await
}

/// Common logic for handling dataset dump requests
async fn handler_inner(
    ctx: Ctx,
    name: Name,
    version: Option<Version>,
    options: DumpOptions,
) -> Result<Json<DumpResponse>, BoxRequestError> {
    tracing::debug!(
        dataset_name=%name,
        dataset_version=%display_selector_version(&version),
        "loading dataset from store"
    );

    // Load the dataset from the store
    // If version is None, the latest version is used
    let dataset = ctx
        .store
        .load_dataset(&name, version.as_ref())
        .await
        .map_err(|err| {
            tracing::debug!(
                dataset_name=%name,
                dataset_version=%display_selector_version(&version),
                error=?err,
                "failed to load dataset"
            );
            if err.is_not_found() {
                Error::NotFound {
                    name: name.clone(),
                    version: version.clone(),
                }
            } else {
                Error::DatasetStoreError(err)
            }
        })?;

    let job_id = ctx
        .scheduler
        .schedule_dataset_dump(dataset, options.end_block)
        .await
        .map_err(|err| {
            tracing::error!(
                dataset_name=%name,
                dataset_version=%display_selector_version(&version),
                error=?err,
                "failed to schedule dataset dump"
            );
            Error::SchedulerError(err)
        })?;

    Ok(Json(DumpResponse { job_id }))
}

/// Request options for dataset dump operations
///
/// Controls the behavior and scope of the data extraction job.
/// These options determine the range of blocks to extract.
#[derive(serde::Deserialize)]
pub struct DumpOptions {
    /// The last block number to extract (optional)
    ///
    /// If not specified, the extraction will continue indefinitely
    /// (until manually stopped or the blockchain tip).
    #[serde(default)]
    end_block: Option<i64>,
}

/// Response returned by the dataset dump endpoint
///
/// Contains the ID of the scheduled dump job for tracking purposes.
#[derive(serde::Serialize)]
pub struct DumpResponse {
    /// The ID of the scheduled dump job
    pub job_id: JobId,
}

/// Errors that can occur during dataset dump operations
///
/// This enum represents all possible error conditions when handling
/// a request to schedule or wait for a dataset dump job.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The dataset selector is invalid
    ///
    /// This occurs when:
    /// - The dataset name contains invalid characters or doesn't follow naming conventions
    /// - The dataset name is empty or malformed
    /// - The version syntax is invalid (e.g., malformed semver)
    /// - Path parameter extraction fails for dataset selection
    #[error("invalid dataset selector: {0}")]
    InvalidSelector(PathRejection),

    /// Dataset not found
    ///
    /// This occurs when:
    /// - No dataset exists with the given name/version
    /// - The dataset has been deleted or moved
    /// - Dataset configuration is missing
    #[error("dataset '{name}'{} not found", version.as_ref().map(|v| format!(" version '{}'", v)).unwrap_or_default()
    )]
    NotFound {
        name: Name,
        version: Option<Version>,
    },

    /// Dataset store error while loading the dataset
    ///
    /// This occurs when:
    /// - The dataset store is not accessible
    /// - There's a configuration error in the store
    /// - I/O errors while reading dataset definitions
    /// - The dataset doesn't exist in the store
    #[error("dataset store error: {0}")]
    DatasetStoreError(#[from] dataset_store::DatasetError),

    /// Scheduler error while scheduling the dump job
    ///
    /// This occurs when:
    /// - The scheduler service is not available
    /// - Job queue is full or rejecting new jobs
    /// - Configuration errors in the scheduler
    #[error("scheduler error: {0}")]
    SchedulerError(#[from] ScheduleJobError),
}

impl RequestError for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidSelector(_) => "INVALID_SELECTOR",
            Error::NotFound { .. } => "DATASET_NOT_FOUND",
            Error::DatasetStoreError(_) => "DATASET_STORE_ERROR",
            Error::SchedulerError(_) => "SCHEDULER_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidSelector(_) => StatusCode::BAD_REQUEST,
            Error::NotFound { .. } => StatusCode::NOT_FOUND,
            Error::DatasetStoreError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::SchedulerError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

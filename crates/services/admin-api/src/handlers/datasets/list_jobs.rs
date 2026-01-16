//! Dataset jobs listing handler

use amp_datasets_registry::error::ResolveRevisionError;
use axum::{
    Json,
    extract::{Path, State, rejection::PathRejection},
    http::StatusCode,
};
use datasets_common::{name::Name, namespace::Namespace, reference::Reference, revision::Revision};
use monitoring::logging;

use crate::{
    ctx::Ctx,
    handlers::{
        error::{ErrorResponse, IntoErrorResponse},
        jobs::job_info::JobInfo,
    },
    scheduler,
};

/// Handler for the `GET /datasets/{namespace}/{name}/versions/{revision}/jobs` endpoint
///
/// Retrieves and returns all jobs for a specific dataset revision.
///
/// ## Path Parameters
/// - `namespace`: Dataset namespace
/// - `name`: Dataset name
/// - `revision`: Dataset revision (version, hash, "latest", or "dev")
///
/// ## Response
/// - **200 OK**: Returns all jobs for the dataset
/// - **400 Bad Request**: Invalid path parameters
/// - **404 Not Found**: Dataset or revision not found
/// - **500 Internal Server Error**: Database connection or query error
///
/// ## Error Codes
/// - `INVALID_PATH`: Invalid path parameters (namespace, name, or revision malformed)
/// - `DATASET_NOT_FOUND`: Dataset revision does not exist
/// - `RESOLVE_REVISION_ERROR`: Failed to resolve dataset revision (database error)
/// - `LIST_JOBS_ERROR`: Failed to list jobs from metadata database (database error)
///
/// This handler:
/// - Validates and extracts the dataset reference from the URL path
/// - Resolves the revision to a manifest hash using the dataset store
/// - Queries all jobs filtered by the manifest hash
/// - Returns all matching jobs
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        get,
        path = "/datasets/{namespace}/{name}/versions/{revision}/jobs",
        tag = "datasets",
        operation_id = "list_dataset_jobs",
        params(
            ("namespace" = String, Path, description = "Dataset namespace"),
            ("name" = String, Path, description = "Dataset name"),
            ("revision" = String, Path, description = "Revision (version, hash, latest, or dev)")
        ),
        responses(
            (status = 200, description = "Successfully retrieved jobs", body = JobsResponse),
            (status = 400, description = "Invalid path parameters", body = crate::handlers::error::ErrorResponse),
            (status = 404, description = "Dataset or revision not found", body = crate::handlers::error::ErrorResponse),
            (status = 500, description = "Internal server error", body = crate::handlers::error::ErrorResponse)
        )
    )
)]
pub async fn handler(
    path: Result<Path<(Namespace, Name, Revision)>, PathRejection>,
    State(ctx): State<Ctx>,
) -> Result<Json<JobsResponse>, ErrorResponse> {
    let reference = match path {
        Ok(Path((namespace, name, revision))) => Reference::new(namespace, name, revision),
        Err(err) => {
            tracing::debug!(
                error = %err,
                error_source = logging::error_source(&err),
                "invalid path parameters"
            );
            return Err(Error::InvalidPath { err }.into());
        }
    };

    // Resolve dataset revision to manifest hash
    let reference = ctx
        .datasets_registry
        .resolve_revision(&reference)
        .await
        .map_err(|err| {
            tracing::error!(
                dataset_reference = %reference,
                error = %err,
                error_source = logging::error_source(&err),
                "failed to resolve dataset revision"
            );
            Error::ResolveRevision(err)
        })?
        .ok_or_else(|| Error::DatasetNotFound {
            reference: reference.clone(),
        })?;

    // Fetch all jobs from metadata database filtered by dataset
    let jobs = ctx
        .scheduler
        .list_jobs_by_dataset(reference.namespace(), reference.name(), reference.hash())
        .await
        .map_err(|err| {
            tracing::error!(
                dataset_reference = %reference,
                error = %err,
                error_source = logging::error_source(&err),
                "failed to list jobs for dataset"
            );
            Error::ListJobs(err)
        })?;

    let jobs = jobs.into_iter().map(Into::into).collect::<Vec<_>>();

    Ok(Json(JobsResponse { jobs }))
}

/// API response containing job information
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct JobsResponse {
    /// List of jobs
    pub jobs: Vec<JobInfo>,
}

/// Errors that can occur during dataset jobs listing
///
/// This enum represents all possible error conditions when handling
/// a request to list jobs for a specific dataset revision.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The path parameters are invalid or malformed
    ///
    /// This occurs when:
    /// - The namespace parameter has an invalid format
    /// - The name parameter has an invalid format
    /// - The revision parameter has an invalid format
    /// - Path parameters are missing or incorrectly structured
    #[error("invalid path parameters: {err}")]
    InvalidPath {
        /// The rejection details from Axum's path extractor
        err: PathRejection,
    },

    /// Failed to resolve dataset revision to manifest hash
    ///
    /// This occurs when:
    /// - Database connection fails during revision resolution
    /// - Query execution encounters an internal database error
    /// - Metadata database query for revision fails
    #[error("failed to resolve dataset revision: {0}")]
    ResolveRevision(#[source] ResolveRevisionError),

    /// Dataset revision does not exist
    ///
    /// This occurs when:
    /// - The specified dataset revision cannot be found
    /// - The version tag does not exist for the dataset
    /// - The manifest hash is not registered in the system
    /// - "latest" or "dev" special revisions have no associated manifest
    #[error("dataset '{reference}' not found")]
    DatasetNotFound {
        /// The dataset reference that was not found
        reference: Reference,
    },

    /// Failed to list jobs for dataset
    ///
    /// This occurs when:
    /// - Database connection fails or is lost during query
    /// - Query execution encounters an internal database error
    /// - Connection pool is exhausted or unavailable
    /// - Metadata database query for jobs filtered by manifest hash fails
    #[error("failed to list jobs for dataset")]
    ListJobs(#[source] scheduler::ListJobsByDatasetError),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidPath { .. } => "INVALID_PATH",
            Error::ResolveRevision(_) => "RESOLVE_REVISION_ERROR",
            Error::DatasetNotFound { .. } => "DATASET_NOT_FOUND",
            Error::ListJobs(_) => "LIST_JOBS_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidPath { .. } => StatusCode::BAD_REQUEST,
            Error::ResolveRevision(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::DatasetNotFound { .. } => StatusCode::NOT_FOUND,
            Error::ListJobs(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

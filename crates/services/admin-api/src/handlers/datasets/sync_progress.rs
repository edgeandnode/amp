//! Dataset sync progress handler

use axum::{
    Json,
    extract::{Path, State, rejection::PathRejection},
    http::StatusCode,
};
use datasets_common::{name::Name, namespace::Namespace, reference::Reference, revision::Revision};
use monitoring::logging;

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
};

/// Handler for the `GET /datasets/{namespace}/{name}/versions/{revision}/sync-progress` endpoint
///
/// Retrieves sync progress information for a specific dataset revision, including
/// per-table current block numbers, job status, and file statistics.
///
/// ## Path Parameters
/// - `namespace`: Dataset namespace
/// - `name`: Dataset name
/// - `revision`: Dataset revision (version, hash, "latest", or "dev")
///
/// ## Response
/// - **200 OK**: Returns sync progress for all tables in the dataset
/// - **400 Bad Request**: Invalid path parameters
/// - **404 Not Found**: Dataset or revision not found
/// - **500 Internal Server Error**: Database connection or query error
///
/// ## Error Codes
/// - `INVALID_PATH`: Invalid path parameters (namespace, name, or revision malformed)
/// - `DATASET_NOT_FOUND`: Dataset revision does not exist
/// - `RESOLVE_REVISION_ERROR`: Failed to resolve dataset revision (database error)
/// - `GET_SYNC_PROGRESS_ERROR`: Failed to get sync progress from metadata database
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        get,
        path = "/datasets/{namespace}/{name}/versions/{revision}/sync-progress",
        tag = "datasets",
        operation_id = "get_dataset_sync_progress",
        params(
            ("namespace" = String, Path, description = "Dataset namespace"),
            ("name" = String, Path, description = "Dataset name"),
            ("revision" = String, Path, description = "Revision (version, hash, latest, or dev)")
        ),
        responses(
            (status = 200, description = "Successfully retrieved sync progress", body = SyncProgressResponse),
            (status = 400, description = "Invalid path parameters", body = crate::handlers::error::ErrorResponse),
            (status = 404, description = "Dataset or revision not found", body = crate::handlers::error::ErrorResponse),
            (status = 500, description = "Internal server error", body = crate::handlers::error::ErrorResponse)
        )
    )
)]
pub async fn handler(
    path: Result<Path<(Namespace, Name, Revision)>, PathRejection>,
    State(ctx): State<Ctx>,
) -> Result<Json<SyncProgressResponse>, ErrorResponse> {
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
    let resolved = ctx
        .dataset_store
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

    // Query sync progress from metadata database
    let progress =
        metadata_db::sync_progress::get_by_manifest_hash(&ctx.metadata_db, resolved.hash())
            .await
            .map_err(|err| {
                tracing::error!(
                    dataset_reference = %resolved,
                    error = %err,
                    error_source = logging::error_source(&err),
                    "failed to get sync progress for dataset"
                );
                Error::GetSyncProgress(err)
            })?;

    // Convert to response format
    let tables = progress
        .into_iter()
        .map(|p| TableSyncProgress {
            table_name: p.table_name.to_string(),
            current_block: p.current_block,
            start_block: p.start_block,
            job_id: p.job_id.map(|id| id.into_i64()),
            job_status: p.job_status.map(|s| s.to_string()),
            files_count: p.files_count,
            total_size_bytes: p.total_size_bytes,
        })
        .collect();

    Ok(Json(SyncProgressResponse {
        dataset_namespace: resolved.namespace().to_string(),
        dataset_name: resolved.name().to_string(),
        revision: reference.revision().to_string(),
        manifest_hash: resolved.hash().to_string(),
        tables,
    }))
}

/// API response containing sync progress information for a dataset
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct SyncProgressResponse {
    /// Dataset namespace
    pub dataset_namespace: String,
    /// Dataset name
    pub dataset_name: String,
    /// Requested revision
    pub revision: String,
    /// Resolved manifest hash
    pub manifest_hash: String,
    /// Sync progress for each table in the dataset
    pub tables: Vec<TableSyncProgress>,
}

/// Sync progress information for a single table
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct TableSyncProgress {
    /// Name of the table within the dataset
    pub table_name: String,
    /// Highest block number that has been synced (null if no data yet)
    pub current_block: Option<i64>,
    /// Lowest block number that has been synced (null if no data yet)
    pub start_block: Option<i64>,
    /// ID of the writer job (null if no active job)
    pub job_id: Option<i64>,
    /// Status of the writer job (null if no active job)
    pub job_status: Option<String>,
    /// Number of Parquet files written for this table
    pub files_count: i64,
    /// Total size of all Parquet files in bytes
    pub total_size_bytes: i64,
}

/// Errors that can occur during sync progress retrieval
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The path parameters are invalid or malformed
    #[error("invalid path parameters: {err}")]
    InvalidPath {
        /// The rejection details from Axum's path extractor
        err: PathRejection,
    },

    /// Failed to resolve dataset revision to manifest hash
    #[error("failed to resolve dataset revision: {0}")]
    ResolveRevision(#[source] amp_dataset_store::ResolveRevisionError),

    /// Dataset revision does not exist
    #[error("dataset '{reference}' not found")]
    DatasetNotFound {
        /// The dataset reference that was not found
        reference: Reference,
    },

    /// Failed to get sync progress from metadata database
    #[error("failed to get sync progress for dataset")]
    GetSyncProgress(#[source] metadata_db::Error),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidPath { .. } => "INVALID_PATH",
            Error::ResolveRevision(_) => "RESOLVE_REVISION_ERROR",
            Error::DatasetNotFound { .. } => "DATASET_NOT_FOUND",
            Error::GetSyncProgress(_) => "GET_SYNC_PROGRESS_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidPath { .. } => StatusCode::BAD_REQUEST,
            Error::ResolveRevision(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::DatasetNotFound { .. } => StatusCode::NOT_FOUND,
            Error::GetSyncProgress(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

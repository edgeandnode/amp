//! Dataset sync progress handler

use std::collections::HashMap;

use axum::{
    Json,
    extract::{Path, State, rejection::PathRejection},
    http::StatusCode,
};
use common::catalog::physical::PhysicalTable;
use datasets_common::{
    name::Name, namespace::Namespace, partial_reference::PartialReference, reference::Reference,
    revision::Revision,
};
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
/// - `GET_DATASET_ERROR`: Failed to retrieve dataset definition
/// - `GET_SYNC_PROGRESS_ERROR`: Failed to get sync progress from metadata database
/// - `PHYSICAL_TABLE_ERROR`: Failed to access physical table metadata
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
    let resolved_ref = ctx
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

    // Get the full dataset definition to access tables
    let dataset = ctx
        .dataset_store
        .get_dataset(&resolved_ref)
        .await
        .map_err(|err| {
            tracing::error!(
                dataset_reference = %resolved_ref,
                error = %err,
                error_source = logging::error_source(&err),
                "failed to get dataset definition"
            );
            Error::GetDataset(err)
        })?;

    // Query active tables info from metadata database (job_id, status)
    let writer_infos = metadata_db::sync_progress::get_active_tables_with_writer_info(
        &ctx.metadata_db,
        resolved_ref.hash(),
    )
    .await
    .map_err(|err| {
        tracing::error!(
            dataset_reference = %resolved_ref,
            error = %err,
            error_source = logging::error_source(&err),
            "failed to get sync progress for dataset"
        );
        Error::GetSyncProgress(err)
    })?;

    // Index writer info by table name for quick lookup
    let writer_info_map: HashMap<_, _> = writer_infos
        .into_iter()
        .map(|info| (info.table_name.to_string(), info))
        .collect();

    let partial_ref = PartialReference::new(
        reference.namespace().clone(),
        reference.name().clone(),
        Some(reference.revision().clone()),
    );
    let mut tables = Vec::new();

    // Iterate over all tables defined in the dataset
    for resolved_table in dataset.resolved_tables(partial_ref) {
        let table_name = resolved_table.name().clone();
        let writer_info = writer_info_map.get(table_name.as_str());

        // Get the active physical table revision if it exists
        let physical_table = ctx
            .data_store
            .get_table_active_revision(&resolved_ref, &table_name)
            .await
            .map_err(|err| {
                tracing::error!(
                    table = %table_name,
                    error = %err,
                    error_source = logging::error_source(&err),
                    "failed to get active physical table"
                );
                Error::PhysicalTable(err.into())
            })?
            .map(|revision| {
                PhysicalTable::from_active_revision(
                    ctx.data_store.clone(),
                    resolved_table.clone(),
                    revision,
                )
            });

        let (current_block, start_block, files_count, total_size_bytes) =
            if let Some(pt) = physical_table {
                // Take a snapshot to get accurate synced range
                let snapshot = pt
                    .snapshot(false, ctx.data_store.clone())
                    .await
                    .map_err(|err| {
                        tracing::error!(
                            table = %table_name,
                            error = %err,
                            error_source = logging::error_source(&*err),
                            "failed to snapshot physical table"
                        );
                        Error::PhysicalTable(err)
                    })?;

                let synced_range = snapshot.synced_range();
                let canonical_segments = snapshot.canonical_segments();

                let files_count = canonical_segments.len() as i64;
                let total_size_bytes = canonical_segments
                    .iter()
                    .map(|s| s.object.size as i64)
                    .sum();

                let (start, end) = match synced_range {
                    Some(range) => (
                        Some(range.start().try_into().unwrap_or(0)),
                        Some(range.end().try_into().unwrap_or(0)),
                    ),
                    None => (None, None),
                };

                (end, start, files_count, total_size_bytes)
            } else {
                // Table hasn't been created/synced yet
                (None, None, 0, 0)
            };

        tables.push(TableSyncProgress {
            table_name: table_name.to_string(),
            current_block,
            start_block,
            job_id: writer_info.and_then(|i| i.job_id).map(|id| id.into_i64()),
            job_status: writer_info
                .and_then(|i| i.job_status)
                .map(|s| s.to_string()),
            files_count,
            total_size_bytes,
        });
    }

    Ok(Json(SyncProgressResponse {
        dataset_namespace: resolved_ref.namespace().to_string(),
        dataset_name: resolved_ref.name().to_string(),
        revision: reference.revision().to_string(),
        manifest_hash: resolved_ref.hash().to_string(),
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

    /// Failed to get dataset definition
    #[error("failed to get dataset definition")]
    GetDataset(#[source] amp_dataset_store::GetDatasetError),

    /// Failed to get sync progress from metadata database
    #[error("failed to get sync progress for dataset")]
    GetSyncProgress(#[source] metadata_db::Error),

    /// Failed to access physical table metadata
    #[error("failed to access physical table")]
    PhysicalTable(#[source] common::BoxError),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidPath { .. } => "INVALID_PATH",
            Error::ResolveRevision(_) => "RESOLVE_REVISION_ERROR",
            Error::DatasetNotFound { .. } => "DATASET_NOT_FOUND",
            Error::GetDataset(_) => "GET_DATASET_ERROR",
            Error::GetSyncProgress(_) => "GET_SYNC_PROGRESS_ERROR",
            Error::PhysicalTable(_) => "PHYSICAL_TABLE_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidPath { .. } => StatusCode::BAD_REQUEST,
            Error::ResolveRevision(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::DatasetNotFound { .. } => StatusCode::NOT_FOUND,
            Error::GetDataset(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::GetSyncProgress(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::PhysicalTable(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

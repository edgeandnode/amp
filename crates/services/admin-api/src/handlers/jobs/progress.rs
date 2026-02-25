//! Job progress handler

use std::collections::HashMap;

use amp_worker_core::jobs::job_id::JobId;
use axum::{
    Json,
    extract::{Path, State, rejection::PathRejection},
    http::StatusCode,
};
use common::physical_table::PhysicalTable;
use datasets_common::{hash_reference::HashReference, table_name::TableName};
use monitoring::logging;

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
    scheduler,
};

/// Handler for the `GET /jobs/{id}/progress` endpoint
///
/// Retrieves progress information for all tables written by a specific job,
/// including current block numbers, file counts, and size statistics.
///
/// ## Path Parameters
/// - `id`: The unique identifier of the job
///
/// ## Response
/// - **200 OK**: Returns progress for all tables written by this job
/// - **400 Bad Request**: Invalid job ID format
/// - **404 Not Found**: Job with the given ID does not exist
/// - **500 Internal Server Error**: Database connection or query error
///
/// ## Error Codes
/// - `INVALID_JOB_ID`: The provided ID is not a valid job identifier
/// - `JOB_NOT_FOUND`: No job exists with the given ID
/// - `GET_JOB_ERROR`: Failed to retrieve job from scheduler
/// - `GET_TABLES_ERROR`: Failed to get tables written by this job
/// - `GET_DATASET_ERROR`: Failed to get dataset definition
/// - `GET_ACTIVE_REVISION_ERROR`: Failed to get active physical table revision
/// - `SNAPSHOT_TABLE_ERROR`: Failed to snapshot physical table
/// - `MULTI_NETWORK_SEGMENTS_ERROR`: Table contains multi-network segments; synced range
///   cannot be computed. This indicates a catalog inconsistency — the state is
///   distinguishable from "no data synced yet" (which returns 200 with null block fields).
/// - `BLOCK_NUMBER_OVERFLOW`: A block number in the synced range cannot be represented as
///   i64. This indicates data-integrity corruption; the block number is reported in the
///   error response rather than silently rewritten to 0.
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        get,
        path = "/jobs/{id}/progress",
        tag = "jobs",
        operation_id = "get_job_progress",
        params(
            ("id" = String, Path, description = "Job ID")
        ),
        responses(
            (status = 200, description = "Successfully retrieved job progress", body = JobProgressResponse),
            (status = 400, description = "Invalid job ID", body = crate::handlers::error::ErrorResponse),
            (status = 404, description = "Job not found", body = crate::handlers::error::ErrorResponse),
            (status = 500, description = "Internal server error", body = crate::handlers::error::ErrorResponse)
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    path: Result<Path<JobId>, PathRejection>,
) -> Result<Json<JobProgressResponse>, ErrorResponse> {
    let job_id = match path {
        Ok(Path(id)) => id,
        Err(err) => {
            tracing::debug!(
                error = %err,
                error_source = logging::error_source(&err),
                "invalid job ID in path"
            );
            return Err(Error::InvalidId { err }.into());
        }
    };

    // Verify the job exists and get its status
    let job = match ctx.scheduler.get_job(job_id).await {
        Ok(Some(job)) => job,
        Ok(None) => return Err(Error::NotFound { id: job_id }.into()),
        Err(err) => {
            tracing::debug!(
                error = %err,
                error_source = logging::error_source(&err),
                job_id = ?job_id,
                "failed to get job"
            );
            return Err(Error::GetJob(err).into());
        }
    };

    // Get all tables associated with this job's writer
    let job_tables = ctx
        .data_store
        .get_tables_by_writer(job_id)
        .await
        .map_err(|err| {
            tracing::error!(
                job_id = ?job_id,
                error = %err,
                error_source = logging::error_source(&err),
                "failed to get tables by writer"
            );
            Error::GetTables(err)
        })?;

    // A job is associated with exactly one dataset, so we can get the hash reference
    // and dataset definition once from the first table
    let Some(first_table) = job_tables.first() else {
        // No tables written by this job yet
        return Ok(Json(JobProgressResponse {
            job_id: *job_id,
            job_status: job.status.to_string(),
            tables: HashMap::new(),
        }));
    };

    let hash_ref = HashReference::new(
        first_table.dataset_namespace.clone().into(),
        first_table.dataset_name.clone().into(),
        first_table.manifest_hash.clone().into(),
    );

    let dataset = ctx
        .dataset_store
        .get_dataset(&hash_ref)
        .await
        .map_err(|err| {
            tracing::error!(
                dataset_reference = %hash_ref,
                error = %err,
                error_source = logging::error_source(&err),
                "failed to get dataset definition"
            );
            Error::GetDataset(err)
        })?;

    let mut tables = HashMap::new();

    // For each table, compute progress
    for job_table in job_tables {
        // Convert table name to datasets_common type
        let table_name: TableName = job_table.table_name.clone().into();

        // Find the table configuration
        let table_config = dataset
            .tables()
            .iter()
            .find(|t| t.name().as_str() == table_name.as_str());

        let table_config = match table_config {
            Some(config) => config,
            None => {
                // Table not in dataset definition, skip (shouldn't happen normally)
                tracing::error!(
                    table = %table_name,
                    dataset_reference = %hash_ref,
                    "table not found in dataset definition, skipping"
                );
                continue;
            }
        };

        // Get the active physical table revision
        let physical_table = ctx
            .data_store
            .get_table_active_revision(&hash_ref, &table_name)
            .await
            .map_err(|err| {
                tracing::error!(
                    table = %table_name,
                    error = %err,
                    error_source = logging::error_source(&err),
                    "failed to get active physical table"
                );
                Error::GetActiveRevision(err)
            })?
            .map(|revision| {
                PhysicalTable::from_revision(
                    ctx.data_store.clone(),
                    hash_ref.clone(),
                    dataset.start_block(),
                    table_config.clone(),
                    revision,
                )
            });

        let (current_block, start_block, files_count, total_size_bytes) =
            if let Some(pt) = physical_table {
                let snapshot = pt.snapshot(false).await.map_err(|err| {
                    tracing::error!(
                        table = %table_name,
                        error = %err,
                        error_source = logging::error_source(&err),
                        "failed to snapshot physical table"
                    );
                    Error::SnapshotTable(err)
                })?;

                let synced_range = snapshot.synced_range().map_err(|err| {
                    tracing::error!(
                        table = %table_name,
                        error = %err,
                        error_source = logging::error_source(&err),
                        "table has multi-network segments; cannot compute job progress"
                    );
                    Error::MultiNetworkSegments(err)
                })?;
                let canonical_segments = snapshot.canonical_segments();

                let files_count = canonical_segments.len() as i64;
                let total_size_bytes = canonical_segments
                    .iter()
                    .map(|s| s.object().size as i64)
                    .sum();

                let (start, end) =
                    match synced_range {
                        Some(range) => {
                            let start: i64 = range.start().try_into().map_err(|_| {
                                Error::BlockNumberOverflow {
                                    block_num: range.start(),
                                }
                            })?;
                            let end: i64 =
                                range
                                    .end()
                                    .try_into()
                                    .map_err(|_| Error::BlockNumberOverflow {
                                        block_num: range.end(),
                                    })?;
                            (Some(start), Some(end))
                        }
                        None => (None, None),
                    };

                (end, start, files_count, total_size_bytes)
            } else {
                (None, None, 0, 0)
            };

        tables.insert(
            job_table.table_name.to_string(),
            TableProgress {
                current_block,
                start_block,
                files_count,
                total_size_bytes,
            },
        );
    }

    Ok(Json(JobProgressResponse {
        job_id: *job_id,
        job_status: job.status.to_string(),
        tables,
    }))
}

/// API response containing progress information for a job
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct JobProgressResponse {
    /// Job ID
    pub job_id: i64,
    /// Current job status
    pub job_status: String,
    /// Progress for each table written by this job, keyed by table name
    pub tables: HashMap<String, TableProgress>,
}

/// Progress information for a single table
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct TableProgress {
    /// Highest block number that has been synced (null if no data yet)
    pub current_block: Option<i64>,
    /// Lowest block number that has been synced (null if no data yet)
    pub start_block: Option<i64>,
    /// Number of Parquet files written for this table
    pub files_count: i64,
    /// Total size of all Parquet files in bytes
    pub total_size_bytes: i64,
}

/// Errors that can occur during job progress retrieval
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The job ID in the URL path is invalid
    #[error("invalid job ID: {err}")]
    InvalidId {
        /// The rejection details from Axum's path extractor
        err: PathRejection,
    },

    /// The requested job was not found
    #[error("job '{id}' not found")]
    NotFound {
        /// The job ID that was not found
        id: JobId,
    },

    /// Failed to retrieve job from scheduler
    #[error("failed to get job")]
    GetJob(#[source] scheduler::GetJobError),

    /// Failed to get tables by writer
    #[error("failed to get tables by writer")]
    GetTables(#[source] amp_data_store::GetTablesByWriterError),

    /// Failed to get dataset definition
    #[error("failed to get dataset definition")]
    GetDataset(#[source] common::dataset_store::GetDatasetError),

    /// Failed to get active table revision
    #[error("failed to get active table revision")]
    GetActiveRevision(#[source] amp_data_store::GetTableActiveRevisionError),

    /// Failed to snapshot physical table
    #[error("failed to snapshot physical table")]
    SnapshotTable(#[source] common::physical_table::SnapshotError),

    /// Table contains multi-network segments; synced range cannot be computed.
    ///
    /// This indicates a catalog inconsistency and is distinguishable from the
    /// "no data synced yet" case, which returns HTTP 200 with null block fields.
    #[error("table has multi-network segments: {0}")]
    MultiNetworkSegments(#[source] common::physical_table::MultiNetworkSegmentsError),

    /// A block number from the synced range cannot be represented as i64.
    ///
    /// The block number is included in the error so that callers can distinguish
    /// this integrity failure from a valid block-0 response.
    #[error("block number {block_num} overflows i64 range")]
    BlockNumberOverflow {
        /// The block number that could not be converted.
        block_num: u64,
    },
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidId { .. } => "INVALID_JOB_ID",
            Error::NotFound { .. } => "JOB_NOT_FOUND",
            Error::GetJob(_) => "GET_JOB_ERROR",
            Error::GetTables(_) => "GET_TABLES_ERROR",
            Error::GetDataset(_) => "GET_DATASET_ERROR",
            Error::GetActiveRevision(_) => "GET_ACTIVE_REVISION_ERROR",
            Error::SnapshotTable(_) => "SNAPSHOT_TABLE_ERROR",
            Error::MultiNetworkSegments(_) => "MULTI_NETWORK_SEGMENTS_ERROR",
            Error::BlockNumberOverflow { .. } => "BLOCK_NUMBER_OVERFLOW",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidId { .. } => StatusCode::BAD_REQUEST,
            Error::NotFound { .. } => StatusCode::NOT_FOUND,
            Error::GetJob(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::GetTables(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::GetDataset(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::GetActiveRevision(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::SnapshotTable(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::MultiNetworkSegments(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::BlockNumberOverflow { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use common::physical_table::MultiNetworkSegmentsError;

    use super::*;

    /// `MULTI_NETWORK_SEGMENTS_ERROR` is the public API contract. Guard against silent changes.
    #[test]
    fn multi_network_segments_error_maps_to_correct_error_code() {
        // Given: a multi-network segments error from synced_range()
        let err = Error::MultiNetworkSegments(MultiNetworkSegmentsError);

        // Then: the error code matches the documented API constant
        assert_eq!(err.error_code(), "MULTI_NETWORK_SEGMENTS_ERROR");
    }

    /// HTTP 500 is distinct from HTTP 200 (no-data) — this guards the status code contract.
    #[test]
    fn multi_network_segments_error_returns_internal_server_error_status() {
        // Given: a multi-network segments error
        let err = Error::MultiNetworkSegments(MultiNetworkSegmentsError);

        // Then: returns 500, distinguishable from the 200 "no data synced yet" case
        assert_eq!(err.status_code(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    /// `BLOCK_NUMBER_OVERFLOW` is the public API contract. Guard against silent changes.
    #[test]
    fn block_number_overflow_maps_to_correct_error_code() {
        //* Given
        let err = Error::BlockNumberOverflow {
            block_num: u64::MAX,
        };

        //* When
        let code = err.error_code();

        //* Then
        assert_eq!(code, "BLOCK_NUMBER_OVERFLOW");
    }

    /// Overflow is a data-integrity failure, not valid "no data" — guards the status code.
    #[test]
    fn block_number_overflow_returns_internal_server_error_status() {
        //* Given
        let err = Error::BlockNumberOverflow {
            block_num: u64::MAX,
        };

        //* When
        let status = err.status_code();

        //* Then
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    }

    /// Overflow carries the offending block number, distinguishing it from block 0.
    #[test]
    fn block_number_overflow_display_includes_block_num() {
        //* Given
        let block_num = u64::MAX;
        let err = Error::BlockNumberOverflow { block_num };

        //* When
        let msg = err.to_string();

        //* Then
        assert!(
            msg.contains(&block_num.to_string()),
            "display should include the overflowing block number; got: {msg}"
        );
    }
}

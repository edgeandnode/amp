use std::{collections::HashSet, time::Duration};

use amp_data_store::physical_table::PhyTableRevisionPath;
use amp_parquet::meta::ParquetMeta;
use amp_worker_core::jobs::{job_id::JobId, status::JobStatus};
use axum::{
    Json,
    extract::{
        Path, Query, State,
        rejection::{PathRejection, QueryRejection},
    },
};
use common::physical_table::{
    FileMetadata,
    segments::{Segment, canonical_chain},
};
use datasets_common::block_num::BlockNum;
use metadata_db::{files::FileId, physical_table_revision::LocationId};
use monitoring::logging;

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
    scheduler,
};

/// Handler for the `DELETE /revisions/{id}/prune` endpoint
///
/// Prunes non-canonical segments from a physical table revision by scheduling
/// them for garbage collection via the GC manifest.
///
/// ## Path Parameters
/// - `id`: The unique identifier of the revision (must be a valid LocationId)
///
/// ## Query Parameters
/// - `before_block` (optional): Only prune segments ending before this block number
/// - `gc_delay_secs` (required): Seconds before files become eligible for GC deletion
///
/// ## Response
/// - **200 OK**: Non-canonical segments scheduled for GC successfully
/// - **400 Bad Request**: Invalid path or query parameters
/// - **404 Not Found**: Revision not found
/// - **409 Conflict**: Writer job is not terminal
/// - **500 Internal Server Error**: Database or object store error
///
/// ## Error Codes
/// - `INVALID_PATH_PARAMETERS`: Invalid path parameters
/// - `INVALID_QUERY_PARAMETERS`: Invalid query parameters
/// - `REVISION_NOT_FOUND`: No revision exists with the specified location ID
/// - `WRITER_JOB_NOT_TERMINAL`: The revision's writer job is still running and must reach a terminal state before pruning
/// - `GET_REVISION_BY_LOCATION_ID_ERROR`: Failed to retrieve revision from database
/// - `GET_WRITER_JOB_ERROR`: Failed to retrieve writer job from database
/// - `GET_FILES_ERROR`: Failed to retrieve file metadata
/// - `PARSE_METADATA_ERROR`: Failed to parse parquet metadata
/// - `CANONICAL_CHAIN_PANICKED`: Canonical chain computation task panicked or was aborted
/// - `PRUNE_ERROR`: Failed to prune non-canonical segments
///
/// ## Behavior
/// This handler:
/// - Validates and extracts the location ID from the URL path
/// - Retrieves the revision to verify it exists
/// - Checks that any writer job is in a terminal state (completed, stopped, or failed)
/// - Retrieves all file metadata for the revision
/// - Computes the canonical chain of segments
/// - Identifies non-canonical segments (segments not in the canonical chain)
/// - Optionally filters by block number (only segments ending before the specified block)
/// - Schedules non-canonical files for garbage collection via the GC manifest
/// - Preserves the revision record and all canonical segments
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        delete,
        path = "/revisions/{id}/prune",
        tag = "revisions",
        operation_id = "prune_table_revision",
        params(
            ("id" = i64, Path, description = "Revision ID"),
            ("before_block" = Option<u64>, Query, description = "Only prune segments ending before this block number"),
            ("gc_delay_secs" = u64, Query, description = "Seconds before files become eligible for GC deletion")
        ),
        responses(
            (status = 200, description = "Non-canonical segments scheduled for GC successfully", body = PruneResponse),
            (status = 400, description = "Invalid path or query parameters", body = ErrorResponse),
            (status = 404, description = "Revision not found", body = ErrorResponse),
            (status = 409, description = "Writer job is not terminal", body = ErrorResponse),
            (status = 500, description = "Internal server error", body = ErrorResponse)
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    path: Result<Path<LocationId>, PathRejection>,
    query: Result<Query<QueryParams>, QueryRejection>,
) -> Result<Json<PruneResponse>, ErrorResponse> {
    let location_id = match path {
        Ok(Path(location_id)) => location_id,
        Err(err) => {
            tracing::debug!(error = %err, error_source = logging::error_source(&err), "invalid path parameters");
            return Err(Error::InvalidPath(err).into());
        }
    };

    let query = match query {
        Ok(Query(query)) => query,
        Err(err) => {
            tracing::debug!(error = %err, error_source = logging::error_source(&err), "invalid query parameters");
            return Err(Error::InvalidQueryParams(err).into());
        }
    };

    tracing::debug!(location_id = %location_id, before_block = ?query.before_block, "pruning non-canonical segments");

    let revision = ctx
        .data_store
        .get_revision_by_location_id(location_id)
        .await
        .map_err(Error::GetRevisionByLocationId)?
        .ok_or_else(|| {
            tracing::debug!(location_id = %location_id, "revision not found");
            Error::NotFound { location_id }
        })?;

    if let Some(writer_job_id) = revision.writer {
        let worker_job_id: JobId = writer_job_id.into();
        let job = ctx
            .scheduler
            .get_job(worker_job_id)
            .await
            .map_err(Error::GetWriterJob)?;

        if let Some(job) = job
            && !job.status.is_terminal()
        {
            tracing::debug!(
                location_id = %location_id,
                writer_job_id = %writer_job_id,
                job_status = %job.status,
                "cannot prune revision with non-terminal writer job"
            );
            return Err(Error::WriterJobNotTerminal {
                location_id,
                job_id: writer_job_id,
                status: job.status,
            }
            .into());
        }
    }

    // Build PhyTableRevision for accessing files
    let phy_table_revision_path: PhyTableRevisionPath = revision.path.into();
    let phy_table_url = amp_data_store::physical_table::PhyTableUrl::new(
        ctx.data_store.url(),
        &phy_table_revision_path,
    );
    let phy_table_revision = amp_data_store::PhyTableRevision {
        location_id: revision.id,
        path: phy_table_revision_path,
        url: phy_table_url,
    };

    // Get all files for the revision
    let files = ctx
        .data_store
        .get_revision_files(&phy_table_revision)
        .await
        .map_err(Error::GetFiles)?;

    // Convert files to Segment objects
    let all_segments: Vec<Segment> = files
        .iter()
        .map(|file_meta| {
            // Parse FileMetadata from PhyTableRevisionFileMetadata
            let file: FileMetadata = file_meta.clone().try_into().map_err(Error::ParseMetadata)?;

            // Extract components for Segment
            let FileMetadata {
                file_id: id,
                object_meta: object,
                parquet_meta:
                    ParquetMeta {
                        ranges, watermark, ..
                    },
                ..
            } = file;

            Ok(Segment::new(id, object, ranges, watermark))
        })
        .collect::<Result<Vec<_>, Error>>()?;

    // Compute canonical chain
    // The blocking computation is offloaded to a dedicated thread pool via `spawn_blocking`
    // to prevent blocking the async runtime. The `canonical_chain` function performs
    // CPU-intensive operations (sorting, chain building) that can take milliseconds.
    let canonical_segments = tokio::task::spawn_blocking({
        let all_segments = all_segments.clone();
        move || {
            canonical_chain(all_segments)
                .map(|chain| chain.0)
                .unwrap_or_default()
        }
    })
    .await
    .map_err(Error::CanonicalChainPanicked)?;

    // Build set of canonical file IDs for fast lookup
    let canonical_ids: HashSet<FileId> = canonical_segments.iter().map(|seg| seg.id()).collect();

    // Identify non-canonical segment IDs, optionally filtering by before_block
    let non_canonical_ids: Vec<FileId> = all_segments
        .iter()
        .filter(|seg| !canonical_ids.contains(&seg.id()))
        .filter(|seg| {
            // Apply before_block filter if specified
            if let Some(cutoff) = query.before_block {
                // For single-network segments, check if end block < cutoff
                // We assume single-network for now (consistent with the codebase)
                if seg.ranges().len() == 1 {
                    return seg.ranges()[0].end() < cutoff;
                }
                // If multi-network, skip it
                false
            } else {
                // No filter, include all non-canonical
                true
            }
        })
        .map(|seg| seg.id())
        .collect();

    let files_scheduled = non_canonical_ids.len() as u64;

    if files_scheduled == 0 {
        tracing::debug!("no non-canonical segments to prune");
        return Ok(Json(PruneResponse {
            files_scheduled: 0,
            gc_delay_secs: query.gc_delay_secs,
        }));
    }

    // Determine GC delay duration
    let gc_delay_secs = query.gc_delay_secs;
    let gc_duration = Duration::from_secs(gc_delay_secs);

    tracing::debug!(
        total_segments = all_segments.len(),
        canonical_segments = canonical_segments.len(),
        non_canonical_segments = files_scheduled,
        before_block = ?query.before_block,
        gc_delay_secs = gc_delay_secs,
        "scheduling non-canonical segments for GC"
    );

    // Schedule files for garbage collection
    metadata_db::gc::upsert(
        &ctx.metadata_db,
        location_id,
        &non_canonical_ids,
        gc_duration,
    )
    .await
    .map_err(Error::ScheduleGc)?;

    tracing::info!(
        location_id = %location_id,
        files_scheduled = files_scheduled,
        before_block = ?query.before_block,
        gc_delay_secs = gc_delay_secs,
        "non-canonical segments scheduled for GC"
    );

    Ok(Json(PruneResponse {
        files_scheduled,
        gc_delay_secs,
    }))
}

/// Query parameters for the prune endpoint
#[derive(Debug, serde::Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::IntoParams))]
pub struct QueryParams {
    /// Only prune segments ending before this block number
    pub before_block: Option<BlockNum>,
    /// Seconds before files become eligible for GC deletion
    pub gc_delay_secs: u64,
}

/// Response for prune operation
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct PruneResponse {
    /// Number of non-canonical files scheduled for garbage collection
    pub files_scheduled: u64,
    /// Seconds before files become eligible for GC deletion
    pub gc_delay_secs: u64,
}

/// Errors that can occur when pruning non-canonical segments
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid path parameters
    ///
    /// This occurs when:
    /// - The location ID in the URL path is not a valid integer
    /// - Path parameter parsing fails
    #[error("Invalid path parameters: {0}")]
    InvalidPath(#[source] PathRejection),

    /// Invalid query parameters
    ///
    /// This occurs when:
    /// - The query string cannot be parsed into the expected format
    #[error("Invalid query parameters: {0}")]
    InvalidQueryParams(#[source] QueryRejection),

    /// Revision not found
    ///
    /// This occurs when:
    /// - The specified location ID doesn't exist in the metadata database
    #[error("Revision with location ID '{location_id}' not found")]
    NotFound { location_id: LocationId },

    /// Writer job is not in a terminal state
    ///
    /// This occurs when:
    /// - The revision has a writer job that is still running, scheduled, or stopping
    /// - The writer job must reach a terminal state before the revision can be pruned
    #[error(
        "Revision with location ID '{location_id}' has writer job {job_id} in non-terminal state '{status}'"
    )]
    WriterJobNotTerminal {
        location_id: LocationId,
        job_id: metadata_db::jobs::JobId,
        status: JobStatus,
    },

    /// Failed to get revision by location ID
    ///
    /// This occurs when:
    /// - The database query to get revision by location ID fails
    /// - Database connection issues
    #[error("Failed to get revision by location ID")]
    GetRevisionByLocationId(#[source] amp_data_store::GetRevisionByLocationIdError),

    /// Failed to get writer job
    ///
    /// This occurs when:
    /// - The database query to get the writer job fails
    /// - Database connection issues
    #[error("Failed to get writer job")]
    GetWriterJob(#[source] scheduler::GetJobError),

    /// Failed to get files
    ///
    /// This occurs when:
    /// - Streaming file metadata from database fails
    #[error("Failed to get files")]
    GetFiles(#[source] amp_data_store::StreamFileMetadataError),

    /// Failed to parse metadata
    ///
    /// This occurs when:
    /// - Parquet metadata JSON cannot be parsed
    #[error("Failed to parse metadata")]
    ParseMetadata(#[source] serde_json::Error),

    /// Canonical chain computation task panicked or was aborted
    ///
    /// This occurs when:
    /// - The `spawn_blocking` task for `canonical_chain` panicked or was aborted
    /// - This surfaces as a recoverable error rather than propagating a process-level panic
    #[error("canonical_chain computation task panicked or was aborted")]
    CanonicalChainPanicked(#[source] tokio::task::JoinError),

    /// Failed to schedule files for garbage collection
    ///
    /// This occurs when:
    /// - Upserting GC manifest entries fails
    /// - Database connection issues
    #[error("Failed to schedule files for garbage collection")]
    ScheduleGc(#[source] metadata_db::Error),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidPath(_) => "INVALID_PATH_PARAMETERS",
            Error::InvalidQueryParams(_) => "INVALID_QUERY_PARAMETERS",
            Error::NotFound { .. } => "REVISION_NOT_FOUND",
            Error::WriterJobNotTerminal { .. } => "WRITER_JOB_NOT_TERMINAL",
            Error::GetRevisionByLocationId(_) => "GET_REVISION_BY_LOCATION_ID_ERROR",
            Error::GetWriterJob(_) => "GET_WRITER_JOB_ERROR",
            Error::GetFiles(_) => "GET_FILES_ERROR",
            Error::ParseMetadata(_) => "PARSE_METADATA_ERROR",
            Error::CanonicalChainPanicked(_) => "CANONICAL_CHAIN_PANICKED",
            Error::ScheduleGc(_) => "PRUNE_ERROR",
        }
    }

    fn status_code(&self) -> axum::http::StatusCode {
        match self {
            Error::InvalidPath(_) => axum::http::StatusCode::BAD_REQUEST,
            Error::InvalidQueryParams(_) => axum::http::StatusCode::BAD_REQUEST,
            Error::NotFound { .. } => axum::http::StatusCode::NOT_FOUND,
            Error::WriterJobNotTerminal { .. } => axum::http::StatusCode::CONFLICT,
            Error::GetRevisionByLocationId(_) => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            Error::GetWriterJob(_) => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            Error::GetFiles(_) => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            Error::ParseMetadata(_) => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            Error::CanonicalChainPanicked(_) => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            Error::ScheduleGc(_) => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

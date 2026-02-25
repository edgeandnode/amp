use amp_data_store::{
    DeleteTableRevisionError, PhyTableRevision, TruncateError,
    physical_table::{PhyTableRevisionPath, PhyTableUrl},
};
use amp_worker_core::jobs::job_id::JobId;
use axum::{
    Json,
    extract::{
        Path, Query, State,
        rejection::{PathRejection, QueryRejection},
    },
};
use futures::TryStreamExt as _;
use metadata_db::{
    files::{FileId, FileNameOwned},
    physical_table_revision::LocationId,
};
use monitoring::logging;
use worker::job::JobStatus;

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
    scheduler,
};

/// Handler for the `DELETE /revisions/{id}/truncate` endpoint
///
/// Truncates a physical table revision by deleting all associated files from
/// object storage and their corresponding metadata, then deletes the revision.
///
/// ## Path Parameters
/// - `id`: The unique identifier of the revision (must be a valid LocationId)
///
/// ## Response
/// - **200 OK**: Revision truncated and deleted successfully
/// - **400 Bad Request**: Invalid path parameters
/// - **404 Not Found**: Revision not found
/// - **409 Conflict**: Revision is currently active, writer job is not terminal,
///   or file metadata rows remain after truncation
/// - **500 Internal Server Error**: Database or object store error
///
/// ## Error Codes
/// - `INVALID_PATH_PARAMETERS`: Invalid path parameters
/// - `INVALID_QUERY_PARAMETERS`: Invalid query parameters
/// - `REVISION_NOT_FOUND`: No revision exists with the specified location ID
/// - `REVISION_IS_ACTIVE`: The revision is currently active and must be deactivated before truncation
/// - `WRITER_JOB_NOT_TERMINAL`: The revision's writer job is still running and must reach a terminal state before truncation
/// - `GET_REVISION_BY_LOCATION_ID_ERROR`: Failed to retrieve revision from database
/// - `GET_WRITER_JOB_ERROR`: Failed to retrieve writer job from database
/// - `TRUNCATE_ERROR`: Failed to truncate revision files
/// - `VERIFY_FILE_METADATA_ERROR`: Failed to verify file metadata cleanup
/// - `FILE_METADATA_REMAINING`: File metadata rows remain after truncation
/// - `DELETE_REVISION_ERROR`: Failed to delete revision row
///
/// ## Behavior
/// This handler:
/// - Validates and extracts the location ID from the URL path
/// - Retrieves the revision to verify it exists
/// - Checks that the revision is inactive (active revisions cannot be truncated)
/// - Checks that any writer job is in a terminal state (completed, stopped, or failed)
/// - Deletes all files from object storage and their metadata rows with bounded concurrency
/// - Verifies no file metadata rows remain for the revision
/// - Deletes the revision record from the metadata database
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        delete,
        path = "/revisions/{id}/truncate",
        tag = "revisions",
        operation_id = "truncate_table_revision",
        params(
            ("id" = i64, Path, description = "Revision ID"),
            ("concurrency" = Option<usize>, Query, description = "Max concurrent file deletions (default: 10)")
        ),
        responses(
            (status = 200, description = "Revision truncated and deleted successfully", body = TruncateResponse),
            (status = 400, description = "Invalid path parameters", body = ErrorResponse),
            (status = 404, description = "Revision not found", body = ErrorResponse),
            (status = 409, description = "Revision is active, writer job is not terminal, or file metadata remaining", body = ErrorResponse),
            (status = 500, description = "Internal server error", body = ErrorResponse)
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    path: Result<Path<LocationId>, PathRejection>,
    query: Result<Query<QueryParams>, QueryRejection>,
) -> Result<Json<TruncateResponse>, ErrorResponse> {
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

    tracing::debug!(location_id = %location_id, "truncating revision");

    let revision = ctx
        .data_store
        .get_revision_by_location_id(location_id)
        .await
        .map_err(Error::GetRevisionByLocationId)?
        .ok_or_else(|| {
            tracing::debug!(location_id = %location_id, "revision not found");
            Error::NotFound { location_id }
        })?;

    if revision.active {
        tracing::debug!(location_id = %location_id, "cannot truncate active revision");
        return Err(Error::RevisionIsActive { location_id }.into());
    }

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
                "cannot truncate revision with non-terminal writer job"
            );
            return Err(Error::WriterJobNotTerminal {
                location_id,
                job_id: writer_job_id,
                status: job.status,
            }
            .into());
        }
    }

    // Convert PhysicalTableRevision to PhyTableRevision for data store operations
    let path: PhyTableRevisionPath = revision.path.into();
    let url = PhyTableUrl::new(ctx.data_store.url(), &path);
    let phy_table_revision = PhyTableRevision {
        location_id: revision.id,
        path,
        url,
    };

    // Step 1: Truncate files (object store + file_metadata rows)
    let files_deleted = ctx
        .data_store
        .truncate_revision(&phy_table_revision, query.concurrency.max(1))
        .await
        .map_err(Error::Truncate)?;

    // Step 2: Verify no file_metadata rows remain for this location_id
    let remaining_files: Vec<(FileId, FileNameOwned)> =
        metadata_db::files::stream_by_location_id_with_details(&ctx.metadata_db, revision.id)
            .map_ok(|f| (f.id, f.file_name))
            .try_collect()
            .await
            .map_err(Error::VerifyFileMetadata)?;

    if !remaining_files.is_empty() {
        tracing::warn!(
            location_id = %location_id,
            remaining_count = remaining_files.len(),
            "file_metadata rows remain after truncation"
        );
        return Err(Error::FileMetadataRemaining {
            location_id,
            remaining: remaining_files
                .into_iter()
                .map(|(file_id, file_name)| RemainingFile { file_id, file_name })
                .collect(),
        }
        .into());
    }

    // Step 3: Delete the revision row
    let deleted = ctx
        .data_store
        .delete_table_revision(revision.id)
        .await
        .map_err(Error::DeleteRevision)?;

    if !deleted {
        // The revision was activated by a concurrent request between the
        // pre-check and the DELETE query.
        tracing::error!(location_id = %location_id, "revision truncated but deletion rejected, revision was concurrently activated");
        return Err(Error::RevisionIsActive { location_id }.into());
    }

    tracing::info!(location_id = %location_id, files_deleted = files_deleted, "revision truncated and deleted");

    Ok(Json(TruncateResponse { files_deleted }))
}

const DEFAULT_CONCURRENCY: usize = 10;

/// Query parameters for the truncate endpoint
#[derive(Debug, serde::Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::IntoParams))]
pub struct QueryParams {
    /// Max concurrent file deletions (default: 10)
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
}

fn default_concurrency() -> usize {
    DEFAULT_CONCURRENCY
}

/// Response for truncate operation
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct TruncateResponse {
    /// Number of files deleted from object storage
    pub files_deleted: u64,
}

/// A file metadata entry that remains after truncation
#[derive(Debug)]
pub struct RemainingFile {
    /// Unique identifier for the file
    pub file_id: FileId,
    /// Name of the file
    pub file_name: FileNameOwned,
}

/// Errors that can occur when truncating a revision
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

    /// Revision is currently active
    ///
    /// This occurs when:
    /// - The revision is the active revision for its table
    /// - The revision must be deactivated before it can be truncated
    #[error("Revision with location ID '{location_id}' is active and cannot be truncated")]
    RevisionIsActive { location_id: LocationId },

    /// Writer job is not in a terminal state
    ///
    /// This occurs when:
    /// - The revision has a writer job that is still running, scheduled, or stopping
    /// - The writer job must reach a terminal state before the revision can be truncated
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

    /// Failed to truncate revision files
    ///
    /// This occurs when:
    /// - File deletion from object store fails
    /// - Streaming file metadata from database fails
    #[error("Failed to truncate revision")]
    Truncate(#[source] TruncateError),

    /// Failed to verify file metadata cleanup
    ///
    /// This occurs when:
    /// - The database query to check remaining file_metadata rows fails
    /// - Database connection issues
    #[error("Failed to verify file metadata cleanup")]
    VerifyFileMetadata(#[source] metadata_db::Error),

    /// File metadata rows remain after truncation
    ///
    /// This occurs when:
    /// - Some file_metadata rows could not be deleted during truncation
    /// - The operation should be retried to clean up remaining rows
    #[error("{}", format_remaining_files(location_id, remaining))]
    FileMetadataRemaining {
        location_id: LocationId,
        remaining: Vec<RemainingFile>,
    },

    /// Failed to delete revision row from metadata database
    ///
    /// This occurs when:
    /// - The database operation to delete the revision row fails
    /// - Database connection issues
    #[error("Failed to delete revision")]
    DeleteRevision(#[source] DeleteTableRevisionError),
}

fn format_remaining_files(location_id: &LocationId, remaining: &[RemainingFile]) -> String {
    let file_details: Vec<String> = remaining
        .iter()
        .map(|f| format!("{} ({})", f.file_id, f.file_name))
        .collect();
    format!(
        "Revision with location ID '{}' has {} remaining file_metadata rows: [{}]",
        location_id,
        remaining.len(),
        file_details.join(", ")
    )
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidPath(_) => "INVALID_PATH_PARAMETERS",
            Error::InvalidQueryParams(_) => "INVALID_QUERY_PARAMETERS",
            Error::NotFound { .. } => "REVISION_NOT_FOUND",
            Error::RevisionIsActive { .. } => "REVISION_IS_ACTIVE",
            Error::WriterJobNotTerminal { .. } => "WRITER_JOB_NOT_TERMINAL",
            Error::GetRevisionByLocationId(_) => "GET_REVISION_BY_LOCATION_ID_ERROR",
            Error::GetWriterJob(_) => "GET_WRITER_JOB_ERROR",
            Error::Truncate(_) => "TRUNCATE_ERROR",
            Error::VerifyFileMetadata(_) => "VERIFY_FILE_METADATA_ERROR",
            Error::FileMetadataRemaining { .. } => "FILE_METADATA_REMAINING",
            Error::DeleteRevision(_) => "DELETE_REVISION_ERROR",
        }
    }

    fn status_code(&self) -> axum::http::StatusCode {
        match self {
            Error::InvalidPath(_) => axum::http::StatusCode::BAD_REQUEST,
            Error::InvalidQueryParams(_) => axum::http::StatusCode::BAD_REQUEST,
            Error::NotFound { .. } => axum::http::StatusCode::NOT_FOUND,
            Error::RevisionIsActive { .. } => axum::http::StatusCode::CONFLICT,
            Error::WriterJobNotTerminal { .. } => axum::http::StatusCode::CONFLICT,
            Error::GetRevisionByLocationId(_) => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            Error::GetWriterJob(_) => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            Error::Truncate(_) => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            Error::VerifyFileMetadata(_) => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            Error::FileMetadataRemaining { .. } => axum::http::StatusCode::CONFLICT,
            Error::DeleteRevision(_) => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

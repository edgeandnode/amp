use amp_data_store::{DeleteTableRevisionError, GetRevisionByLocationIdError};
use axum::{
    extract::{Path, State, rejection::PathRejection},
    http::StatusCode,
};
use metadata_db::physical_table_revision::LocationId;
use monitoring::logging;

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
    scheduler,
};

/// Handler for the `DELETE /revisions/{id}` endpoint
///
/// Deletes a physical table revision by its location ID.
///
/// ## Path Parameters
/// - `id`: The unique identifier of the revision (must be a valid LocationId)
///
/// ## Response
/// - **204 No Content**: Revision deleted successfully
/// - **400 Bad Request**: Invalid path parameters
/// - **404 Not Found**: Revision not found
/// - **409 Conflict**: Revision is currently active or writer job is not in a terminal state
/// - **500 Internal Server Error**: Database error
///
/// ## Error Codes
/// - `INVALID_PATH_PARAMETERS`: Invalid path parameters
/// - `REVISION_NOT_FOUND`: No revision exists with the specified location ID
/// - `REVISION_IS_ACTIVE`: The revision is currently active and must be deactivated before deletion
/// - `WRITER_JOB_NOT_TERMINAL`: The revision's writer job is still running and must reach a terminal state before deletion
/// - `GET_REVISION_BY_LOCATION_ID_ERROR`: Failed to retrieve revision from database
/// - `GET_WRITER_JOB_ERROR`: Failed to retrieve writer job from database
/// - `DELETE_TABLE_REVISION_ERROR`: Failed to delete revision from database
///
/// ## Behavior
/// This handler:
/// - Validates and extracts the location ID from the URL path
/// - Retrieves the revision to verify it exists
/// - Checks that the revision is inactive (active revisions cannot be deleted)
/// - Checks that any writer job is in a terminal state (completed, stopped, or failed)
/// - Deletes the revision and all associated file metadata (via CASCADE)
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        delete,
        path = "/revisions/{id}",
        tag = "revisions",
        operation_id = "delete_table_revision",
        params(
            ("id" = i64, Path, description = "Revision ID")
        ),
        responses(
            (status = 204, description = "Revision deleted successfully"),
            (status = 400, description = "Invalid path parameters", body = ErrorResponse),
            (status = 404, description = "Revision not found", body = ErrorResponse),
            (status = 409, description = "Revision is active or writer job is not terminal", body = ErrorResponse),
            (status = 500, description = "Internal server error", body = ErrorResponse)
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    path: Result<Path<LocationId>, PathRejection>,
) -> Result<StatusCode, ErrorResponse> {
    let location_id = match path {
        Ok(Path(location_id)) => location_id,
        Err(err) => {
            tracing::debug!(
                error = %err,
                error_source = logging::error_source(&err),
                "path parameter validation failed"
            );
            return Err(Error::InvalidPath(err).into());
        }
    };

    tracing::debug!(location_id = %location_id, "revision deletion requested");

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
        tracing::debug!(location_id = %location_id, "revision deletion rejected, revision is active");
        return Err(Error::RevisionIsActive { location_id }.into());
    }

    if let Some(writer_job_id) = revision.writer {
        let worker_job_id: worker::job::JobId = writer_job_id.into();
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
                "revision deletion rejected, writer job not terminal"
            );
            return Err(Error::WriterJobNotTerminal { location_id }.into());
        }
    }

    let deleted = ctx
        .data_store
        .delete_table_revision(location_id)
        .await
        .map_err(Error::DeleteRevision)?;

    if !deleted {
        // The revision was active and wasn't deleted — this means it was
        // activated by a concurrent request between checking and the DELETE query.
        tracing::error!(location_id = %location_id, "revision deletion rejected");
        return Err(Error::RevisionIsActive { location_id }.into());
    }

    tracing::info!(location_id = %location_id, "revision deleted");

    Ok(StatusCode::NO_CONTENT)
}

/// Errors that can occur when deleting a revision
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid path parameters
    ///
    /// This occurs when:
    /// - The location ID in the URL path is not a valid integer
    /// - Path parameter parsing fails
    #[error("Invalid path parameters: {0}")]
    InvalidPath(#[source] PathRejection),

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
    /// - The revision must be deactivated before it can be deleted
    #[error("Revision with location ID '{location_id}' is active and cannot be deleted")]
    RevisionIsActive { location_id: LocationId },

    /// Writer job is not in a terminal state
    ///
    /// This occurs when:
    /// - The revision has a writer job that is still running, scheduled, or stopping
    /// - The writer job must reach a terminal state before the revision can be deleted
    #[error(
        "Revision with location ID '{location_id}' has an active writer job and cannot be deleted"
    )]
    WriterJobNotTerminal { location_id: LocationId },

    /// Failed to get revision by location ID
    ///
    /// This occurs when:
    /// - The database query to get revision by location ID fails
    /// - Database connection issues
    #[error("Failed to get revision by location ID")]
    GetRevisionByLocationId(#[source] GetRevisionByLocationIdError),

    /// Failed to get writer job
    ///
    /// This occurs when:
    /// - The database query to get the writer job fails
    /// - Database connection issues
    #[error("Failed to get writer job")]
    GetWriterJob(#[source] scheduler::GetJobError),

    /// Failed to delete revision
    ///
    /// This occurs when:
    /// - The database delete operation fails
    /// - Database connection issues
    #[error("Failed to delete table revision")]
    DeleteRevision(#[source] DeleteTableRevisionError),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidPath(_) => "INVALID_PATH_PARAMETERS",
            Error::NotFound { .. } => "REVISION_NOT_FOUND",
            Error::RevisionIsActive { .. } => "REVISION_IS_ACTIVE",
            Error::WriterJobNotTerminal { .. } => "WRITER_JOB_NOT_TERMINAL",
            Error::GetRevisionByLocationId(_) => "GET_REVISION_BY_LOCATION_ID_ERROR",
            Error::GetWriterJob(_) => "GET_WRITER_JOB_ERROR",
            Error::DeleteRevision(_) => "DELETE_TABLE_REVISION_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidPath(_) => StatusCode::BAD_REQUEST,
            Error::NotFound { .. } => StatusCode::NOT_FOUND,
            Error::RevisionIsActive { .. } => StatusCode::CONFLICT,
            Error::WriterJobNotTerminal { .. } => StatusCode::CONFLICT,
            Error::GetRevisionByLocationId(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::GetWriterJob(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::DeleteRevision(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

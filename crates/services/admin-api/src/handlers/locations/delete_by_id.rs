//! Locations delete handler

use axum::{
    extract::{
        Path, Query, State,
        rejection::{PathRejection, QueryRejection},
    },
    http::StatusCode,
};
use common::{BoxError, store::object_store};
use futures::{TryStreamExt, stream};
use metadata_db::{JobId, LocationId};
use object_store::path::Path as ObjectPath;

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
};

/// Query parameters for the locations delete endpoint
#[derive(Debug, serde::Deserialize)]
pub struct QueryParams {
    /// Force deletion even if location is active
    #[serde(default)]
    force: bool,
}

/// Handler for the `DELETE /locations/{id}` endpoint
///
/// Deletes a specific location by its ID from the metadata database.
///
/// ## Path Parameters
/// - `id`: The unique identifier of the location to delete (must be a positive integer)
///
/// ## Query Parameters
/// - `force`: (optional, default: false) Force deletion even if location is active
///
/// ## Response
/// - **204 No Content**: Location successfully deleted
/// - **400 Bad Request**: Invalid location ID format or invalid query parameters
/// - **404 Not Found**: Location with the given ID does not exist
/// - **409 Conflict**: Location is active (without force=true) or has an ongoing job
/// - **500 Internal Server Error**: Database connection or query error
///
/// ## Error Codes
/// - `INVALID_LOCATION_ID`: The provided ID is not a valid positive integer
/// - `INVALID_QUERY_PARAMETERS`: The query parameters cannot be parsed
/// - `LOCATION_NOT_FOUND`: No location exists with the given ID
/// - `ACTIVE_LOCATION_CONFLICT`: Location is active and cannot be deleted without force=true
/// - `ONGOING_JOB_CONFLICT`: Location has an ongoing job and cannot be deleted
/// - `METADATA_DB_ERROR`: Internal database error occurred
///
/// ## Safety Checks
/// - Active locations require `force=true` to be deleted
/// - Locations with ongoing jobs cannot be deleted (even with force=true)
/// - Users must stop active jobs before deleting associated locations
///
/// This handler:
/// - Validates and extracts the location ID from the URL path
/// - Validates optional query parameters (force flag)
/// - Performs safety checks for active locations and ongoing jobs
/// - Deletes associated files from object store
/// - Deletes the location from the metadata database
/// - Returns appropriate HTTP status codes and error messages
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        delete,
        path = "/locations/{id}",
        tag = "locations",
        operation_id = "locations_delete",
        params(
            ("id" = i64, Path, description = "Location ID"),
            ("force" = Option<bool>, Query, description = "Force deletion even if location is active")
        ),
        responses(
            (status = 204, description = "Location successfully deleted"),
            (status = 400, description = "Invalid location ID or query parameters", body = crate::handlers::error::ErrorResponse),
            (status = 404, description = "Location not found", body = crate::handlers::error::ErrorResponse),
            (status = 409, description = "Location is active or has ongoing job", body = crate::handlers::error::ErrorResponse),
            (status = 500, description = "Internal server error", body = crate::handlers::error::ErrorResponse)
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    path: Result<Path<LocationId>, PathRejection>,
    query: Result<Query<QueryParams>, QueryRejection>,
) -> Result<StatusCode, ErrorResponse> {
    let location_id = match path {
        Ok(Path(path)) => path,
        Err(err) => {
            tracing::debug!(error=?err, "Invalid location ID in path");
            return Err(Error::InvalidId { err }.into());
        }
    };

    let query = match query {
        Ok(Query(query)) => query,
        Err(err) => {
            tracing::debug!(error=?err, "Invalid query parameters");
            return Err(Error::InvalidQueryParams { err }.into());
        }
    };

    let location =
        metadata_db::physical_table::get_by_id_with_details(&ctx.metadata_db, location_id)
            .await
            .map_err(Error::MetadataDbError)?
            .ok_or_else(|| {
                tracing::debug!(location_id = %location_id, "Location not found");
                Error::NotFound { id: location_id }
            })?;

    if location.active() && !query.force {
        tracing::debug!(location_id = %location_id, "Cannot delete active location without force flag");
        return Err(Error::ActiveLocation { id: location_id }.into());
    }

    if let Some(writer_job) = &location.writer
        && !writer_job.status.is_terminal()
    {
        tracing::debug!(
            location_id = %location_id,
            job_id = %writer_job.id,
            job_status = ?writer_job.status,
            "Cannot delete location with ongoing job"
        );

        return Err(Error::OngoingJob {
            location_id,
            job_id: writer_job.id,
        }
        .into());
    }

    let location_url = location.url().clone().try_into().map_err(|err| {
        tracing::error!(
            location_id = %location.id(),
            location_url = %location.url(),
            error = ?err,
            "Invalid location URL from database, cannot create object store"
        );
        Error::ObjectStoreError(Box::new(err))
    })?;

    let (store, _) = object_store(&location_url).map_err(|err| {
        tracing::error!(
            location_id = %location.id(),
            location_url = %location_url,
            error = ?err,
            "Failed to create object store instance for location deletion"
        );
        Error::ObjectStoreError(Box::new(err))
    })?;

    let loc_id = location.id();
    let file_paths: Vec<ObjectPath> = ctx
        .metadata_db
        .stream_files_by_location_id_with_details(loc_id)
        .try_filter_map(|file_metadata| async move {
            match ObjectPath::parse(&file_metadata.file_name) {
                Ok(path1) => Ok(Some(path1)),
                Err(path_error) => {
                    tracing::warn!(
                        location_id = %loc_id,
                        file_metadata_id = %file_metadata.id,
                        file_name = %file_metadata.file_name,
                        error = ?path_error,
                        "Invalid file path filtered out from deletion"
                    );
                    Ok(None)
                }
            }
        })
        .try_collect()
        .await
        .map_err(|err| {
            tracing::error!(
                location_id = %loc_id,
                error = ?err,
                "Failed to list file metadata from database for location deletion"
            );
            Error::MetadataDbError(err)
        })?;

    let num_files = file_paths.len();
    tracing::info!(
        location_id = %loc_id,
        num_files = num_files,
        "Starting bulk deletion of files"
    );

    let _ = metadata_db::physical_table::delete_by_id(&ctx.metadata_db, location_id)
        .await
        .map_err(Error::MetadataDbError)?;

    tracing::info!(
        location_id = %loc_id,
        "Location metadata deleted from database, proceeding to delete files"
    );

    let paths_stream = Box::pin(stream::iter(file_paths.into_iter().map(Ok)));
    let deleted_paths = store
        .delete_stream(paths_stream)
        .try_collect::<Vec<ObjectPath>>()
        .await
        .map_err(|err| {
            tracing::error!(
                location_id = %loc_id,
                error = ?err,
                "Bulk deletion operation failed"
            );
            Error::ObjectStoreError(Box::new(err))
        })?;

    if deleted_paths.len() == num_files {
        tracing::info!(
            location_id = %loc_id,
            deleted_files = deleted_paths.len(),
            "Successfully deleted all files"
        );
    } else {
        tracing::warn!(
            location_id = %loc_id,
            expected = num_files,
            actual = deleted_paths.len(),
            "Incomplete files deletion: some files may have already been deleted or may not exist"
        );
    }

    tracing::info!(
        location_id = %location_id,
        "Successfully deleted location files and metadata"
    );

    Ok(StatusCode::NO_CONTENT)
}

/// Errors that can occur during location deletion
///
/// This enum represents all possible error conditions that can occur
/// when handling a `DELETE /locations/{id}` request, from path parsing
/// to database operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The location ID in the URL path is invalid
    ///
    /// This occurs when:
    /// - The ID is not a valid number (e.g., "abc", "1.5")
    /// - The ID is zero or negative (e.g., "0", "-5")
    /// - The ID is too large to fit in an i64
    #[error("invalid location ID: {err}")]
    InvalidId {
        /// The rejection details from Axum's path extractor
        err: PathRejection,
    },

    /// The requested location was not found in the database
    ///
    /// This occurs when the location ID is valid but no location
    /// record exists with that ID in the metadata database.
    #[error("location '{id}' not found")]
    NotFound {
        /// The location ID that was not found
        id: LocationId,
    },

    /// Invalid query parameters
    ///
    /// This occurs when query parameters cannot be parsed.
    #[error("invalid query parameters: {err}")]
    InvalidQueryParams {
        /// The rejection details from Axum's query extractor
        err: QueryRejection,
    },

    /// Location is active and cannot be deleted without force
    ///
    /// This occurs when trying to delete an active location without the force parameter.
    #[error("location '{id}' is currently active and cannot be deleted without force=true")]
    ActiveLocation {
        /// The location ID that is active
        id: LocationId,
    },

    /// Location has an ongoing job and cannot be deleted
    ///
    /// This occurs when trying to delete a location that has a job in a non-terminal state.
    #[error(
        "location '{location_id}' has ongoing job '{job_id}' and cannot be deleted without force=true"
    )]
    OngoingJob {
        /// The location ID with the ongoing job
        location_id: LocationId,
        /// The job ID that is ongoing
        job_id: JobId,
    },

    /// An error occurred while querying the metadata database
    ///
    /// This covers database connection issues, query failures,
    /// and other internal database errors.
    #[error("metadata db error: {0}")]
    MetadataDbError(metadata_db::Error),

    /// Object store error occurred during file deletion
    ///
    /// This covers errors that occur when deleting files from the object store.
    #[error("object store error: {0}")]
    ObjectStoreError(BoxError),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidId { .. } => "INVALID_LOCATION_ID",
            Error::NotFound { .. } => "LOCATION_NOT_FOUND",
            Error::InvalidQueryParams { .. } => "INVALID_QUERY_PARAMETERS",
            Error::ActiveLocation { .. } => "ACTIVE_LOCATION_CONFLICT",
            Error::OngoingJob { .. } => "ONGOING_JOB_CONFLICT",
            Error::MetadataDbError(_) => "METADATA_DB_ERROR",
            Error::ObjectStoreError(_) => "OBJECT_STORE_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidId { .. } => StatusCode::BAD_REQUEST,
            Error::NotFound { .. } => StatusCode::NOT_FOUND,
            Error::InvalidQueryParams { .. } => StatusCode::BAD_REQUEST,
            Error::ActiveLocation { .. } => StatusCode::CONFLICT,
            Error::OngoingJob { .. } => StatusCode::CONFLICT,
            Error::MetadataDbError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::ObjectStoreError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

//! Files delete by ID handler

use axum::{
    extract::{
        Path, Query, State,
        rejection::{PathRejection, QueryRejection},
    },
    http::StatusCode,
};
use common::{BoxError, store::object_store};
use http_common::{BoxRequestError, RequestError};
use metadata_db::{FileId, LocationId};
use object_store::path::Path as ObjectPath;

use crate::ctx::Ctx;

/// Query parameters for the files delete endpoint
#[derive(Debug, serde::Deserialize)]
pub struct QueryParams {
    /// Force deletion even if file's location is active
    #[serde(default)]
    force: bool,
}

/// Handler for the `DELETE /files/{file_id}` endpoint
///
/// Deletes a specific file both from the object store and metadata database by its ID.
///
/// ## Path Parameters
/// - `file_id`: The unique identifier of the file to delete (must be a positive integer)
///
/// ## Query Parameters
/// - `force`: (optional, default: false) Force deletion even if file's location is active
///
/// ## Response
/// - **204 No Content**: File was successfully deleted from both store and database
/// - **400 Bad Request**: Invalid file ID format, invalid query parameters
/// - **404 Not Found**: File with the given ID does not exist in the database
/// - **409 Conflict**: File is in active location (without force=true)
/// - **500 Internal Server Error**: Database connection, object store, or query error
///
/// ## Error Codes
/// - `INVALID_FILE_ID`: The provided ID is not a valid positive integer
/// - `INVALID_QUERY_PARAMETERS`: The query parameters cannot be parsed
/// - `FILE_NOT_FOUND`: No file exists with the given ID in the database
/// - `ACTIVE_LOCATION_FILE_CONFLICT`: File is in active location and cannot be deleted without force=true
/// - `METADATA_DB_ERROR`: Internal database error occurred
/// - `OBJECT_STORE_ERROR`: Error occurred while deleting from object store
///
/// ## Safety Checks
/// - Files in active locations require `force=true` to be deleted
/// - This prevents accidental deletion of files from currently active locations
///
/// ## Deletion Process
/// This handler performs a two-stage deletion:
/// 1. **Safety Check**: Verifies if file's location is active and force flag is provided
/// 2. **Object Store Deletion**: Attempts to delete the file from the object store
///    - If file not found in store, logs a warning but continues
///    - If other store errors occur, returns an error
/// 3. **Database Deletion**: Removes the file metadata from the database
///    - Always attempted even if store deletion fails
///    - Returns 404 if file metadata doesn't exist
///
/// This handler:
/// - Validates and extracts the file ID from the URL path
/// - Validates optional query parameters (force flag)
/// - Retrieves file metadata to determine location and construct object store path
/// - Performs safety checks for active locations
/// - Deletes the file from the object store (continues if file not found)
/// - Deletes the file metadata record from the database
/// - Returns appropriate HTTP status codes and error messages
#[tracing::instrument(skip_all, err)]
pub async fn handler(
    State(ctx): State<Ctx>,
    path: Result<Path<FileId>, PathRejection>,
    query: Result<Query<QueryParams>, QueryRejection>,
) -> Result<StatusCode, BoxRequestError> {
    let file_id = match path {
        Ok(Path(path)) => path,
        Err(err) => {
            tracing::debug!(error=?err, "invalid file ID in path");
            return Err(Error::InvalidId { err }.into());
        }
    };

    let query = match query {
        Ok(Query(query)) => query,
        Err(err) => {
            tracing::debug!(error=?err, "invalid query parameters");
            return Err(Error::InvalidQueryParams { err }.into());
        }
    };

    // First, get the file metadata to determine location for object store deletion
    let file_metadata = match ctx.metadata_db.get_file_by_id_with_details(file_id).await {
        Ok(Some(metadata)) => metadata,
        Ok(None) => {
            tracing::debug!(file_id=%file_id, "file not found in database");
            return Err(Error::NotFound { id: file_id }.into());
        }
        Err(err) => {
            tracing::debug!(error=?err, file_id=%file_id, "failed to query file metadata");
            return Err(Error::MetadataDbError(err).into());
        }
    };

    // Check if file's location is active and force flag is required
    if file_metadata.location_active && !query.force {
        tracing::debug!(
            file_id=%file_id,
            location_id=%file_metadata.location_id,
            "Cannot delete file from active location without force flag"
        );
        return Err(Error::ActiveLocationFile {
            file_id,
            location_id: file_metadata.location_id,
        }
        .into());
    }

    // Convert URL to ObjectStoreUrl
    let location_url = file_metadata.url.clone().try_into().map_err(|err| {
        tracing::error!(
            file_id=%file_id,
            file_location_url=%file_metadata.url,
            error=?err,
            "invalid location URL from database, cannot create object store"
        );
        Error::ObjectStoreError(Box::new(err))
    })?;

    let (store, _) = object_store(&location_url).map_err(|err| {
        tracing::error!(
            file_id=%file_id,
            file_location_url=%location_url,
            error=?err,
            "failed to create object store instance for file deletion"
        );
        Error::ObjectStoreError(Box::new(err))
    })?;

    // Parse the file path for object store deletion
    let file_path = ObjectPath::parse(&file_metadata.file_name).map_err(|err| {
        tracing::error!(
            file_id=%file_id,
            file_location_url=%location_url,
            error=?err,
            "invalid file path, cannot delete from object store"
        );
        Error::ObjectStoreError(Box::new(err))
    })?;

    // Attempt to delete from object store
    match store.delete(&file_path).await {
        Ok(()) => {
            tracing::info!(
                file_id=?file_id,
                file_location_url=%location_url,
                "successfully deleted file from object store"
            );
        }
        Err(object_store::Error::NotFound { .. }) => {
            tracing::warn!(
                file_id=%file_id,
                file_path=%file_path,
                "file not found in object store, proceeding with db entry deletion"
            );
        }
        Err(err) => {
            tracing::error!(
                file_id=%file_id,
                file_location_url=%location_url,
                error=?err,
                "failed to delete file from object store"
            );
            return Err(Error::ObjectStoreError(Box::new(err)).into());
        }
    }

    // Delete the file metadata record from database
    match ctx.metadata_db.delete_file(file_id).await {
        Ok(true) => {
            tracing::info!(
                file_id=%file_id,
                file_location_url=%location_url,
                "file metadata deleted successfully from database"
            );
        }
        Ok(false) => {
            tracing::warn!(
                file_id=%file_id,
                file_location_url=%location_url,
                "file metadata not found during database deletion"
            );
        }
        Err(err) => {
            tracing::debug!(
                file_id=%file_id,
                file_location_url=%location_url,
                error=?err,
                "failed to delete file metadata from database"
            );
            return Err(Error::MetadataDbError(err).into());
        }
    };

    Ok(StatusCode::NO_CONTENT)
}

/// Errors that can occur during file metadata deletion
///
/// This enum represents all possible error conditions that can occur
/// when handling a `DELETE /files/{file_id}` request, from path parsing
/// to database operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The file ID in the URL path is invalid
    ///
    /// This occurs when:
    /// - The ID is not a valid number (e.g., "abc", "1.5")
    /// - The ID is zero or negative (e.g., "0", "-5")
    /// - The ID is too large to fit in an i64
    #[error("invalid file ID: {err}")]
    InvalidId {
        /// The rejection details from Axum's path extractor
        err: PathRejection,
    },

    /// Invalid query parameters
    ///
    /// This occurs when query parameters cannot be parsed.
    #[error("invalid query parameters: {err}")]
    InvalidQueryParams {
        /// The rejection details from Axum's query extractor
        err: QueryRejection,
    },

    /// The requested file was not found in the database
    ///
    /// This occurs when the file ID is valid but no file
    /// record exists with that ID in the metadata database.
    #[error("file '{id}' not found")]
    NotFound {
        /// The file ID that was not found
        id: FileId,
    },

    /// File's location is active and cannot be deleted without force
    ///
    /// This occurs when trying to delete a file from an active location without the force parameter.
    #[error(
        "file '{file_id}' is in active location '{location_id}' and cannot be deleted without force=true"
    )]
    ActiveLocationFile {
        /// The file ID that cannot be deleted
        file_id: FileId,
        /// The location ID that is active
        location_id: LocationId,
    },

    /// An error occurred while querying the metadata database
    ///
    /// This covers database connection issues, query failures,
    /// and other internal database errors.
    #[error("metadata db error: {0}")]
    MetadataDbError(#[from] metadata_db::Error),

    /// Object store error occurred during file deletion
    ///
    /// This covers errors that occur when deleting files from the object store,
    /// including network issues, authentication failures, and storage errors.
    #[error("object store error: {0}")]
    ObjectStoreError(BoxError),
}

impl RequestError for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidId { .. } => "INVALID_FILE_ID",
            Error::InvalidQueryParams { .. } => "INVALID_QUERY_PARAMETERS",
            Error::NotFound { .. } => "FILE_NOT_FOUND",
            Error::ActiveLocationFile { .. } => "ACTIVE_LOCATION_FILE_CONFLICT",
            Error::MetadataDbError(_) => "METADATA_DB_ERROR",
            Error::ObjectStoreError(_) => "OBJECT_STORE_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidId { .. } => StatusCode::BAD_REQUEST,
            Error::InvalidQueryParams { .. } => StatusCode::BAD_REQUEST,
            Error::NotFound { .. } => StatusCode::NOT_FOUND,
            Error::ActiveLocationFile { .. } => StatusCode::CONFLICT,
            Error::MetadataDbError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::ObjectStoreError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

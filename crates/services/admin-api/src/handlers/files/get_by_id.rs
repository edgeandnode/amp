//! Files get by ID handler

use axum::{
    Json,
    extract::{Path, State, rejection::PathRejection},
    http::StatusCode,
};
use metadata_db::{FileId, LocationId};
use monitoring::logging;

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
};

/// Handler for the `GET /files/{file_id}` endpoint
///
/// Retrieves and returns a specific file by its ID from the metadata database.
///
/// ## Path Parameters
/// - `file_id`: The unique identifier of the file to retrieve (must be a positive integer)
///
/// ## Response
/// - **200 OK**: Returns the file information as JSON
/// - **400 Bad Request**: Invalid file ID format (not a number, zero, or negative)
/// - **404 Not Found**: File with the given ID does not exist
/// - **500 Internal Server Error**: Database connection or query error
///
/// ## Error Codes
/// - `INVALID_FILE_ID`: The provided ID is not a valid positive integer
/// - `FILE_NOT_FOUND`: No file exists with the given ID
/// - `METADATA_DB_ERROR`: Internal database error occurred
///
/// This handler:
/// - Validates and extracts the file ID from the URL path
/// - Queries the metadata database for the file with location information
/// - Returns appropriate HTTP status codes and error messages
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        get,
        path = "/files/{file_id}",
        tag = "files",
        operation_id = "files_get",
        params(
            ("file_id" = i64, Path, description = "File ID")
        ),
        responses(
            (status = 200, description = "Successfully retrieved file information", body = FileInfo),
            (status = 400, description = "Invalid file ID", body = crate::handlers::error::ErrorResponse),
            (status = 404, description = "File not found", body = crate::handlers::error::ErrorResponse),
            (status = 500, description = "Internal server error", body = crate::handlers::error::ErrorResponse)
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    path: Result<Path<FileId>, PathRejection>,
) -> Result<Json<FileInfo>, ErrorResponse> {
    let file_id = match path {
        Ok(Path(path)) => path,
        Err(err) => {
            tracing::debug!(error = %err, error_source = logging::error_source(&err), "invalid file ID in path");
            return Err(Error::InvalidId { err }.into());
        }
    };

    // Get file metadata with details from the database
    match ctx.metadata_db.get_file_by_id_with_details(file_id).await {
        Ok(Some(file_metadata)) => {
            tracing::debug!(file_id=?file_id, "successfully retrieved file metadata");
            Ok(Json(file_metadata.into()))
        }
        Ok(None) => {
            tracing::debug!(file_id=?file_id, "file not found in database");
            Err(Error::NotFound { id: file_id }.into())
        }
        Err(err) => {
            tracing::debug!(error = %err, error_source = logging::error_source(&err), file_id=?file_id, "failed to query file metadata");
            Err(Error::MetadataDbError(err).into())
        }
    }
}

/// File information returned by the API
///
/// This struct represents file metadata from the database in a format
/// suitable for API responses. It contains all the essential information
/// about Parquet files and their associated metadata within locations.
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct FileInfo {
    /// Unique identifier for this file (64-bit integer)
    #[cfg_attr(feature = "utoipa", schema(value_type = i64))]
    pub id: FileId,
    /// Location ID this file belongs to (64-bit integer)
    #[cfg_attr(feature = "utoipa", schema(value_type = i64))]
    pub location_id: LocationId,
    /// Full path of the file (e.g., "s3://bucket/path/blocks_0000000000_0000099999.parquet")
    pub file_path: String,
    /// Size of the file object in bytes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_size: Option<i64>,
    /// ETag of the file object for caching and version identification
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_e_tag: Option<String>,
    /// Version identifier of the file object in the storage system
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_version: Option<String>,
    /// Parquet file metadata as JSON containing schema and statistics
    #[cfg_attr(feature = "utoipa", schema(value_type = serde_json::Value))]
    pub metadata: serde_json::Value,
}

impl From<metadata_db::FileMetadataWithDetails> for FileInfo {
    /// Converts a database `FileMetadataWithDetails` record into an API-friendly `FileInfo`
    ///
    /// This conversion preserves all fields from the database record.
    fn from(value: metadata_db::FileMetadataWithDetails) -> Self {
        Self {
            id: value.id,
            location_id: value.location_id,
            file_path: value.file_path,
            object_size: value.object_size,
            object_e_tag: value.object_e_tag,
            object_version: value.object_version,
            metadata: value.metadata,
        }
    }
}

/// Errors that can occur during file retrieval
///
/// This enum represents all possible error conditions that can occur
/// when handling a `GET /files/{file_id}` request, from path parsing
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

    /// The requested file was not found in the database
    ///
    /// This occurs when the file ID is valid but no file
    /// record exists with that ID in the metadata database.
    #[error("file '{id}' not found")]
    NotFound {
        /// The file ID that was not found
        id: FileId,
    },

    /// An error occurred while querying the metadata database
    ///
    /// This covers database connection issues, query failures,
    /// and other internal database errors.
    #[error("metadata db error: {0}")]
    MetadataDbError(#[source] metadata_db::Error),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidId { .. } => "INVALID_FILE_ID",
            Error::NotFound { .. } => "FILE_NOT_FOUND",
            Error::MetadataDbError(_) => "METADATA_DB_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidId { .. } => StatusCode::BAD_REQUEST,
            Error::NotFound { .. } => StatusCode::NOT_FOUND,
            Error::MetadataDbError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

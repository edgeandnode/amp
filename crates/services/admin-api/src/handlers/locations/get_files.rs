//! Location files listing handler

use axum::{
    Json,
    extract::{
        Path, Query, State,
        rejection::{PathRejection, QueryRejection},
    },
    http::StatusCode,
};
use metadata_db::{FileId, LocationId};

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
};

/// Default number of files returned per page
const DEFAULT_PAGE_LIMIT: usize = 50;

/// Maximum number of files allowed per page
const MAX_PAGE_LIMIT: usize = 1000;

/// Query parameters for the files listing endpoint
#[derive(Debug, serde::Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct QueryParams {
    /// Maximum number of files to return (default: 50, max: 1000)
    #[serde(default = "default_limit")]
    limit: usize,
    /// ID of the last file from the previous page for pagination
    #[cfg_attr(feature = "utoipa", schema(value_type = Option<i64>))]
    last_file_id: Option<FileId>,
}

fn default_limit() -> usize {
    DEFAULT_PAGE_LIMIT
}

/// Minimal file information for location file listings
///
/// This struct represents essential file metadata for list endpoints,
/// containing only the most relevant information needed for file browsing
/// within a location context.
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct FileListInfo {
    /// Unique identifier for this file (64-bit integer)
    #[cfg_attr(feature = "utoipa", schema(value_type = i64))]
    pub id: FileId,
    /// Name of the file (e.g., "blocks_0000000000_0000099999.parquet")
    pub file_name: String,
    /// Size of the file object in bytes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_size: Option<i64>,
}

impl From<metadata_db::FileMetadata> for FileListInfo {
    /// Converts a database `FileMetadata` record into a minimal `FileListInfo` for listings
    ///
    /// This conversion extracts only essential fields needed for file listing:
    /// - File ID for identification
    /// - File name for display  
    /// - Object size for reference
    fn from(value: metadata_db::FileMetadata) -> Self {
        Self {
            id: value.id,
            file_name: value.file_name,
            object_size: value.object_size,
        }
    }
}

/// Collection response for location file listings
///
/// This response structure provides paginated file data with
/// cursor-based pagination support for efficient traversal.
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct LocationFilesResponse {
    /// List of files in this page with minimal information
    pub files: Vec<FileListInfo>,
    /// Cursor for the next page of results - use as last_file_id in next request (None if no more results)
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "utoipa", schema(value_type = Option<i64>))]
    pub next_cursor: Option<FileId>,
}

/// Handler for the `GET /locations/{location_id}/files` endpoint
///
/// Retrieves and returns a paginated list of files for a specific location from the metadata database.
///
/// ## Path Parameters
/// - `location_id`: The unique identifier of the location (must be a positive integer)
///
/// ## Query Parameters
/// - `limit`: Maximum number of files to return (default: 50, max: 1000)
/// - `last_file_id`: ID of the last file from previous page for cursor-based pagination
///
/// ## Response
/// - **200 OK**: Returns paginated file data with next cursor
/// - **400 Bad Request**: Invalid location ID format or invalid limit parameter
/// - **500 Internal Server Error**: Database connection or query error
///
/// ## Error Codes
/// - `INVALID_LOCATION_ID`: Invalid location ID format
/// - `INVALID_QUERY_PARAMETERS`: Invalid query parameters (limit out of range)
/// - `LIMIT_TOO_LARGE`: Limit exceeds maximum allowed value
/// - `LIMIT_INVALID`: Limit is zero or negative
/// - `METADATA_DB_ERROR`: Internal database error occurred
///
/// This handler:
/// - Validates and extracts the location ID from the URL path
/// - Accepts query parameters for pagination (limit, last_file_id)
/// - Validates the limit parameter (max 1000)
/// - Calls the metadata DB to list files with pagination for the specified location
/// - Returns a structured response with minimal file info and next cursor
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        get,
        path = "/locations/{location_id}/files",
        tag = "locations",
        operation_id = "locations_list_files",
        params(
            ("location_id" = i64, Path, description = "Location ID")
        ),
        responses(
            (status = 200, description = "Successfully retrieved location files", body = LocationFilesResponse),
            (status = 400, description = "Invalid location ID or query parameters"),
            (status = 500, description = "Internal server error")
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    path: Result<Path<LocationId>, PathRejection>,
    query: Result<Query<QueryParams>, QueryRejection>,
) -> Result<Json<LocationFilesResponse>, ErrorResponse> {
    let location_id = match path {
        Ok(Path(path)) => path,
        Err(err) => {
            tracing::debug!(error=?err, "invalid location ID in path");
            return Err(Error::InvalidLocationId { err }.into());
        }
    };

    let query = match query {
        Ok(Query(params)) => params,
        Err(err) => {
            tracing::debug!(error=?err, "invalid query parameters");
            return Err(Error::InvalidQueryParams { err }.into());
        }
    };

    // Validate limit
    let limit = if query.limit > MAX_PAGE_LIMIT {
        return Err(Error::LimitTooLarge {
            limit: query.limit,
            max: MAX_PAGE_LIMIT,
        }
        .into());
    } else if query.limit == 0 {
        return Err(Error::LimitInvalid.into());
    } else {
        query.limit
    };

    // Fetch files from metadata DB with pagination
    let files = ctx
        .metadata_db
        .list_files_by_location_id(location_id, limit as i64, query.last_file_id)
        .await
        .map_err(|err| {
            tracing::debug!(error=?err, location_id=?location_id, "failed to list files for location");
            Error::MetadataDbError(err)
        })?;

    // Determine next cursor (ID of the last file in this page)
    let next_cursor = files.last().map(|file| file.id);
    let files = files.into_iter().take(limit).map(Into::into).collect();

    Ok(Json(LocationFilesResponse { files, next_cursor }))
}

/// Errors that can occur during location files listing
///
/// This enum represents all possible error conditions that can occur
/// when handling a `GET /locations/{location_id}/files` request with pagination parameters.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The location ID in the URL path is invalid
    ///
    /// This occurs when:
    /// - The ID is not a valid number (e.g., "abc", "1.5")
    /// - The ID is zero or negative (e.g., "0", "-5")
    /// - The ID is too large to fit in an i64
    #[error("invalid location ID: {err}")]
    InvalidLocationId {
        /// The rejection details from Axum's path extractor
        err: PathRejection,
    },

    /// The query parameters are invalid or malformed
    ///
    /// This occurs when query parameters cannot be parsed, such as:
    /// - Invalid integer format for limit or last_file_id
    /// - Malformed query string syntax
    #[error("invalid query parameters: {err}")]
    InvalidQueryParams {
        /// The rejection details from Axum's query extractor
        err: QueryRejection,
    },

    /// The requested limit exceeds the maximum allowed value
    ///
    /// This occurs when the limit parameter is greater than the maximum
    /// allowed page size.
    #[error("limit {limit} exceeds maximum allowed limit of {max}")]
    LimitTooLarge {
        /// The requested limit value
        limit: usize,
        /// The maximum allowed limit
        max: usize,
    },

    /// The requested limit is invalid (zero or negative)
    ///
    /// This occurs when the limit parameter is 0, which would result
    /// in no items being returned.
    #[error("limit must be greater than 0")]
    LimitInvalid,

    /// An error occurred while querying the metadata database
    ///
    /// This covers database connection issues, query failures,
    /// and other internal database errors.
    #[error("metadata db error: {0}")]
    MetadataDbError(#[from] metadata_db::Error),
}

impl IntoErrorResponse for Error {
    /// Returns the error code string for API responses
    ///
    /// These error codes are returned in the API response body to help
    /// clients programmatically identify and handle different error types.
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidLocationId { .. } => "INVALID_LOCATION_ID",
            Error::InvalidQueryParams { .. } => "INVALID_QUERY_PARAMETERS",
            Error::LimitTooLarge { .. } => "LIMIT_TOO_LARGE",
            Error::LimitInvalid => "LIMIT_INVALID",
            Error::MetadataDbError(_) => "METADATA_DB_ERROR",
        }
    }

    /// Returns the appropriate HTTP status code for each error type
    ///
    /// Maps internal error types to standard HTTP status codes:
    /// - Invalid Request → 400 Bad Request
    /// - Database Error → 500 Internal Server Error
    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidLocationId { .. } => StatusCode::BAD_REQUEST,
            Error::InvalidQueryParams { .. } => StatusCode::BAD_REQUEST,
            Error::LimitTooLarge { .. } => StatusCode::BAD_REQUEST,
            Error::LimitInvalid => StatusCode::BAD_REQUEST,
            Error::MetadataDbError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

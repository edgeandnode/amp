//! Locations get all handler

use axum::{
    Json,
    extract::{Query, State, rejection::QueryRejection},
    http::StatusCode,
};
use metadata_db::LocationId;

use super::location_info::LocationInfo;
use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
};

/// Default number of locations returned per page
const DEFAULT_PAGE_LIMIT: usize = 50;

/// Maximum number of locations allowed per page
const MAX_PAGE_LIMIT: usize = 1000;

/// Query parameters for the locations listing endpoint
#[derive(Debug, serde::Deserialize)]
pub struct QueryParams {
    /// Maximum number of locations to return (default: 50, max: 1000)
    #[serde(default = "default_limit")]
    limit: usize,

    /// ID of the last location from the previous page for pagination
    last_location_id: Option<LocationId>,
}

fn default_limit() -> usize {
    DEFAULT_PAGE_LIMIT
}

/// Handler for the `GET /locations` endpoint
///
/// Retrieves and returns a paginated list of locations from the metadata database.
///
/// ## Query Parameters
/// - `limit`: Maximum number of locations to return (default: 50, max: 1000)
/// - `last_location_id`: ID of the last location from previous page for cursor-based pagination
///
/// ## Response
/// - **200 OK**: Returns paginated location data with next cursor
/// - **400 Bad Request**: Invalid limit parameter (0, negative, or > 1000)
/// - **500 Internal Server Error**: Database connection or query error
///
/// ## Error Codes
/// - `INVALID_REQUEST`: Invalid query parameters (limit out of range)
/// - `METADATA_DB_ERROR`: Internal database error occurred
///
/// This handler:
/// - Accepts query parameters for pagination (limit, last_location_id)
/// - Validates the limit parameter (max 1000)
/// - Calls the metadata DB to list locations with pagination
/// - Returns a structured response with locations and next cursor
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        get,
        path = "/locations",
        tag = "locations",
        operation_id = "locations_list",
        params(
            ("limit" = Option<usize>, Query, description = "Maximum number of locations to return (default: 50, max: 1000)"),
            ("last_location_id" = Option<String>, Query, description = "ID of the last location from the previous page for pagination")
        ),
        responses(
            (status = 200, description = "Successfully retrieved locations", body = LocationsResponse),
            (status = 400, description = "Invalid query parameters", body = crate::handlers::error::ErrorResponse),
            (status = 500, description = "Internal server error", body = crate::handlers::error::ErrorResponse)
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    query: Result<Query<QueryParams>, QueryRejection>,
) -> Result<Json<LocationsResponse>, ErrorResponse> {
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

    // Fetch locations from metadata DB
    let locations =
        metadata_db::physical_table::list(&ctx.metadata_db, limit as i64, query.last_location_id)
            .await
            .map_err(|err| {
                tracing::debug!(error=?err, "failed to list locations");
                Error::MetadataDbError(err)
            })?;

    // Determine next cursor (ID of the last location in this page)
    let next_cursor = locations.last().map(|location| *location.id);
    let locations = locations.into_iter().take(limit).map(Into::into).collect();

    Ok(Json(LocationsResponse {
        locations,
        next_cursor,
    }))
}

/// API response containing location information
///
/// This response structure provides paginated location data with
/// cursor-based pagination support for efficient traversal.
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct LocationsResponse {
    /// List of locations in this page
    pub locations: Vec<LocationInfo>,
    /// Cursor for the next page of results (None if no more results)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<i64>,
}

/// Errors that can occur during location listing
///
/// This enum represents all possible error conditions that can occur
/// when handling a `GET /locations` request with pagination parameters.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The query parameters are invalid or malformed
    ///
    /// This occurs when query parameters cannot be parsed, such as:
    /// - Invalid integer format for limit or last_location_id
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
            Error::InvalidQueryParams { .. } => StatusCode::BAD_REQUEST,
            Error::LimitTooLarge { .. } => StatusCode::BAD_REQUEST,
            Error::LimitInvalid => StatusCode::BAD_REQUEST,
            Error::MetadataDbError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

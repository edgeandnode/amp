//! Locations get by ID handler

use axum::{
    Json,
    extract::{Path, State, rejection::PathRejection},
    http::StatusCode,
};
use metadata_db::LocationId;

use super::location_info::LocationInfoWithDetails;
use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
};

/// Handler for the `GET /locations/{id}` endpoint
///
/// Retrieves and returns a specific location by its ID from the metadata database.
///
/// ## Path Parameters
/// - `id`: The unique identifier of the location to retrieve (must be a positive integer)
///
/// ## Response
/// - **200 OK**: Returns the location information as JSON
/// - **400 Bad Request**: Invalid location ID format (not a number, zero, or negative)
/// - **404 Not Found**: Location with the given ID does not exist
/// - **500 Internal Server Error**: Database connection or query error
///
/// ## Error Codes
/// - `INVALID_LOCATION_ID`: The provided ID is not a valid positive integer
/// - `LOCATION_NOT_FOUND`: No location exists with the given ID
/// - `METADATA_DB_ERROR`: Internal database error occurred
///
/// This handler:
/// - Validates and extracts the location ID from the URL path
/// - Queries the metadata database for the location
/// - Returns appropriate HTTP status codes and error messages
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        get,
        path = "/locations/{id}",
        tag = "locations",
        operation_id = "locations_get",
        params(
            ("id" = i64, Path, description = "Location ID")
        ),
        responses(
            (status = 200, description = "Successfully retrieved location information", body = LocationInfoWithDetails),
            (status = 400, description = "Invalid location ID", body = crate::handlers::error::ErrorResponse),
            (status = 404, description = "Location not found", body = crate::handlers::error::ErrorResponse),
            (status = 500, description = "Internal server error", body = crate::handlers::error::ErrorResponse)
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    path: Result<Path<LocationId>, PathRejection>,
) -> Result<Json<LocationInfoWithDetails>, ErrorResponse> {
    let id = match path {
        Ok(Path(path)) => path,
        Err(err) => {
            tracing::debug!(error=?err, "invalid location ID in path");
            return Err(Error::InvalidId { err }.into());
        }
    };

    match metadata_db::physical_table::get_by_id_with_details(&ctx.metadata_db, id).await {
        Ok(Some(location)) => Ok(Json(location.into())),
        Ok(None) => Err(Error::NotFound { id }.into()),
        Err(err) => {
            tracing::debug!(error=?err, location_id=?id, "failed to get location");
            Err(Error::MetadataDbError(err).into())
        }
    }
}

/// Errors that can occur during location retrieval
///
/// This enum represents all possible error conditions that can occur
/// when handling a `GET /locations/{id}` request, from path parsing
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

    /// An error occurred while querying the metadata database
    ///
    /// This covers database connection issues, query failures,
    /// and other internal database errors.
    #[error("metadata db error: {0}")]
    MetadataDbError(#[from] metadata_db::Error),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidId { .. } => "INVALID_LOCATION_ID",
            Error::NotFound { .. } => "LOCATION_NOT_FOUND",
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

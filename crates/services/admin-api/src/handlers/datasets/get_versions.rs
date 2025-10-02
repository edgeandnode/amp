//! Dataset versions get all handler

use axum::{
    Json,
    extract::{
        Path, Query, State,
        rejection::{PathRejection, QueryRejection},
    },
    http::StatusCode,
};
use datasets_common::{name::Name, version::Version};

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
};

/// Default number of dataset versions returned per page
const DEFAULT_PAGE_LIMIT: usize = 50;

/// Maximum number of dataset versions allowed per page
const MAX_PAGE_LIMIT: usize = 1000;

/// Query parameters for the dataset versions listing endpoint
#[serde_with::serde_as]
#[derive(Debug, serde::Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::IntoParams))]
#[cfg_attr(feature = "utoipa", into_params(parameter_in = Query))]
pub struct QueryParams {
    /// Maximum number of dataset versions to return (default: 50, max: 1000)
    #[serde(default = "default_limit")]
    #[cfg_attr(feature = "utoipa", param(minimum = 1, maximum = 1000))]
    limit: usize,

    /// Last version from the previous page for pagination
    #[serde_as(as = "Option<serde_with::DisplayFromStr>")]
    #[cfg_attr(feature = "utoipa", param(value_type = Option<String>))]
    last_version: Option<Version>,
}

fn default_limit() -> usize {
    DEFAULT_PAGE_LIMIT
}

/// Handler for the `GET /datasets/{name}/versions` endpoint
///
/// Retrieves and returns a paginated list of versions for a specific dataset from the metadata database registry.
///
/// ## Path Parameters
/// - `name`: Dataset name
///
/// ## Query Parameters
/// - `limit`: Maximum number of versions to return (default: 50, max: 1000)
/// - `last_version`: Last version from previous page for pagination - version string (e.g., "1.0.0")
///
/// ## Response
/// - **200 OK**: Returns paginated dataset versions with next cursor
/// - **400 Bad Request**: Invalid limit parameter (0, negative, or > 1000) or cursor format
/// - **404 Not Found**: Dataset with the given name does not exist
/// - **500 Internal Server Error**: Database connection or query error
///
/// ## Error Codes
/// - `INVALID_SELECTOR`: Invalid dataset name format
/// - `INVALID_QUERY_PARAMETERS`: Invalid query parameters format
/// - `LIMIT_TOO_LARGE`: Limit exceeds maximum allowed value
/// - `LIMIT_INVALID`: Limit is zero or negative
/// - `METADATA_DB_ERROR`: Internal database error occurred
///
/// ## Behavior
/// This handler provides comprehensive dataset version information from the registry including:
/// - All versions for the specified dataset from the metadata database
/// - Cursor-based pagination for efficient traversal of large version lists
/// - Proper ordering by version DESC (newest first)
///
/// The handler:
/// - Accepts path parameter for dataset name and query parameters for pagination (limit, last_version)
/// - Validates the dataset name and limit parameter (max 1000)
/// - Parses and validates version string format
/// - Calls the metadata DB to list dataset versions with pagination
/// - Returns a structured response with versions and next cursor
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        get,
        path = "/datasets/{name}/versions",
        tag = "datasets",
        operation_id = "datasets_list_versions",
        params(
            ("name" = String, Path, description = "Dataset name"),
            QueryParams
        ),
        responses(
            (status = 200, description = "Returns paginated dataset versions with next cursor", body = DatasetVersionsResponse),
            (status = 400, description = "Invalid limit parameter or cursor format"),
            (status = 404, description = "Dataset with the given name does not exist"),
            (status = 500, description = "Internal server error")
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    path: Result<Path<Name>, PathRejection>,
    query: Result<Query<QueryParams>, QueryRejection>,
) -> Result<Json<DatasetVersionsResponse>, ErrorResponse> {
    let name = match path {
        Ok(Path(name)) => name,
        Err(err) => {
            tracing::debug!(error=?err, "invalid dataset name path parameter");
            return Err(Error::InvalidSelector(err).into());
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

    // Fetch dataset versions from metadata DB
    let last_version = query.last_version.as_ref();

    let versions = ctx
        .metadata_db
        .list_dataset_versions(&name, limit as i64, last_version)
        .await
        .map_err(|err| {
            tracing::debug!(error=?err, dataset_name=%name, "failed to list dataset versions");
            Error::MetadataDbError(err)
        })?;

    // Determine next cursor (version of the last dataset version in this page)
    let next_cursor = versions.last().cloned().map(Into::into);
    let versions = versions.into_iter().take(limit).map(Into::into).collect();

    Ok(Json(DatasetVersionsResponse {
        versions,
        next_cursor,
    }))
}

/// Collection response for dataset versions listing with cursor-based pagination
#[serde_with::serde_as]
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct DatasetVersionsResponse {
    /// List of dataset versions in this page
    #[cfg_attr(feature = "utoipa", schema(value_type = Vec<String>))]
    pub versions: Vec<Version>,
    /// Cursor for the next page of results (None if no more results)
    #[serde_as(as = "Option<serde_with::DisplayFromStr>")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "utoipa", schema(value_type = Option<String>))]
    pub next_cursor: Option<Version>,
}

/// Errors that can occur during dataset versions listing
///
/// This enum represents all possible error conditions that can occur
/// when handling a `GET /datasets/{name}/versions` request with pagination parameters.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The dataset name selector is invalid
    ///
    /// This occurs when:
    /// - The dataset name contains invalid characters or doesn't follow naming conventions
    /// - The dataset name is empty or malformed
    /// - Path parameter extraction fails for dataset name
    #[error("invalid dataset selector: {0}")]
    InvalidSelector(PathRejection),

    /// The query parameters are invalid or malformed
    ///
    /// This occurs when query parameters cannot be parsed, such as:
    /// - Invalid integer format for limit
    /// - Invalid version format for last_version
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
            Error::InvalidSelector(_) => "INVALID_SELECTOR",
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
            Error::InvalidSelector(_) => StatusCode::BAD_REQUEST,
            Error::InvalidQueryParams { .. } => StatusCode::BAD_REQUEST,
            Error::LimitTooLarge { .. } => StatusCode::BAD_REQUEST,
            Error::LimitInvalid => StatusCode::BAD_REQUEST,
            Error::MetadataDbError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

//! Datasets get all handler

use axum::{
    Json,
    extract::{Query, State, rejection::QueryRejection},
    http::StatusCode,
};
use datasets_common::{
    name::{Name, NameError},
    version::{Version, VersionError},
};
use http_common::{BoxRequestError, RequestError};

use crate::ctx::Ctx;

/// Default number of datasets returned per page
const DEFAULT_PAGE_LIMIT: usize = 50;

/// Maximum number of datasets allowed per page
const MAX_PAGE_LIMIT: usize = 1000;

/// Query parameters for the datasets listing endpoint
#[serde_with::serde_as]
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct QueryParams {
    /// Maximum number of datasets to return (default: 50, max: 1000)
    #[serde(default = "default_limit")]
    limit: usize,

    /// Last dataset from the previous page for pagination in "name:version" format
    #[serde_as(as = "Option<serde_with::DisplayFromStr>")]
    last_dataset_id: Option<Cursor>,
}

fn default_limit() -> usize {
    DEFAULT_PAGE_LIMIT
}

/// Handler for the `GET /datasets` endpoint
///
/// Retrieves and returns a paginated list of datasets from the metadata database registry.
///
/// ## Query Parameters
/// - `limit`: Maximum number of datasets to return (default: 50, max: 1000)
/// - `last_dataset_id`: Last dataset from previous page for pagination in "name:version" format (e.g., "eth_mainnet:1.0.0")
///   - The colon `:` character is valid per RFC 3986 and doesn't require URL encoding
///   - Both encoded (`eth_mainnet%3A1.0.0`) and unencoded (`eth_mainnet:1.0.0`) formats are accepted
///   - For maximum compatibility across browsers, URL encoding the colon as `%3A` is recommended
///
/// ## Response
/// - **200 OK**: Returns paginated dataset data with next cursor
/// - **400 Bad Request**: Invalid limit parameter (0, negative, or > 1000) or cursor format
/// - **500 Internal Server Error**: Database connection or query error
///
/// ## Error Codes
/// - `INVALID_QUERY_PARAMETERS`: Invalid query parameters format
/// - `LIMIT_TOO_LARGE`: Limit exceeds maximum allowed value
/// - `LIMIT_INVALID`: Limit is zero or negative
/// - `METADATA_DB_ERROR`: Internal database error occurred
///
/// ## Behavior
/// This handler provides comprehensive dataset information from the registry including:
/// - Dataset names, versions, and owners from the metadata database
/// - Cursor-based pagination for efficient traversal of large dataset lists
/// - Proper ordering by dataset name ASC and version DESC (newest first within each dataset)
///
/// The handler:
/// - Accepts query parameters for pagination (limit, last_dataset_id)
/// - Validates the limit parameter (max 1000)
/// - Parses and validates cursor format ("name:version")
/// - Calls the metadata DB to list datasets with pagination
/// - Returns a structured response with datasets and next cursor
#[tracing::instrument(skip_all, err)]
pub async fn handler(
    State(ctx): State<Ctx>,
    query: Result<Query<QueryParams>, QueryRejection>,
) -> Result<Json<DatasetsResponse>, BoxRequestError> {
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

    // Fetch datasets from metadata DB
    let last_dataset = query
        .last_dataset_id
        .as_ref()
        .map(|Cursor(name, version)| (name, version));

    let datasets = ctx
        .metadata_db
        .list_datasets(limit as i64, last_dataset)
        .await
        .map_err(|err| {
            tracing::debug!(error=?err, "failed to list datasets");
            Error::MetadataDbError(err)
        })?;

    // Determine next cursor (name and version of the last dataset in this page)
    let next_cursor = datasets.last().map(|dataset| {
        // SAFETY: Dataset names are validated before insertion into the DB
        // The following parse should never fail unless the DB is corrupted
        let name = dataset.name.as_str().parse().expect("invalid dataset name");
        Cursor(name, dataset.version.clone().into())
    });
    let datasets = datasets.into_iter().take(limit).map(Into::into).collect();

    Ok(Json(DatasetsResponse {
        datasets,
        next_cursor,
    }))
}

/// Represents dataset information for API responses from the metadata database registry
#[derive(Debug, serde::Serialize)]
pub struct DatasetRegistryInfo {
    /// The name of the dataset
    pub name: Name,
    /// The version of the dataset
    pub version: Version,
    /// The owner of the dataset
    pub owner: String,
}

impl From<metadata_db::Dataset> for DatasetRegistryInfo {
    fn from(dataset: metadata_db::Dataset) -> Self {
        Self {
            name: dataset.name.into(),
            version: dataset.version.into(),
            owner: dataset.owner,
        }
    }
}

/// Collection response for dataset listings with cursor-based pagination
#[serde_with::serde_as]
#[derive(Debug, serde::Serialize)]
pub struct DatasetsResponse {
    /// List of datasets in this page
    pub datasets: Vec<DatasetRegistryInfo>,
    /// Cursor for the next page of results (None if no more results)
    #[serde_as(as = "Option<serde_with::DisplayFromStr>")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<Cursor>,
}

/// Cursor for dataset pagination containing both name and version
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Cursor(pub Name, pub Version);

impl Cursor {
    /// Access the name field
    pub fn name(&self) -> &Name {
        &self.0
    }

    /// Access the version field
    pub fn version(&self) -> &Version {
        &self.1
    }
}

impl std::str::FromStr for Cursor {
    type Err = InvalidCursorError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.split_once(':') {
            Some((name_str, version_str)) => {
                let name = name_str.parse().map_err(InvalidCursorError::InvalidName)?;
                let version = version_str
                    .parse()
                    .map_err(InvalidCursorError::InvalidVersion)?;
                Ok(Self(name, version))
            }
            None => Err(InvalidCursorError::InvalidFormat(s.to_string())),
        }
    }
}

impl std::fmt::Display for Cursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.name(), self.version())
    }
}

/// Errors that can occur when parsing a [`Cursor`] from string
#[derive(Debug, thiserror::Error)]
pub enum InvalidCursorError {
    /// The cursor format is invalid (missing colon separator)
    #[error("invalid cursor format '{0}', expected 'name:version'")]
    InvalidFormat(String),

    /// The dataset name is invalid
    #[error("invalid dataset name in cursor: {0}")]
    InvalidName(NameError),

    /// The dataset version is invalid
    #[error("invalid dataset version in cursor: {0}")]
    InvalidVersion(VersionError),
}

/// Errors that can occur during dataset listing
///
/// This enum represents all possible error conditions that can occur
/// when handling a `GET /datasets` request with pagination parameters.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The query parameters are invalid or malformed
    ///
    /// This occurs when query parameters cannot be parsed, such as:
    /// - Invalid integer format for limit
    /// - Invalid cursor format (not "name:version")
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

impl RequestError for Error {
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

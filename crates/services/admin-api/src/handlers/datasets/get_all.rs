//! Datasets get all handler

use axum::{Json, extract::State, http::StatusCode};
use datasets_common::{name::Name, version_tag::VersionTag};

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
};

/// Handler for the `GET /datasets` endpoint
///
/// Retrieves and returns a complete list of all datasets from the metadata database registry.
///
/// ## Response
/// - **200 OK**: Returns all datasets
/// - **500 Internal Server Error**: Database connection or query error
///
/// ## Error Codes
/// - `METADATA_DB_ERROR`: Internal database error occurred
///
/// ## Behavior
/// This handler provides comprehensive dataset information from the registry including:
/// - Dataset names, versions, and owners from the metadata database
/// - Lexicographical ordering by dataset name ASC, then version string DESC within each dataset
///
/// The handler:
/// - Calls the metadata DB to list all datasets
/// - Returns a structured response with all datasets
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        get,
        path = "/datasets",
        tag = "datasets",
        operation_id = "datasets_list",
        responses(
            (status = 200, description = "Returns all datasets", body = DatasetsResponse),
            (status = 500, description = "Internal server error")
        )
    )
)]
pub async fn handler(State(ctx): State<Ctx>) -> Result<Json<DatasetsResponse>, ErrorResponse> {
    // Fetch all datasets from metadata DB
    let datasets = ctx.metadata_db.list_datasets().await.map_err(|err| {
        tracing::debug!(error=?err, "failed to list datasets");
        Error::MetadataDbError(err)
    })?;

    let datasets = datasets.into_iter().map(Into::into).collect();

    Ok(Json(DatasetsResponse { datasets }))
}

/// Represents dataset information for API responses from the metadata database registry
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct DatasetRegistryInfo {
    /// The name of the dataset
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub name: Name,
    /// The version of the dataset
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub version: VersionTag,
}

impl From<metadata_db::Dataset> for DatasetRegistryInfo {
    fn from(dataset: metadata_db::Dataset) -> Self {
        Self {
            name: dataset.name.into(),
            version: dataset.version.into(),
        }
    }
}

/// Collection response for dataset listings
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct DatasetsResponse {
    /// List of all datasets
    pub datasets: Vec<DatasetRegistryInfo>,
}

/// Errors that can occur during dataset listing
///
/// This enum represents all possible error conditions that can occur
/// when handling a `GET /datasets` request.
#[derive(Debug, thiserror::Error)]
pub enum Error {
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
            Error::MetadataDbError(_) => "METADATA_DB_ERROR",
        }
    }

    /// Returns the appropriate HTTP status code for each error type
    ///
    /// Maps internal error types to standard HTTP status codes:
    /// - Database Error → 500 Internal Server Error
    fn status_code(&self) -> StatusCode {
        match self {
            Error::MetadataDbError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

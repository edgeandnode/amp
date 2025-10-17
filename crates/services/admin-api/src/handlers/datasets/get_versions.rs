//! Dataset versions get all handler

use axum::{
    Json,
    extract::{Path, State, rejection::PathRejection},
    http::StatusCode,
};
use datasets_common::{name::Name, version_tag::VersionTag};

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
};

/// Handler for the `GET /datasets/{name}/versions` endpoint
///
/// Retrieves and returns a complete list of all versions for a specific dataset from the metadata database registry.
///
/// ## Path Parameters
/// - `name`: Dataset name
///
/// ## Response
/// - **200 OK**: Returns all dataset versions
/// - **400 Bad Request**: Invalid dataset name format
/// - **500 Internal Server Error**: Database connection or query error
///
/// ## Error Codes
/// - `INVALID_SELECTOR`: Invalid dataset name format
/// - `METADATA_DB_ERROR`: Internal database error occurred
///
/// ## Behavior
/// This handler provides comprehensive dataset version information from the registry including:
/// - All versions for the specified dataset from the metadata database
/// - Lexicographical ordering by version string DESC (e.g., "2.0.0" > "1.9.0" > "1.2.3" > "1.10.0")
///
/// The handler:
/// - Accepts path parameter for dataset name
/// - Validates the dataset name
/// - Calls the metadata DB to list all dataset versions
/// - Returns a structured response with all versions
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
        ),
        responses(
            (status = 200, description = "Returns all dataset versions", body = DatasetVersionsResponse),
            (status = 400, description = "Invalid dataset name format"),
            (status = 500, description = "Internal server error")
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    path: Result<Path<Name>, PathRejection>,
) -> Result<Json<DatasetVersionsResponse>, ErrorResponse> {
    let name = match path {
        Ok(Path(name)) => name,
        Err(err) => {
            tracing::debug!(error=?err, "invalid dataset name path parameter");
            return Err(Error::InvalidSelector(err).into());
        }
    };

    // Fetch all dataset versions from metadata DB
    let versions = ctx
        .metadata_db
        .list_dataset_versions(&name)
        .await
        .map_err(|err| {
            tracing::debug!(error=?err, dataset_name=%name, "failed to list dataset versions");
            Error::MetadataDbError(err)
        })?;

    let versions = versions.into_iter().map(Into::into).collect();

    Ok(Json(DatasetVersionsResponse { versions }))
}

/// Collection response for dataset versions listing
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct DatasetVersionsResponse {
    /// List of all dataset versions
    #[cfg_attr(feature = "utoipa", schema(value_type = Vec<String>))]
    pub versions: Vec<VersionTag>,
}

/// Errors that can occur during dataset versions listing
///
/// This enum represents all possible error conditions that can occur
/// when handling a `GET /datasets/{name}/versions` request.
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
            Error::MetadataDbError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

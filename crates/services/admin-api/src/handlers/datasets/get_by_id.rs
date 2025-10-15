use axum::{
    Json,
    extract::{Path, State, rejection::PathRejection},
    http::StatusCode,
};
use datasets_common::{name::Name, version::Version};
use metadata_db::TableId;

use super::tracing::display_selector_version;
use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
};

/// Handler for dataset retrieval endpoint without version
///
/// Retrieves detailed information about the latest version of a specific dataset,
/// including its tables and active locations.
/// URL pattern: `GET /datasets/{name}`
///
/// ## Path Parameters
/// - `name`: Dataset name
///
/// ## Response
/// - **200 OK**: Returns the dataset information as JSON
/// - **400 Bad Request**: Invalid dataset name or invalid request parameters
/// - **404 Not Found**: Dataset with the given name does not exist
/// - **500 Internal Server Error**: Dataset store or database connection error
///
/// ## Error Codes
/// - `INVALID_SELECTOR`: The provided dataset name is not valid (invalid name format or parsing error)
/// - `DATASET_NOT_FOUND`: No dataset exists with the given name
/// - `DATASET_STORE_ERROR`: Failed to load dataset from the dataset store
/// - `METADATA_DB_ERROR`: Database error while retrieving active locations for tables
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        get,
        path = "/datasets/{id}",
        tag = "datasets",
        operation_id = "datasets_get",
        params(
            ("id" = String, Path, description = "Dataset name")
        ),
        responses(
            (status = 200, description = "Successfully retrieved dataset information", body = DatasetInfo),
            (status = 400, description = "Invalid dataset name", body = crate::handlers::error::ErrorResponse),
            (status = 404, description = "Dataset not found", body = crate::handlers::error::ErrorResponse),
            (status = 500, description = "Internal server error", body = crate::handlers::error::ErrorResponse)
        )
    )
)]
pub async fn handler(
    State(ctx): State<Ctx>,
    path: Result<Path<Name>, PathRejection>,
) -> Result<Json<DatasetInfo>, ErrorResponse> {
    let name = match path {
        Ok(Path(name)) => name,
        Err(err) => {
            tracing::debug!(error=?err, "invalid dataset path parameters");
            return Err(Error::InvalidSelector(err).into());
        }
    };

    handler_inner(ctx, name, None).await
}

/// Handler for dataset retrieval endpoint with specific version
///
/// Retrieves detailed information about a specific version of a dataset,
/// including its tables and active locations.
/// URL pattern: `GET /datasets/{name}/versions/{version}`
///
/// ## Path Parameters
/// - `name`: Dataset name
/// - `version`: Specific dataset version
///
/// ## Response
/// - **200 OK**: Returns the dataset information as JSON
/// - **400 Bad Request**: Invalid dataset name/version or invalid request parameters
/// - **404 Not Found**: Dataset with the given name/version does not exist
/// - **500 Internal Server Error**: Dataset store or database connection error
///
/// ## Error Codes
/// - `INVALID_SELECTOR`: The provided dataset name or version is not valid (invalid name format, malformed version, or parsing error)
/// - `DATASET_NOT_FOUND`: No dataset exists with the given name/version
/// - `DATASET_STORE_ERROR`: Failed to load dataset from the dataset store
/// - `METADATA_DB_ERROR`: Database error while retrieving active locations for tables
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        get,
        path = "/datasets/{name}/versions/{version}",
        tag = "datasets",
        operation_id = "datasets_get_version",
        params(
            ("name" = String, Path, description = "Dataset name"),
            ("version" = String, Path, description = "Dataset version")
        ),
        responses(
            (status = 200, description = "Successfully retrieved dataset information", body = DatasetInfo),
            (status = 400, description = "Invalid dataset name or version", body = crate::handlers::error::ErrorResponse),
            (status = 404, description = "Dataset not found", body = crate::handlers::error::ErrorResponse),
            (status = 500, description = "Internal server error", body = crate::handlers::error::ErrorResponse)
        )
    )
)]
pub async fn handler_with_version(
    State(ctx): State<Ctx>,
    path: Result<Path<(Name, Version)>, PathRejection>,
) -> Result<Json<DatasetInfo>, ErrorResponse> {
    let (name, version) = match path {
        Ok(Path((name, version))) => (name, version),
        Err(err) => {
            tracing::debug!(error=?err, "invalid dataset path parameters");
            return Err(Error::InvalidSelector(err).into());
        }
    };

    handler_inner(ctx, name, Some(version)).await
}

/// Common logic for retrieving dataset information
async fn handler_inner(
    ctx: Ctx,
    name: Name,
    version: Option<Version>,
) -> Result<Json<DatasetInfo>, ErrorResponse> {
    tracing::debug!(
        dataset_name=%name,
        dataset_version=%display_selector_version(&version),
        "loading dataset from store"
    );

    // Get the dataset from the store
    // If version is None, the latest version is used
    let dataset = match ctx.dataset_store.get_dataset(&name, version.as_ref()).await {
        Ok(Some(dataset)) => dataset,
        Ok(None) => {
            tracing::debug!(
                dataset_name=%name,
                dataset_version=%display_selector_version(&version),
                "dataset not found"
            );
            return Err(Error::NotFound {
                name: name.clone(),
                version: version.clone(),
            }
            .into());
        }
        Err(err) => {
            tracing::debug!(
                dataset_name=%name,
                dataset_version=%display_selector_version(&version),
                error=?err,
                "failed to load dataset"
            );
            return Err(Error::DatasetStoreError(err).into());
        }
    };

    let mut tables = Vec::with_capacity(dataset.tables.len());
    let dataset_version = match dataset.kind.as_str() {
        "manifest" => dataset.dataset_version(),
        _ => None,
    };

    for table in dataset.tables {
        let table_name = table.name().to_string();
        let network = table.network().to_string();

        // Resolve active location for this table
        let table_id = TableId {
            dataset: &dataset.name,
            dataset_version: dataset_version.as_deref(),
            table: table.name(),
        };

        let active_location = ctx
            .metadata_db
            .get_active_location(table_id)
            .await
            .map_err(|err| {
                tracing::debug!(
                    dataset_name=%name,
                    dataset_version=%display_selector_version(&version),
                    table=%table.name(),
                    error=?err,
                    "failed to get active location for table"
                );
                Error::MetadataDbError(err)
            })?
            .map(|(url, _)| url.to_string());

        tables.push(TableInfo {
            name: table_name,
            active_location,
            network,
        });
    }

    Ok(Json(DatasetInfo {
        name: dataset.name,
        version: dataset.version.unwrap_or_default(),
        kind: dataset.kind,
        tables,
    }))
}

/// Represents dataset information for API responses from the dataset store
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct DatasetInfo {
    /// The name of the dataset (validated identifier)
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub name: Name,
    /// The version of the dataset using semantic versioning (e.g., "1.0.0")
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub version: Version,
    /// The kind/type of dataset (e.g., "evm-rpc", "firehose", "sql")
    pub kind: String,
    /// List of tables contained in the dataset with their details
    pub tables: Vec<TableInfo>,
}

/// Represents table information within a dataset
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct TableInfo {
    /// The name of the table
    pub name: String,
    /// Currently active location URL for this table
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_location: Option<String>,
    /// Associated network for this table
    pub network: String,
}

/// Errors that can occur during dataset retrieval
///
/// This enum represents all possible error conditions when handling
/// a request to retrieve a specific dataset by name and optional version.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The dataset selector is invalid
    ///
    /// This occurs when:
    /// - The dataset name contains invalid characters or doesn't follow naming conventions
    /// - The dataset name is empty or malformed
    /// - The version syntax is invalid (e.g., malformed semver)
    /// - Path parameter extraction fails for dataset selection
    #[error("invalid dataset selector: {0}")]
    InvalidSelector(PathRejection),

    /// Dataset not found
    ///
    /// This occurs when:
    /// - No dataset exists with the given name/version
    /// - The dataset has been deleted or moved
    /// - Dataset configuration is missing
    #[error("dataset '{name}'{} not found", version.as_ref().map(|v| format!(" version '{}'", v)).unwrap_or_default())]
    NotFound {
        name: Name,
        version: Option<Version>,
    },

    /// Dataset store error while getting the dataset
    ///
    /// This occurs when:
    /// - The dataset store is not accessible
    /// - There's a configuration error in the store
    /// - I/O errors while reading dataset definitions
    #[error("dataset store error: {0}")]
    DatasetStoreError(#[from] dataset_store::GetDatasetError),

    /// Metadata database error while retrieving active locations
    ///
    /// This occurs when:
    /// - Database connection is lost
    /// - SQL query errors while fetching table locations
    /// - Database schema inconsistencies
    #[error("metadata database error: {0}")]
    MetadataDbError(#[from] metadata_db::Error),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidSelector(_) => "INVALID_SELECTOR",
            Error::NotFound { .. } => "DATASET_NOT_FOUND",
            Error::DatasetStoreError(_) => "DATASET_STORE_ERROR",
            Error::MetadataDbError(_) => "METADATA_DB_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidSelector(_) => StatusCode::BAD_REQUEST,
            Error::NotFound { .. } => StatusCode::NOT_FOUND,
            Error::DatasetStoreError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::MetadataDbError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

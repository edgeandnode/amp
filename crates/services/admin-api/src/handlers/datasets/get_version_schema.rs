use axum::{
    Json,
    extract::{Path, State, rejection::PathRejection},
    http::StatusCode,
};
use datasets_common::{manifest::DataType, name::Name, version::Version};
use datasets_derived::manifest::{ArrowSchema, Field, TableSchema};

use crate::{
    ctx::Ctx,
    handlers::error::{ErrorResponse, IntoErrorResponse},
};

/// Handler for dataset schema retrieval endpoint with specific version
///
/// Retrieves the schema for all tables in a specific version of a dataset.
/// URL pattern: `GET /datasets/{name}/versions/{version}/schema`
///
/// ## Path Parameters
/// - `name`: Dataset name
/// - `version`: Specific dataset version
///
/// ## Response
/// - **200 OK**: Returns the dataset schema information as JSON
/// - **400 Bad Request**: Invalid dataset name/version or invalid request parameters
/// - **404 Not Found**: Dataset with the given name/version does not exist
/// - **500 Internal Server Error**: Dataset store error
///
/// ## Error Codes
/// - `INVALID_SELECTOR`: The provided dataset name or version is not valid (invalid name format, malformed version, or parsing error)
/// - `DATASET_NOT_FOUND`: No dataset exists with the given name/version
/// - `DATASET_STORE_ERROR`: Failed to load dataset from the dataset store
///
/// This handler:
/// - Validates and extracts the dataset name and version from the URL path
/// - Loads the dataset from the dataset store
/// - Converts table schemas to TableSchemaInfo format with Arrow field details
/// - Returns the dataset schema information as JSON
#[tracing::instrument(skip_all, err)]
#[cfg_attr(
    feature = "utoipa",
    utoipa::path(
        get,
        path = "/datasets/{name}/versions/{version}/schema",
        tag = "datasets",
        operation_id = "datasets_get_version_schema",
        params(
            ("name" = String, Path, description = "Dataset name"),
            ("version" = String, Path, description = "Dataset version")
        ),
        responses(
            (status = 200, description = "Successfully retrieved dataset schema", body = DatasetSchemaResponse),
            (status = 400, description = "Invalid dataset name or version", body = crate::handlers::error::ErrorResponse),
            (status = 404, description = "Dataset not found", body = crate::handlers::error::ErrorResponse),
            (status = 500, description = "Internal server error", body = crate::handlers::error::ErrorResponse)
        )
    )
)]
pub async fn handler_with_version(
    State(ctx): State<Ctx>,
    path: Result<Path<(Name, Version)>, PathRejection>,
) -> Result<Json<DatasetSchemaResponse>, ErrorResponse> {
    let (name, version) = match path {
        Ok(Path((name, version))) => (name, version),
        Err(err) => {
            tracing::debug!(error=?err, "invalid dataset path parameters");
            return Err(Error::InvalidSelector(err).into());
        }
    };

    tracing::debug!(
        dataset_name=%name,
        dataset_version=%version,
        "loading dataset schema from store"
    );

    // Get the dataset from the store
    let dataset = match ctx.dataset_store.get_dataset(&name, Some(&version)).await {
        Ok(Some(dataset)) => dataset,
        Ok(None) => {
            tracing::debug!(
                dataset_name=%name,
                dataset_version=%version,
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
                dataset_version=%version,
                error=?err,
                "failed to load dataset"
            );
            return Err(Error::DatasetStoreError(err).into());
        }
    };

    // Convert each table's schema to TableSchema format
    let mut tables = Vec::with_capacity(dataset.tables.len());
    for table in dataset.tables {
        let table_name = table.name().to_string();
        let network = table.network().to_string();

        // Convert SchemaRef to TableSchema
        // We need to manually construct TableSchema from the Arrow Schema
        let schema = table.schema();
        let table_schema = TableSchema {
            arrow: ArrowSchema {
                fields: schema
                    .fields()
                    .iter()
                    .map(|f| Field {
                        name: f.name().clone(),
                        type_: DataType(f.data_type().clone()),
                        nullable: f.is_nullable(),
                    })
                    .collect(),
            },
        };

        tables.push(TableSchemaInfo {
            name: table_name,
            network,
            schema: table_schema,
        });
    }

    Ok(Json(DatasetSchemaResponse {
        name: dataset.name,
        version: dataset.version.unwrap_or_default(),
        tables,
    }))
}

/// Represents dataset schema information for API responses
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct DatasetSchemaResponse {
    /// The name of the dataset (validated identifier)
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub name: Name,
    /// The version of the dataset using semantic versioning (e.g., "1.0.0")
    #[cfg_attr(feature = "utoipa", schema(value_type = String))]
    pub version: Version,
    /// List of tables with their schemas
    pub tables: Vec<TableSchemaInfo>,
}

/// Represents table schema information within a dataset
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct TableSchemaInfo {
    /// The name of the table
    pub name: String,
    /// Associated network for this table
    pub network: String,
    /// The schema for this table
    #[cfg_attr(feature = "utoipa", schema(value_type = serde_json::Value))]
    pub schema: TableSchema,
}

/// Errors that can occur during dataset schema retrieval
///
/// This enum represents all possible error conditions when handling
/// a request to retrieve the schema for a specific dataset by name and version.
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
    #[error("dataset '{name}' version '{version}' not found")]
    NotFound { name: Name, version: Version },

    /// Dataset store error while getting the dataset
    ///
    /// This occurs when:
    /// - The dataset store is not accessible
    /// - There's a configuration error in the store
    /// - I/O errors while reading dataset definitions
    #[error("dataset store error: {0}")]
    DatasetStoreError(#[from] dataset_store::GetDatasetError),
}

impl IntoErrorResponse for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidSelector(_) => "INVALID_SELECTOR",
            Error::NotFound { .. } => "DATASET_NOT_FOUND",
            Error::DatasetStoreError(_) => "DATASET_STORE_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidSelector(_) => StatusCode::BAD_REQUEST,
            Error::NotFound { .. } => StatusCode::NOT_FOUND,
            Error::DatasetStoreError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

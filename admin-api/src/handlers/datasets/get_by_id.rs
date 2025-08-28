//! Dataset get by ID handler

use axum::{
    Json,
    extract::{Path, State},
};
use common::Dataset;
use http_common::BoxRequestError;
use metadata_db::TableId;

use super::error::Error;
use crate::{ctx::Ctx, handlers::common::validate_dataset_name};

/// Handler for the `GET /datasets/{id}` endpoint
///
/// Retrieves detailed information about a specific dataset by its ID, including
/// its tables and active locations.
///
/// ## Path Parameters
/// - `id`: The unique identifier of the dataset to retrieve (dataset name)
///
/// ## Response
/// - **200 OK**: Returns the dataset information as JSON
/// - **400 Bad Request**: Invalid dataset ID format (invalid dataset name)
/// - **404 Not Found**: Dataset with the given ID does not exist
/// - **500 Internal Server Error**: Dataset store or database connection error
///
/// ## Error Codes
/// - `INVALID_ID`: The provided dataset name is not valid (contains invalid characters or format)
/// - `NOT_FOUND`: No dataset exists with the given name
/// - `STORE_ERROR`: Failed to load dataset from the dataset store
/// - `METADATA_DB_ERROR`: Database error while retrieving active locations for tables
///
/// ## Behavior
/// This handler provides detailed information for a specific dataset including:
/// - Dataset name, type, and version information
/// - Complete list of tables within the dataset with their network associations
/// - Active storage locations for each table (if available via metadata DB)
/// - Table-level metadata and configuration details
///
/// The handler:
/// - Validates the dataset ID (name) format according to naming conventions
/// - Retrieves the specified dataset from the configured dataset store
/// - Collects comprehensive information about all tables in the dataset
/// - For each table, determines its currently active storage location
/// - Returns a structured JSON response with the collected information
/// - Handles cases where the dataset doesn't exist or metadata DB is unavailable
#[tracing::instrument(skip_all, err)]
pub async fn handler(
    State(ctx): State<Ctx>,
    Path(id): Path<String>,
) -> Result<Json<DatasetInfo>, BoxRequestError> {
    // Validate the dataset ID (the dataset name)
    if let Err(err) = validate_dataset_name(&id) {
        tracing::debug!(id=%id, error=?err, "invalid dataset ID");
        return Err(Error::InvalidId {
            name: id,
            source: err,
        }
        .into());
    }

    let dataset = ctx.store.load_dataset(&id, None).await.map_err(|err| {
        tracing::debug!(id=%id, error=?err, "failed to load dataset");
        if err.is_not_found() {
            Error::NotFound { name: id }
        } else {
            Error::StoreError(err)
        }
    })?;

    let dataset_info = try_into_dataset_response(&ctx, dataset).await?;

    Ok(Json(dataset_info))
}

/// Transforms a dataset object into a response type with location information
async fn try_into_dataset_response(ctx: &Ctx, dataset: Dataset) -> Result<DatasetInfo, Error> {
    let mut table_infos = Vec::with_capacity(dataset.tables.len());
    let dataset_version = match dataset.kind.as_str() {
        "manifest" => dataset.dataset_version(),
        _ => None,
    };
    for table in dataset.tables {
        let table_id = TableId {
            dataset: &dataset.name,
            dataset_version: dataset_version.as_deref(),
            table: table.name(),
        };

        // Resolve active location for this table
        let active_location = ctx
            .metadata_db
            .get_active_location(table_id)
            .await
            .map_err(|err| {
                tracing::debug!(table=%table.name(), error=?err, "failed to get active location for table");
                Error::MetadataDbError(err)
            })?
            .map(|(url, _)| url.to_string());

        table_infos.push(TableInfo {
            name: table.name().to_string(),
            active_location,
            network: table.network().to_string(),
        });
    }

    Ok(DatasetInfo {
        name: dataset.name,
        kind: dataset.kind,
        tables: table_infos,
    })
}

/// Represents dataset information for the API response
#[derive(Debug, serde::Serialize)]
pub struct DatasetInfo {
    /// The name of the dataset
    pub name: String,
    /// The kind of dataset (e.g., "subgraph", "firehose")
    pub kind: String,
    /// List of tables contained in the dataset
    pub tables: Vec<TableInfo>,
}

/// Represents table information within a dataset
#[derive(Debug, serde::Serialize)]
pub struct TableInfo {
    /// The name of the table
    pub name: String,
    /// Currently active location URL for this table
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_location: Option<String>,
    /// Associated network for this table
    pub network: String,
}

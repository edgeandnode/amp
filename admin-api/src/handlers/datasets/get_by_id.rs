//! Dataset get by ID handler

use axum::{
    extract::{Path, State},
    Json,
};
use common::Dataset;
use http_common::BoxRequestError;
use metadata_db::TableId;

use super::error::Error;
use crate::{ctx::Ctx, handlers::common::validate_dataset_name};

/// Handler for the `GET /datasets/:id` endpoint
///
/// Retrieves and returns information about a specific dataset by ID (dataset name),
/// including its tables and active locations.
///
/// This handler:
/// - Validates the dataset ID format
/// - Retrieves the specified dataset from the dataset store
/// - Collects information about its tables
/// - For each table, attempts to determine its active location if a metadata DB is available
/// - Returns a structured response with the collected information
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

    let dataset = ctx.store.load_dataset(&id).await.map_err(|err| {
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
    for table in dataset.tables {
        let table_id = TableId {
            dataset: &dataset.name,
            dataset_version: None,
            table: &table.name,
        };

        // Resolve active location for this table
        let active_location = ctx
            .metadata_db
            .get_active_location(table_id)
            .await
            .map_err(|err| {
                tracing::debug!(table=%table.name, error=?err, "failed to get active location for table");
                Error::MetadataDbError(err)
            })?
            .map(|(url, _)| url.to_string());

        table_infos.push(TableInfo {
            name: table.name,
            active_location,
            network: table.network,
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

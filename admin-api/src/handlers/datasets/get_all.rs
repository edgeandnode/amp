//! Datasets HTTP handlers

use axum::{extract::State, http::StatusCode, Json};
use common::catalog::logical::DatasetWithProvider;
use http_common::{BoxRequestError, RequestError};
use metadata_db::TableId;

use crate::ctx::Ctx;

/// Handler for the `GET /datasets` endpoint
///
/// Retrieves and returns information about all datasets in the system, including their tables
/// and active locations if a metadata DB is configured.
///
/// This handler:
/// - Retrieves all datasets from the dataset store
/// - For each dataset, collects information about its tables
/// - For each table, attempts to determine its active location if a metadata DB is available
/// - Returns a structured response with the collected information
#[tracing::instrument(skip_all, err)]
pub async fn handler(State(ctx): State<Ctx>) -> Result<Json<DatasetsResponse>, BoxRequestError> {
    let datasets_with_provider = ctx
        .store
        .all_datasets()
        .await
        .map_err(DatasetsError::StoreError)?;

    let datasets_response = try_into_datasets_response(&ctx, datasets_with_provider).await?;

    Ok(Json(datasets_response))
}

/// Transforms dataset objects into response types with location information
async fn try_into_datasets_response(
    ctx: &Ctx,
    datasets: impl IntoIterator<Item = DatasetWithProvider>,
) -> Result<DatasetsResponse, DatasetsError> {
    let mut dataset_infos = Vec::new();
    for DatasetWithProvider { dataset, .. } in datasets {
        // Get table information for each table in the dataset
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
                .map_err(DatasetsError::MetadataDbError)?
                .map(|(url, _)| url.to_string());

            table_infos.push(TableInfo {
                name: table.name,
                active_location,
                network: table.network,
            });
        }

        dataset_infos.push(DatasetInfo {
            name: dataset.name,
            kind: dataset.kind,
            tables: table_infos,
        });
    }

    Ok(DatasetsResponse {
        datasets: dataset_infos,
    })
}

/// API response containing dataset information
#[derive(Debug, serde::Serialize)]
pub struct DatasetsResponse {
    /// List of datasets available in the system
    pub datasets: Vec<DatasetInfo>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub network: Option<String>,
}

/// Errors that can occur when processing dataset requests
#[derive(Debug, thiserror::Error)]
enum DatasetsError {
    /// Error originating from the dataset store
    #[error("failed to get datasets: {0}")]
    StoreError(dataset_store::DatasetError),

    /// Error originating from the metadata database
    #[error("metadata database error: {0}")]
    MetadataDbError(metadata_db::Error),
}

impl RequestError for DatasetsError {
    /// Maps dataset errors to HTTP status codes
    fn status_code(&self) -> StatusCode {
        match self {
            DatasetsError::StoreError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            DatasetsError::MetadataDbError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// Provides error codes for API responses
    fn error_code(&self) -> &'static str {
        match self {
            DatasetsError::StoreError(_) => "DATASET_STORE_ERROR",
            DatasetsError::MetadataDbError(_) => "METADATA_DB_ERROR",
        }
    }
}

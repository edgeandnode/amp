use axum::{Json, extract::State, http::StatusCode};
use http_common::{BoxRequestError, RequestError};
use metadata_db::TableId;

use super::dataset_info::{DatasetInfo, TableInfo};
use crate::ctx::Ctx;

/// Handler for the `GET /datasets` endpoint
///
/// Lists all available datasets in the system with their table information and
/// active locations.
///
/// ## Response
/// - **200 OK**: Returns list of datasets with their tables and location information
/// - **500 Internal Server Error**: Dataset store or database connection error
///
/// ## Error Codes
/// - `DATASET_STORE_ERROR`: Failed to retrieve datasets from the dataset store
/// - `METADATA_DB_ERROR`: Database error while retrieving active locations for tables
///
/// ## Behavior
/// This handler provides comprehensive dataset information including:
/// - Dataset names and types (manifest, SQL, etc.)
/// - All tables within each dataset with their network associations
/// - Active storage locations for each table (if available via metadata DB)
/// - Dataset version information for manifest-based datasets
///
/// The handler:
/// - Retrieves all datasets from the configured dataset store
/// - For each dataset, collects detailed information about its tables
/// - For each table, attempts to determine its currently active storage location
/// - Returns a structured JSON response with the collected information
/// - Handles cases where metadata DB is unavailable (active_location will be null)
#[tracing::instrument(skip_all, err)]
pub async fn handler(State(ctx): State<Ctx>) -> Result<Json<DatasetsResponse>, BoxRequestError> {
    let datasets_with_provider = ctx.store.get_all_datasets().await.map_err(|err| {
        tracing::debug!(error=?err, "failed to load all datasets");
        Error::DatasetStoreError(err)
    })?;

    let mut datasets = Vec::new();
    for dataset in datasets_with_provider {
        // Get table information for each table in the dataset
        let mut tables = Vec::with_capacity(dataset.tables.len());
        let dataset_version = match dataset.kind.as_str() {
            "manifest" => dataset.dataset_version(),
            _ => None,
        };

        for table in dataset.tables {
            let name = table.name().to_string();
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
                        table = %table.name(),
                        error = ?err,
                        "failed to get active location for table"
                    );
                    Error::MetadataDbError(err)
                })?
                .map(|(url, _)| url.to_string());

            tables.push(TableInfo {
                name,
                active_location,
                network,
            });
        }

        datasets.push(DatasetInfo {
            name: dataset.name,
            version: dataset.version,
            kind: dataset.kind,
            tables,
        });
    }

    Ok(Json(DatasetsResponse { datasets }))
}

/// API response containing multiple dataset information
///
/// This response type is specific to the get_all handler and contains
/// a collection of datasets with their detailed information.
#[derive(Debug, serde::Serialize)]
pub struct DatasetsResponse {
    /// List of datasets available in the system
    pub datasets: Vec<DatasetInfo>,
}

/// Errors that can occur during dataset listing
///
/// This enum represents all possible error conditions when handling
/// a request to list all datasets in the system.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Dataset store error while retrieving datasets
    ///
    /// This occurs when:
    /// - The dataset store is not accessible
    /// - There's a configuration error in the store
    /// - I/O errors while reading dataset definitions
    #[error("dataset store error: {0}")]
    DatasetStoreError(#[from] dataset_store::GetAllDatasetsError),
    /// Metadata database error while retrieving active locations
    ///
    /// This occurs when:
    /// - Database connection is lost
    /// - SQL query errors while fetching table locations
    /// - Database schema inconsistencies
    #[error("metadata database error: {0}")]
    MetadataDbError(#[from] metadata_db::Error),
}

impl RequestError for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::DatasetStoreError(_) => "DATASET_STORE_ERROR",
            Error::MetadataDbError(_) => "METADATA_DB_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::DatasetStoreError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::MetadataDbError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

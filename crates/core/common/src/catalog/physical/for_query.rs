//! Arrow Flight physical catalog construction.
//!
//! This module provides physical catalog creation for Arrow Flight query execution.
//! It accepts a pre-created logical catalog and adds physical parquet locations.

use std::sync::Arc;

use amp_data_store::DataStore;
use datasets_common::{hash_reference::HashReference, table_name::TableName};

use crate::catalog::{
    dataset_access::DatasetAccess,
    logical::LogicalCatalog,
    physical::{Catalog, PhysicalTable},
};

/// Creates a full catalog with physical data access for SQL query execution.
///
/// This function builds a complete catalog by adding physical parquet file locations
/// to an existing logical catalog, enabling actual query execution with DataFusion.
///
/// ## Parameters
///
/// - `dataset_store`: Used to retrieve dataset metadata including start_block
/// - `data_store`: Used to query metadata database for physical parquet locations
/// - `logical`: Pre-created logical catalog containing table schemas and UDFs
///
/// ## Where Used
///
/// This function is used exclusively in the **Query Execution Path**:
///
/// - **Arrow Flight DoGet** (`crates/services/server/src/flight.rs`):
///   - Called during Arrow Flight `DoGet` phase to execute user queries
///   - Provides physical catalog for streaming query results to clients
///
/// ## Implementation
///
/// The function:
/// 1. Iterates through tables in the logical catalog
/// 2. Queries metadata database for physical parquet locations
/// 3. Retrieves dataset metadata to get start_block
/// 4. Constructs physical catalog for query execution
pub async fn create(
    dataset_store: &impl DatasetAccess,
    data_store: &DataStore,
    logical: LogicalCatalog,
) -> Result<Catalog, CreateCatalogError> {
    let mut physical_tables = Vec::new();
    for table in &logical.tables {
        let dataset_ref = table.dataset_reference();

        let revision = data_store
            .get_table_active_revision(dataset_ref, table.name())
            .await
            .map_err(|source| CreateCatalogError::PhysicalTableRetrieval {
                dataset: dataset_ref.clone(),
                table: table.name().clone(),
                source,
            })?
            .ok_or_else(|| CreateCatalogError::TableNotSynced {
                dataset: dataset_ref.clone(),
                table: table.name().clone(),
            })?;

        // Retrieve dataset to get start_block
        let dataset = dataset_store
            .get_dataset(dataset_ref)
            .await
            .map_err(|source| CreateCatalogError::DatasetRetrieval {
                dataset: dataset_ref.clone(),
                source,
            })?;

        let physical_table = PhysicalTable::from_active_revision(
            data_store.clone(),
            table.dataset_reference().clone(),
            dataset.start_block,
            table.table().clone(),
            revision,
            table.sql_table_ref_schema().to_string(),
        );
        physical_tables.push(Arc::new(physical_table));
    }

    Ok(Catalog::new(physical_tables, logical))
}

/// Errors that can occur when creating a physical catalog.
///
/// Returned by [`create`] when catalog creation fails.
#[derive(Debug, thiserror::Error)]
pub enum CreateCatalogError {
    /// Failed to retrieve physical table metadata from the metadata database.
    ///
    /// This occurs when querying the metadata database for the active physical
    /// location of a table fails due to database connection issues, query errors,
    /// or other database-related problems.
    #[error("Failed to retrieve physical table metadata for table {dataset}.{table}")]
    PhysicalTableRetrieval {
        /// The hash reference of the dataset containing the table
        dataset: HashReference,
        /// The name of the table for which metadata retrieval failed
        table: TableName,
        #[source]
        source: amp_data_store::GetTableActiveRevisionError,
    },

    /// Table has not been synced and no physical location exists.
    ///
    /// This occurs when attempting to load a physical catalog for a table that
    /// has been defined but has not yet been dumped/synced to storage. The table
    /// exists in the dataset definition but has no physical parquet files.
    #[error("Table {dataset}.{table} has not been synced")]
    TableNotSynced {
        /// The hash reference of the dataset containing the table
        dataset: HashReference,
        /// The name of the table that has not been synced
        table: TableName,
    },

    /// Failed to retrieve dataset metadata.
    ///
    /// This occurs when retrieving the dataset to extract start_block fails.
    #[error("Failed to retrieve dataset {dataset}")]
    DatasetRetrieval {
        /// The hash reference of the dataset
        dataset: HashReference,
        #[source]
        source: crate::BoxError,
    },
}

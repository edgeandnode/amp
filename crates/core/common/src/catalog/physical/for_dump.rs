//! Derived dataset physical catalog construction.
//!
//! This module provides physical catalog creation for derived dataset execution.
//! It accepts a pre-created logical catalog and adds physical parquet locations.

use amp_data_store::DataStore;
use datasets_common::{hash_reference::HashReference, table_name::TableName};

use crate::catalog::{
    dataset_access::DatasetAccess,
    logical::LogicalCatalog,
    physical::{Catalog, PhysicalTable},
};

/// Creates a full catalog with physical data access for derived dataset dumps.
///
/// This function builds a complete catalog by adding physical parquet file locations
/// to an existing logical catalog.
///
/// ## Parameters
///
/// - `dataset_store`: Used to retrieve dataset metadata including start_block
/// - `data_store`: Used to query metadata database for physical parquet locations
/// - `logical`: Pre-created logical catalog containing table schemas and UDFs
///
/// ## Where Used
///
/// Called exclusively by `dump_derived_dataset` in `crates/core/dump/src/derived_dataset.rs`
/// during the dump execution phase (NOT during validation).
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
        let table_name = table.name();

        let revision = data_store
            .get_table_active_revision(dataset_ref, table_name)
            .await
            .map_err(|err| CreateCatalogError::PhysicalTableRetrieval {
                dataset: dataset_ref.clone(),
                table: table_name.clone(),
                source: err,
            })?
            .ok_or(CreateCatalogError::TableNotSynced {
                dataset: dataset_ref.clone(),
                table: table_name.clone(),
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
        physical_tables.push(physical_table.into());
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

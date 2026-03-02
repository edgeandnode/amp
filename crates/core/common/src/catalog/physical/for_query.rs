//! Arrow Flight physical catalog construction.
//!
//! This module provides physical catalog creation for Arrow Flight query execution.
//! It resolves tables directly from parsed SQL table references, bypassing the
//! logical catalog.

use std::collections::BTreeSet;

use amp_data_store::DataStore;
use datasets_common::{
    hash_reference::HashReference, partial_reference::PartialReference, reference::Reference,
    table_name::TableName,
};

use super::catalog::{Catalog, CatalogTable};
use crate::{
    dataset_store::{DatasetStore, GetDatasetError, ResolveRevisionError},
    physical_table::table::PhysicalTable,
    sql::TableReference,
};

/// Creates a full catalog with physical data access for SQL query execution.
///
/// This function resolves tables directly from parsed SQL table references,
/// converting partial references to hash references, loading dataset metadata,
/// and building physical table entries for query execution.
///
/// ## Parameters
///
/// - `dataset_store`: Used to resolve references and retrieve dataset metadata
/// - `data_store`: Used to query metadata database for physical parquet locations
/// - `table_refs`: Parsed SQL table references with partial dataset references
pub async fn create(
    dataset_store: &DatasetStore,
    data_store: &DataStore,
    table_refs: Vec<TableReference<PartialReference>>,
) -> Result<Catalog, CreateCatalogError> {
    let mut entries = Vec::new();
    let mut seen = BTreeSet::new();

    for table_ref in &table_refs {
        let TableReference::Partial { schema, table } = table_ref else {
            continue;
        };

        let reference: Reference = PartialReference::clone(schema).into();

        // Resolve to hash reference
        let hash_ref = dataset_store
            .resolve_revision(&reference)
            .await
            .map_err(|source| CreateCatalogError::ResolveRevision {
                reference: reference.clone(),
                source,
            })?
            .ok_or_else(|| CreateCatalogError::DatasetNotFound {
                reference: reference.clone(),
            })?;

        // Deduplicate by (schema_name, hash, table_name) — different SQL aliases
        // for the same dataset+table need separate physical entries because
        // attach_to matches by sql_schema_name.
        let sql_schema_name = schema.to_string();
        let dedup_key = (sql_schema_name.clone(), hash_ref.clone(), table.clone());
        if !seen.insert(dedup_key) {
            continue;
        }

        // Load dataset
        let dataset = dataset_store
            .get_dataset(&hash_ref)
            .await
            .map_err(|source| CreateCatalogError::DatasetRetrieval {
                dataset: hash_ref.clone(),
                source,
            })?;

        // Find the table in the dataset
        let dataset_table = dataset
            .tables()
            .iter()
            .find(|t| t.name() == table.as_ref())
            .ok_or_else(|| CreateCatalogError::DatasetTableNotFound {
                dataset: hash_ref.clone(),
                table: (**table).clone(),
            })?
            .clone();

        // Get physical revision
        let revision = data_store
            .get_table_active_revision(&hash_ref, table)
            .await
            .map_err(|source| CreateCatalogError::PhysicalTableRetrieval {
                dataset: hash_ref.clone(),
                table: (**table).clone(),
                source,
            })?
            .ok_or_else(|| CreateCatalogError::TableNotSynced {
                dataset: hash_ref.clone(),
                table: (**table).clone(),
            })?;

        let physical_table = PhysicalTable::from_revision(
            data_store.clone(),
            hash_ref,
            dataset.start_block(),
            dataset_table,
            revision,
        );
        entries.push(CatalogTable::new(physical_table.into(), sql_schema_name));
    }

    Ok(Catalog::new(vec![], vec![], entries, Default::default()))
}

/// Errors that can occur when creating a physical catalog.
///
/// Returned by [`create`] when catalog creation fails.
#[derive(Debug, thiserror::Error)]
pub enum CreateCatalogError {
    /// Failed to resolve a dataset reference to a hash reference.
    #[error("Failed to resolve dataset reference {reference}")]
    ResolveRevision {
        /// The reference that failed to resolve
        reference: Reference,
        #[source]
        source: ResolveRevisionError,
    },

    /// Dataset reference resolved but no dataset was found.
    #[error("Dataset not found: {reference}")]
    DatasetNotFound {
        /// The reference that was not found
        reference: Reference,
    },

    /// Table not found in dataset definition.
    #[error("Table {table} not found in dataset {dataset}")]
    DatasetTableNotFound {
        /// The hash reference of the dataset
        dataset: HashReference,
        /// The table name that was not found
        table: TableName,
    },

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
        source: GetDatasetError,
    },
}

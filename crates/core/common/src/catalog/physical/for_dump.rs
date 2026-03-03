//! Derived dataset physical catalog construction.
//!
//! This module provides physical catalog creation for derived dataset execution.
//! It resolves dependency tables from manifest deps and SQL table references,
//! then adds physical parquet locations.

use std::collections::{BTreeMap, btree_map::Entry};

use amp_data_store::DataStore;
use datafusion::logical_expr::ScalarUDF;
use datasets_common::{hash::Hash, hash_reference::HashReference, table_name::TableName};
use datasets_derived::deps::DepAlias;

use super::catalog::{Catalog, CatalogTable};
use crate::{
    catalog::logical::LogicalTable,
    dataset_store::{DatasetStore, GetDatasetError},
    physical_table::table::PhysicalTable,
    sql::TableReference,
};

/// Creates a full catalog with physical data access for derived dataset dumps.
///
/// This function resolves dependency tables from manifest deps and SQL table references,
/// loads dataset metadata, builds physical table entries, and constructs the catalog.
///
/// ## Parameters
///
/// - `dataset_store`: Used to retrieve dataset metadata including start_block
/// - `data_store`: Used to query metadata database for physical parquet locations
/// - `manifest_deps`: Dependency alias → hash reference mappings from the manifest
/// - `table_refs`: Parsed SQL table references with dep alias schemas
/// - `udfs`: Pre-resolved self-ref UDFs (from logical catalog)
pub async fn create(
    dataset_store: &DatasetStore,
    data_store: &DataStore,
    manifest_deps: &BTreeMap<DepAlias, HashReference>,
    table_refs: Vec<TableReference<DepAlias>>,
    udfs: Vec<ScalarUDF>,
) -> Result<Catalog, CreateCatalogError> {
    // Resolve table references to LogicalTable instances
    let mut tables_by_hash: BTreeMap<Hash, BTreeMap<TableReference<DepAlias>, LogicalTable>> =
        Default::default();

    for table_ref in &table_refs {
        match table_ref {
            TableReference::Bare { .. } => {
                return Err(CreateCatalogError::UnqualifiedTable {
                    table_ref: table_ref.to_string(),
                });
            }
            TableReference::Partial { schema, table } => {
                let dataset_ref = manifest_deps.get(schema.as_ref()).ok_or_else(|| {
                    CreateCatalogError::DependencyAliasNotFound {
                        alias: schema.as_ref().clone(),
                    }
                })?;

                let Entry::Vacant(entry) = tables_by_hash
                    .entry(dataset_ref.hash().clone())
                    .or_default()
                    .entry(table_ref.clone())
                else {
                    continue;
                };

                let dataset = dataset_store
                    .get_dataset(dataset_ref)
                    .await
                    .map_err(|err| CreateCatalogError::GetDataset {
                        reference: dataset_ref.clone(),
                        source: err,
                    })?;

                let dataset_table = dataset
                    .tables()
                    .iter()
                    .find(|t| t.name() == table)
                    .ok_or_else(|| CreateCatalogError::TableNotFoundInDataset {
                        table_name: table.as_ref().clone(),
                        reference: dataset_ref.clone(),
                    })?;

                let resolved_table = LogicalTable::new(
                    schema.to_string(),
                    dataset_ref.clone(),
                    dataset_table.clone(),
                );

                entry.insert(resolved_table);
            }
        }
    }

    let logical_tables: Vec<LogicalTable> = tables_by_hash
        .into_values()
        .flat_map(|map| map.into_values())
        .collect();

    // Build physical catalog entries from resolved logical tables
    let mut entries = Vec::new();
    for table in &logical_tables {
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

        let dataset = dataset_store
            .get_dataset(dataset_ref)
            .await
            .map_err(|source| CreateCatalogError::DatasetRetrieval {
                dataset: dataset_ref.clone(),
                source,
            })?;

        let sql_schema_name = table.sql_schema_name().to_string();
        let physical_table = PhysicalTable::from_revision(
            data_store.clone(),
            table.dataset_reference().clone(),
            dataset.start_block(),
            table.table().clone(),
            revision,
        );
        entries.push(CatalogTable::new(physical_table.into(), sql_schema_name));
    }

    // Build dep_aliases map
    let dep_aliases: BTreeMap<String, HashReference> = manifest_deps
        .iter()
        .map(|(alias, hash_ref)| (alias.to_string(), hash_ref.clone()))
        .collect();

    Ok(Catalog::new(logical_tables, udfs, entries, dep_aliases))
}

/// Errors that can occur when creating a physical catalog.
///
/// Returned by [`create`] when catalog creation fails.
#[derive(Debug, thiserror::Error)]
pub enum CreateCatalogError {
    /// Table is not qualified with a schema/dataset name.
    #[error("Unqualified table '{table_ref}', all tables must be qualified with a dataset")]
    UnqualifiedTable {
        /// The unqualified table reference string
        table_ref: String,
    },

    /// Dependency alias not found when processing table reference.
    #[error(
        "Dependency alias '{alias}' referenced in table reference but not provided in dependencies"
    )]
    DependencyAliasNotFound {
        /// The dependency alias that was not found in the dependencies map
        alias: DepAlias,
    },

    /// Failed to retrieve dataset from store when loading dataset for table reference.
    #[error("Failed to retrieve dataset '{reference}' for table reference")]
    GetDataset {
        /// The hash reference of the dataset that failed to load
        reference: HashReference,
        #[source]
        source: GetDatasetError,
    },

    /// Table not found in dataset.
    #[error("Table '{table_name}' not found in dataset '{reference}'")]
    TableNotFoundInDataset {
        /// The name of the table that was not found
        table_name: TableName,
        /// The hash reference of the dataset that was searched
        reference: HashReference,
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

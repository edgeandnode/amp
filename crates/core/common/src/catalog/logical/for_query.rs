//! Arrow Flight logical catalog construction with dynamic resolution.
//!
//! This module creates LogicalCatalog for Arrow Flight SQL query planning.
//! Uses dynamic resolution (PartialReference) supporting version tags and "latest".

use std::collections::{BTreeMap, btree_map::Entry};

use datafusion::logical_expr::ScalarUDF;
use datasets_common::{
    func_name::ETH_CALL_FUNCTION_NAME, hash::Hash, hash_reference::HashReference,
    partial_reference::PartialReference, reference::Reference, table_name::TableName,
};
use js_runtime::isolate_pool::IsolatePool;

use crate::{
    BoxError, ResolvedTable,
    catalog::{dataset_access::DatasetAccess, logical::LogicalCatalog},
    sql::{FunctionReference, TableReference},
};

/// Resolved SQL references tuple (table refs, function refs) using partial references.
pub type ResolvedReferences = (
    Vec<TableReference<PartialReference>>,
    Vec<FunctionReference<PartialReference>>,
);

/// Creates a logical catalog for SQL query planning without physical data access.
///
/// This function builds a logical catalog with schemas only, enabling query plan generation
/// and schema inference without accessing physical parquet files.
///
/// ## Where Used
///
/// This function is used exclusively in the **Query Execution Path** for the planning phase:
///
/// - **Arrow Flight GetFlightInfo** (`crates/services/server/src/flight.rs`):
///   - Called to generate query plan and return schema to clients
///   - Fast response without accessing physical data files
///   - Precedes actual query execution which uses `catalog_for_sql`
///
/// ## Implementation
///
/// The function analyzes the SQL query to:
/// 1. Extract table references and function names from the query
/// 2. Resolve dataset names to hashes via the dataset store
/// 3. Build logical catalog with schemas and UDFs
/// 4. Return logical catalog for use with `PlanningContext::new()`
///
/// Unlike `catalog_for_sql`, this does not query the metadata database for physical
/// parquet locations, making it faster for planning-only operations.
pub async fn create(
    dataset_store: &impl DatasetAccess,
    isolate_pool: &IsolatePool,
    refs: ResolvedReferences,
) -> Result<LogicalCatalog, CreateCatalogError> {
    let (table_refs, func_refs) = refs;

    // Resolve logical catalog using shared helpers
    let tables = resolve_tables(dataset_store, table_refs)
        .await
        .map_err(CreateCatalogError::ResolveTables)?;
    let udfs = resolve_udfs(dataset_store, isolate_pool, func_refs)
        .await
        .map_err(CreateCatalogError::ResolveUdfs)?;

    Ok(LogicalCatalog { tables, udfs })
}

/// Resolves table references to ResolvedTable instances using dynamic resolution.
///
/// Processes each table reference, resolves the dataset reference to a hash,
/// loads the dataset, finds the table, and creates a ResolvedTable for catalog construction.
async fn resolve_tables(
    dataset_store: &impl DatasetAccess,
    refs: impl IntoIterator<Item = TableReference<PartialReference>>,
) -> Result<Vec<ResolvedTable>, ResolveTablesError> {
    // Use hash-based map to deduplicate datasets and collect resolved tables
    // Inner map: table_ref -> ResolvedTable (deduplicates table references)
    let mut tables: BTreeMap<Hash, BTreeMap<TableReference<PartialReference>, ResolvedTable>> =
        BTreeMap::new();

    for table_ref in refs {
        match &table_ref {
            TableReference::Bare { .. } => {
                return Err(ResolveTablesError::UnqualifiedTable {
                    table_ref: table_ref.to_string(),
                });
            }
            TableReference::Partial { schema, table } => {
                // Schema is already parsed as PartialReference, convert to Reference
                let reference: Reference = schema.as_ref().clone().into();

                // Resolve reference to hash reference
                let dataset_ref = dataset_store
                    .resolve_revision(&reference)
                    .await
                    .map_err(|err| ResolveTablesError::ResolveDatasetReference {
                        reference: reference.clone(),
                        source: err,
                    })?
                    .ok_or_else(|| ResolveTablesError::ResolveDatasetReference {
                        reference: reference.clone(),
                        source: format!("Dataset '{}' not found", reference).into(),
                    })?;

                // Skip if table reference is already resolved (optimization to avoid redundant dataset loading)
                let Entry::Vacant(entry) = tables
                    .entry(dataset_ref.hash().clone())
                    .or_default()
                    .entry(table_ref.clone())
                else {
                    continue;
                };

                // Load dataset by hash reference (cached by dataset_store)
                let dataset = dataset_store
                    .get_dataset(&dataset_ref)
                    .await
                    .map_err(|err| ResolveTablesError::LoadDataset {
                        reference: dataset_ref.clone(),
                        source: err,
                    })?;

                // Find table in dataset
                let dataset_table = dataset
                    .tables
                    .iter()
                    .find(|t| t.name() == table)
                    .ok_or_else(|| ResolveTablesError::TableNotFoundInDataset {
                        table_name: table.as_ref().clone(),
                        reference: dataset_ref.clone(),
                    })?;

                // Create ResolvedTable
                let resolved_table = ResolvedTable::new(
                    dataset_table.clone(),
                    schema.to_string(),
                    dataset_ref.clone(),
                    dataset.start_block,
                );

                // Insert into vacant entry
                entry.insert(resolved_table);
            }
        }
    }

    // Flatten to Vec<ResolvedTable>
    Ok(tables
        .into_values()
        .flat_map(|map| map.into_values())
        .collect())
}

/// Resolves function references to ScalarUDF instances using dynamic resolution.
///
/// Processes each function reference, resolves the dataset reference,
/// loads the dataset, and retrieves or creates the UDF.
async fn resolve_udfs(
    dataset_store: &impl DatasetAccess,
    isolate_pool: &IsolatePool,
    refs: impl IntoIterator<Item = FunctionReference<PartialReference>>,
) -> Result<Vec<ScalarUDF>, ResolveUdfsError> {
    // Track UDFs from external dependencies - outer key: dataset hash, inner key: function reference
    // Inner map ensures deduplication: multiple function references to the same UDF share one instance
    let mut udfs: BTreeMap<Hash, BTreeMap<FunctionReference<PartialReference>, ScalarUDF>> =
        BTreeMap::new();

    for func_ref in refs {
        match &func_ref {
            // Skip bare functions - they are assumed to be built-in functions (Amp or DataFusion)
            FunctionReference::Bare { .. } => continue,
            FunctionReference::Qualified { schema, function } => {
                // Schema is already parsed as PartialReference, convert to Reference
                let reference: Reference = schema.as_ref().clone().into();

                // Resolve reference to hash reference
                let dataset_ref = dataset_store
                    .resolve_revision(&reference)
                    .await
                    .map_err(|err| ResolveUdfsError::ResolveDatasetReference {
                        reference: reference.clone(),
                        source: err,
                    })?
                    .ok_or_else(|| ResolveUdfsError::ResolveDatasetReference {
                        reference: reference.clone(),
                        source: format!("Dataset '{}' not found", reference).into(),
                    })?;

                // Check vacancy BEFORE loading dataset
                let Entry::Vacant(entry) = udfs
                    .entry(dataset_ref.hash().clone())
                    .or_default()
                    .entry(func_ref.clone())
                else {
                    continue;
                };

                // Only load dataset if UDF not already resolved
                let dataset = dataset_store
                    .get_dataset(&dataset_ref)
                    .await
                    .map_err(|err| ResolveUdfsError::LoadDataset {
                        reference: dataset_ref.clone(),
                        source: err,
                    })?;

                // Get the UDF for this function reference
                let udf = if function.as_ref() == ETH_CALL_FUNCTION_NAME {
                    dataset_store
                        .eth_call_for_dataset(&schema.to_string(), &dataset)
                        .await
                        .map_err(|err| ResolveUdfsError::EthCallUdfCreation {
                            reference: dataset_ref.clone(),
                            source: err,
                        })?
                        .ok_or_else(|| ResolveUdfsError::EthCallNotAvailable {
                            reference: dataset_ref.clone(),
                        })?
                } else {
                    dataset
                        .function_by_name(schema.to_string(), function, isolate_pool.clone())
                        .ok_or_else(|| ResolveUdfsError::FunctionNotFoundInDataset {
                            function_name: func_ref.to_string(),
                            reference: dataset_ref,
                        })?
                };

                entry.insert(udf);
            }
        }
    }

    // Flatten to Vec<ScalarUDF>
    Ok(udfs
        .into_values()
        .flat_map(|map| map.into_values())
        .collect())
}

/// Errors specific to create operations
///
/// This error type is used by `create()` to create
/// a logical catalog for Arrow Flight query planning (GetFlightInfo).
#[derive(Debug, thiserror::Error)]
pub enum CreateCatalogError {
    /// Failed to resolve table references to ResolvedTable instances.
    #[error(transparent)]
    ResolveTables(ResolveTablesError),

    /// Failed to resolve function references to UDF instances.
    #[error(transparent)]
    ResolveUdfs(ResolveUdfsError),
}

/// Errors that can occur when resolving table references.
#[derive(Debug, thiserror::Error)]
pub enum ResolveTablesError {
    /// Table is not qualified with a schema/dataset name.
    ///
    /// All tables must be qualified with a dataset reference in the schema portion.
    /// Unqualified tables (e.g., just `table_name`) are not allowed.
    #[error("Unqualified table '{table_ref}', all tables must be qualified with a dataset")]
    UnqualifiedTable {
        /// The unqualified table reference string
        table_ref: String,
    },

    /// Failed to resolve dataset reference to a hash reference.
    ///
    /// This occurs when the dataset store cannot resolve a reference to its
    /// corresponding content hash. Common causes include:
    /// - Dataset does not exist in the store
    /// - Version tag not found
    /// - Storage backend errors
    /// - Invalid reference format
    /// - Database connection issues
    #[error("Failed to resolve dataset reference '{reference}'")]
    ResolveDatasetReference {
        /// The dataset reference that failed to resolve
        reference: Reference,
        #[source]
        source: BoxError,
    },

    /// Failed to load dataset from the dataset store.
    ///
    /// This occurs when loading a dataset definition fails. Common causes include:
    /// - Dataset does not exist in the store
    /// - Dataset manifest is invalid or corrupted
    /// - Unsupported dataset kind
    /// - Storage backend errors when reading the dataset
    /// - Manifest file not found in object store
    #[error("Failed to load dataset '{reference}'")]
    LoadDataset {
        /// The hash reference of the dataset that failed to load
        reference: HashReference,
        #[source]
        source: BoxError,
    },

    /// Table not found in dataset.
    ///
    /// This occurs when the table name is referenced in the SQL query but the
    /// dataset does not contain a table with that name.
    #[error("Table '{table_name}' not found in dataset '{reference}'")]
    TableNotFoundInDataset {
        /// The name of the table that was not found
        table_name: TableName,
        /// The hash reference of the dataset that was searched
        reference: HashReference,
    },
}

/// Errors that can occur when resolving UDF references.
#[derive(Debug, thiserror::Error)]
pub enum ResolveUdfsError {
    /// Failed to resolve dataset reference to a hash reference.
    ///
    /// This occurs when the dataset store cannot resolve a reference to its
    /// corresponding content hash. Common causes include:
    /// - Dataset does not exist in the store
    /// - Version tag not found
    /// - Storage backend errors
    /// - Invalid reference format
    /// - Database connection issues
    #[error("Failed to resolve dataset reference '{reference}'")]
    ResolveDatasetReference {
        /// The dataset reference that failed to resolve
        reference: Reference,
        #[source]
        source: BoxError,
    },

    /// Failed to load dataset from the dataset store.
    ///
    /// This occurs when loading a dataset definition fails. Common causes include:
    /// - Dataset does not exist in the store
    /// - Dataset manifest is invalid or corrupted
    /// - Unsupported dataset kind
    /// - Storage backend errors when reading the dataset
    /// - Manifest file not found in object store
    #[error("Failed to load dataset '{reference}'")]
    LoadDataset {
        /// The hash reference of the dataset that failed to load
        reference: HashReference,
        #[source]
        source: BoxError,
    },

    /// Failed to create ETH call UDF for dataset referenced in function name.
    ///
    /// This occurs when creating the eth_call user-defined function for a function fails:
    /// - Invalid provider configuration for the dataset
    /// - Provider connection issues
    /// - Dataset is not an EVM RPC dataset but eth_call was requested
    #[error("Failed to create ETH call UDF for dataset '{reference}'")]
    EthCallUdfCreation {
        /// The hash reference of the dataset for which eth_call UDF creation failed
        reference: HashReference,
        #[source]
        source: BoxError,
    },

    /// eth_call function not available for dataset.
    ///
    /// This occurs when the eth_call function is referenced in SQL but the
    /// dataset does not support eth_call (not an EVM RPC dataset or no provider configured).
    #[error("Function 'eth_call' not available for dataset '{reference}'")]
    EthCallNotAvailable {
        /// The hash reference of the dataset that does not support eth_call
        reference: HashReference,
    },

    /// Function not found in dataset.
    ///
    /// This occurs when a function is referenced in the SQL query but the
    /// dataset does not contain a function with that name.
    #[error("Function '{function_name}' not found in dataset '{reference}'")]
    FunctionNotFoundInDataset {
        /// The name of the function that was not found
        function_name: String,
        /// The hash reference of the dataset that was searched
        reference: HashReference,
    },
}

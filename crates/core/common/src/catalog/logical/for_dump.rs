//! Derived dataset logical catalog construction with pre-resolved dependencies.
//!
//! This module creates LogicalCatalog for derived dataset SQL validation.
//! Uses static dependency resolution (DepAlias → Hash mappings) for deterministic execution.

use std::{
    collections::{BTreeMap, btree_map::Entry},
    sync::Arc,
};

use datafusion::logical_expr::{ScalarUDF, async_udf::AsyncScalarUDF};
use datasets_common::{
    deps::alias::{DepAlias, DepAliasOrSelfRef},
    func_name::{ETH_CALL_FUNCTION_NAME, FuncName},
    hash::Hash,
    hash_reference::HashReference,
    manifest::Function,
    table_name::TableName,
};
use js_runtime::isolate_pool::IsolatePool;

use crate::{
    BoxError, ResolvedTable,
    catalog::{dataset_access::DatasetAccess, logical::LogicalCatalog},
    js_udf::JsUdf,
    sql::{FunctionReference, TableReference},
};

/// Resolved SQL references tuple (table refs, function refs) for derived dataset execution.
pub type ResolvedReferences = (
    Vec<TableReference<DepAlias>>,
    Vec<FunctionReference<DepAliasOrSelfRef>>,
);

/// Creates a logical catalog for derived dataset SQL validation without physical data access.
///
/// This function builds a logical catalog with schemas only, enabling query plan generation
/// and schema inference without accessing physical parquet files.
///
/// ## Where Used
///
/// This function is used during derived dataset dump execution when only logical validation
/// is needed (such as during query planning phases).
///
/// ## Implementation
///
/// The function:
/// 1. Destructures the references tuple into table and function references
/// 2. Resolves table references to ResolvedTable instances using pre-resolved dependencies
/// 3. Resolves function references to ScalarUDF instances
/// 4. Returns a LogicalCatalog containing tables and UDFs
pub async fn create(
    dataset_store: &impl DatasetAccess,
    isolate_pool: &IsolatePool,
    manifest_deps: &BTreeMap<DepAlias, HashReference>,
    manifest_udfs: &BTreeMap<FuncName, Function>,
    refs: ResolvedReferences,
) -> Result<LogicalCatalog, CreateCatalogError> {
    let (table_refs, func_refs) = refs;

    let tables = resolve_tables(dataset_store, manifest_deps, table_refs)
        .await
        .map_err(CreateCatalogError::ResolveTables)?;
    let udfs = resolve_udfs(
        dataset_store,
        isolate_pool,
        manifest_deps,
        manifest_udfs,
        func_refs,
    )
    .await
    .map_err(CreateCatalogError::ResolveUdfs)?;

    Ok(LogicalCatalog { tables, udfs })
}

/// Resolves table references to ResolvedTable instances using pre-resolved dependencies.
///
/// Processes each table reference, looks up the dataset by hash, finds the table
/// within the dataset, and creates a ResolvedTable for catalog construction.
async fn resolve_tables(
    dataset_store: &impl DatasetAccess,
    manifest_deps: &BTreeMap<DepAlias, HashReference>,
    refs: impl IntoIterator<Item = TableReference<DepAlias>>,
) -> Result<Vec<ResolvedTable>, ResolveTablesError> {
    // Use hash-based map to deduplicate datasets and collect resolved tables
    // Inner map: table_ref -> ResolvedTable (deduplicates table references)
    let mut tables: BTreeMap<Hash, BTreeMap<TableReference<DepAlias>, ResolvedTable>> =
        BTreeMap::new();

    for table_ref in refs {
        match &table_ref {
            TableReference::Bare { .. } => {
                return Err(ResolveTablesError::UnqualifiedTable {
                    table_ref: table_ref.to_string(),
                });
            }
            TableReference::Partial { schema, table } => {
                // Schema is already parsed as DepAlias, lookup in dependencies map
                let dataset_ref = manifest_deps.get(schema.as_ref()).ok_or_else(|| {
                    ResolveTablesError::DependencyAliasNotFound {
                        alias: schema.as_ref().clone(),
                    }
                })?;

                // Skip if table reference is already resolved (optimization to avoid redundant dataset loading)
                let Entry::Vacant(entry) = tables
                    .entry(dataset_ref.hash().clone())
                    .or_default()
                    .entry(table_ref.clone())
                else {
                    continue;
                };

                // Load dataset by hash (cached by dataset_store)
                let dataset = dataset_store
                    .get_dataset(dataset_ref)
                    .await
                    .map_err(|err| ResolveTablesError::GetDataset {
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

/// Resolves function references to ScalarUDF instances using pre-resolved dependencies.
///
/// Processes each function reference:
/// - For external dependencies (dep.function): loads dataset and retrieves UDF
/// - For self-references (self.function): creates JsUdf from the manifest's function definition
/// - Skips bare functions (built-in DataFusion/Amp functions)
async fn resolve_udfs(
    dataset_store: &impl DatasetAccess,
    isolate_pool: &IsolatePool,
    manifest_deps: &BTreeMap<DepAlias, HashReference>,
    manifest_udfs: &BTreeMap<FuncName, Function>,
    refs: impl IntoIterator<Item = FunctionReference<DepAliasOrSelfRef>>,
) -> Result<Vec<ScalarUDF>, ResolveUdfsError> {
    // Track UDFs from external dependencies - outer key: dataset hash, inner key: function reference
    // Inner map ensures deduplication: multiple function references to the same UDF share one instance
    let mut udfs: BTreeMap<Hash, BTreeMap<FunctionReference<DepAliasOrSelfRef>, ScalarUDF>> =
        BTreeMap::new();
    // Track UDFs defined in this manifest (bare functions and self-references) - separate from dependency functions
    // Ensures deduplication: multiple references to the same function share one instance
    let mut self_udfs: BTreeMap<FunctionReference<DepAliasOrSelfRef>, ScalarUDF> = BTreeMap::new();

    for func_ref in refs {
        match &func_ref {
            // Skip bare functions - they are assumed to be built-in functions (Amp or DataFusion)
            FunctionReference::Bare { function: _ } => continue,
            FunctionReference::Qualified { schema, function } => {
                // Match on schema type: DepAlias (external dependency) or SelfRef (same-dataset function)
                match schema.as_ref() {
                    DepAliasOrSelfRef::DepAlias(dep_alias) => {
                        // External dependency reference - lookup in dependencies map
                        let dataset_ref = manifest_deps.get(dep_alias).ok_or_else(|| {
                            ResolveUdfsError::DependencyAliasNotFound {
                                alias: dep_alias.clone(),
                            }
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
                        let dataset =
                            dataset_store
                                .get_dataset(dataset_ref)
                                .await
                                .map_err(|err| ResolveUdfsError::GetDataset {
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
                                .function_by_name(
                                    schema.to_string(),
                                    function,
                                    isolate_pool.clone(),
                                )
                                .ok_or_else(|| ResolveUdfsError::FunctionNotFoundInDataset {
                                    function_name: (**function).clone(),
                                    reference: dataset_ref.clone(),
                                })?
                        };

                        entry.insert(udf);
                    }
                    DepAliasOrSelfRef::SelfRef => {
                        // Same-dataset function reference (self.function_name)
                        // Look up function in the functions map (defined in this dataset)
                        let func_def = manifest_udfs.get(function).ok_or_else(|| {
                            ResolveUdfsError::SelfReferencedFunctionNotFound {
                                function_name: (**function).clone(),
                            }
                        })?;

                        // Skip if function reference is already resolved (optimization)
                        let Entry::Vacant(entry) = self_udfs.entry(func_ref.clone()) else {
                            continue;
                        };

                        // Create UDF from Function definition using JsUdf
                        // Use "self" as schema qualifier to preserve case sensitivity
                        let udf = AsyncScalarUDF::new(Arc::new(JsUdf::new(
                            isolate_pool.clone(),
                            Some(datasets_common::deps::alias::SELF_REF_KEYWORD.to_string()), // Schema = "self"
                            func_def.source.source.clone(),
                            func_def.source.filename.clone().into(),
                            Arc::from(function.as_ref().as_str()),
                            func_def
                                .input_types
                                .iter()
                                .map(|dt| dt.clone().into_arrow())
                                .collect(),
                            func_def.output_type.clone().into_arrow(),
                        )))
                        .into_scalar_udf();

                        entry.insert(udf);
                    }
                }
            }
        }
    }

    // Flatten and combine UDFs
    Ok(self_udfs
        .into_values()
        .chain(udfs.into_values().flat_map(|map| map.into_values()))
        .collect())
}

/// Errors specific to create operations.
///
/// This error type is used by `create()` to create
/// a logical catalog for derived dataset execution.
#[derive(Debug, thiserror::Error)]
pub enum CreateCatalogError {
    /// Failed to resolve table references to ResolvedTable instances.
    #[error(transparent)]
    ResolveTables(ResolveTablesError),

    /// Failed to resolve function references to UDF instances.
    #[error(transparent)]
    ResolveUdfs(ResolveUdfsError),
}

/// Errors that can occur when resolving table references with dependencies.
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

    /// Dependency alias not found when processing table reference.
    ///
    /// This occurs when a table reference uses an alias that was not provided
    /// in the dependencies map.
    #[error(
        "Dependency alias '{alias}' referenced in table reference but not provided in dependencies"
    )]
    DependencyAliasNotFound {
        /// The dependency alias that was not found in the dependencies map
        alias: DepAlias,
    },

    /// Failed to retrieve dataset from store when loading dataset for table reference.
    ///
    /// This occurs when loading a dataset definition fails:
    /// - Dataset not found in the store
    /// - Dataset manifest is invalid or corrupted
    /// - Unsupported dataset kind
    /// - Storage backend errors when reading the dataset
    #[error("Failed to retrieve dataset '{reference}' for table reference")]
    GetDataset {
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

/// Errors that can occur when resolving UDF references with dependencies.
#[derive(Debug, thiserror::Error)]
pub enum ResolveUdfsError {
    /// Dependency alias not found when processing function reference.
    ///
    /// This occurs when a function reference uses an alias that was not provided
    /// in the dependencies map.
    #[error(
        "Dependency alias '{alias}' referenced in function reference but not provided in dependencies"
    )]
    DependencyAliasNotFound {
        /// The dependency alias that was not found in the dependencies map
        alias: DepAlias,
    },

    /// Failed to retrieve dataset from store when loading dataset for function.
    ///
    /// This occurs when loading a dataset definition for a function fails:
    /// - Dataset not found in the store
    /// - Dataset manifest is invalid or corrupted
    /// - Unsupported dataset kind
    /// - Storage backend errors when reading the dataset
    #[error("Failed to retrieve dataset '{reference}' for function reference")]
    GetDataset {
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
    #[error("Failed to create ETH call UDF for dataset '{reference}' for function reference")]
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
        function_name: FuncName,
        /// The hash reference of the dataset that was searched
        reference: HashReference,
    },

    /// Self-referenced function not found in manifest's functions map.
    ///
    /// This occurs when a SQL query uses `self.function_name` syntax but the
    /// function is not defined in the manifest's `functions` section.
    #[error("Self-referenced function '{function_name}' not found in manifest functions")]
    SelfReferencedFunctionNotFound {
        /// The function name that was referenced but not defined
        function_name: FuncName,
    },
}

//! Schema inference catalog construction for derived dataset validation
//!
//! This module provides catalog creation for schema inference when validating
//! derived dataset manifests via the admin API.
//!
//! ## Key Functions
//!
//! - [`create`] - Creates a LogicalCatalog for SQL validation
//! - [`resolve_tables_with_deps`] - Resolves table references using pre-resolved dependencies
//! - [`resolve_udfs_with_deps`] - Resolves function references to ScalarUDF instances

use std::{
    collections::{BTreeMap, btree_map::Entry},
    sync::Arc,
};

use datafusion::logical_expr::{ScalarUDF, async_udf::AsyncScalarUDF};
use datasets_common::{
    func_name::{ETH_CALL_FUNCTION_NAME, FuncName},
    hash::Hash,
    hash_reference::HashReference,
    table_name::TableName,
};
use datasets_derived::{
    dataset::Dataset as DerivedDataset,
    deps::{DepAlias, DepAliasOrSelfRef, SELF_REF_KEYWORD},
    manifest::Function,
};
use js_runtime::{isolate_pool::IsolatePool, js_udf::JsUdf};

use crate::{
    BoxError,
    catalog::{
        dataset_access::DatasetAccess,
        logical::{LogicalCatalog, LogicalTable},
    },
    sql::{FunctionReference, TableReference},
};

/// Map of table names to (table references, function references)
pub type TableReferencesMap = BTreeMap<
    TableName,
    (
        Vec<TableReference<DepAlias>>,
        Vec<FunctionReference<DepAliasOrSelfRef>>,
    ),
>;

/// Creates a LogicalCatalog for SQL validation with pre-resolved dependencies.
///
/// This function is used during derived dataset manifest validation to create a logical
/// catalog that can validate SQL queries against specific dataset versions.
///
/// ## Process
///
/// 1. Flattens table references from the references map
/// 2. Resolves all table references to LogicalTable instances
/// 3. Flattens function references from the references map
/// 4. Resolves all function references to ScalarUDF instances
/// 5. Creates and returns a LogicalCatalog
///
/// ## Related Functions
///
/// - [`resolve_tables`] - Resolves table references to LogicalTable instances
/// - [`resolve_udfs`] - Resolves function references to UDFs
pub async fn create(
    dataset_store: &impl DatasetAccess,
    isolate_pool: IsolatePool,
    manifest_deps: BTreeMap<DepAlias, HashReference>,
    manifest_udfs: BTreeMap<FuncName, Function>,
    refs: TableReferencesMap,
) -> Result<LogicalCatalog, CreateLogicalCatalogError> {
    let table_refs: Vec<_> = refs
        .iter()
        .flat_map(|(name, (table_refs, _))| {
            table_refs.iter().map(move |table_ref| (name, table_ref))
        })
        .collect();

    let tables = resolve_tables(dataset_store, &manifest_deps, table_refs)
        .await
        .map_err(CreateLogicalCatalogError::ResolveTables)?;

    let func_refs: Vec<_> = refs
        .iter()
        .flat_map(|(name, (_, func_refs))| func_refs.iter().map(move |func_ref| (name, func_ref)))
        .collect();

    let udfs = resolve_udfs(
        dataset_store,
        isolate_pool,
        &manifest_deps,
        &manifest_udfs,
        func_refs,
    )
    .await
    .map_err(CreateLogicalCatalogError::ResolveUdfs)?;

    Ok(LogicalCatalog { tables, udfs })
}

#[derive(Debug, thiserror::Error)]
pub enum CreateLogicalCatalogError {
    /// Failed to resolve table references to LogicalTable instances
    #[error(transparent)]
    ResolveTables(ResolveTablesError),

    /// Failed to resolve function references to UDF instances
    #[error(transparent)]
    ResolveUdfs(ResolveUdfsError),
}

/// Resolves table references to LogicalTable instances using pre-resolved dependencies.
///
/// Processes each table reference across all tables, looks up datasets by hash, finds tables
/// within datasets, and creates LogicalTable instances for catalog construction.
async fn resolve_tables<'a>(
    dataset_store: &impl DatasetAccess,
    manifest_deps: &BTreeMap<DepAlias, HashReference>,
    refs: impl IntoIterator<Item = (&'a TableName, &'a TableReference<DepAlias>)> + 'a,
) -> Result<Vec<LogicalTable>, ResolveTablesError> {
    // Use hash-based map to deduplicate datasets across ALL tables
    // Inner map: table_ref -> LogicalTable (deduplicates table references)
    let mut tables: BTreeMap<Hash, BTreeMap<TableReference<DepAlias>, LogicalTable>> =
        BTreeMap::new();

    // Process all table references - fail fast on first error
    for (table_name, table_ref) in refs {
        match table_ref {
            TableReference::Bare { .. } => {
                return Err(ResolveTablesError::UnqualifiedTable {
                    table_name: table_name.clone(),
                    table_ref: table_ref.to_string(),
                });
            }
            TableReference::Partial { schema, table } => {
                // Schema is already parsed as DepAlias, lookup in dependencies map
                let dataset_ref = manifest_deps.get(schema.as_ref()).ok_or_else(|| {
                    ResolveTablesError::DependencyAliasNotFound {
                        table_name: table_name.clone(),
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
                        table_name: table_name.clone(),
                        reference: dataset_ref.clone(),
                        source: err,
                    })?;

                // Find table in dataset
                let dataset_table = dataset
                    .tables()
                    .iter()
                    .find(|t| t.name() == table)
                    .ok_or_else(|| ResolveTablesError::TableNotFoundInDataset {
                        table_name: table_name.clone(),
                        referenced_table_name: table.as_ref().clone(),
                        reference: dataset_ref.clone(),
                    })?;

                let resolved_table = LogicalTable::new(
                    schema.to_string(),
                    dataset_ref.clone(),
                    dataset_table.clone(),
                );

                // Insert into vacant entry
                entry.insert(resolved_table);
            }
        }
    }

    // Flatten to Vec<LogicalTable>
    Ok(tables
        .into_values()
        .flat_map(|map| map.into_values())
        .collect())
}

/// Resolves function references to ScalarUDF instances using pre-resolved dependencies.
///
/// Processes each function reference across all tables:
/// - For external dependencies (dep.function): loads dataset and retrieves UDF
/// - For self-references (self.function): creates JsUdf from the manifest's function definition
/// - Skips bare functions (built-in DataFusion/Amp functions)
async fn resolve_udfs<'a>(
    dataset_store: &impl DatasetAccess,
    isolate_pool: IsolatePool,
    manifest_deps: &BTreeMap<DepAlias, HashReference>,
    manifest_udfs: &BTreeMap<FuncName, Function>,
    refs: impl IntoIterator<Item = (&'a TableName, &'a FunctionReference<DepAliasOrSelfRef>)> + 'a,
) -> Result<Vec<ScalarUDF>, ResolveUdfsError> {
    // Track UDFs from external dependencies - outer key: dataset hash, inner key: function reference
    // Inner map ensures deduplication: multiple function references to the same UDF share one instance
    let mut udfs: BTreeMap<Hash, BTreeMap<FunctionReference<DepAliasOrSelfRef>, ScalarUDF>> =
        BTreeMap::new();
    // Track UDFs defined in this manifest (bare functions and self-references) - separate from dependency functions
    // Ensures deduplication: multiple references to the same function share one instance
    let mut self_udfs: BTreeMap<FunctionReference<DepAliasOrSelfRef>, ScalarUDF> = BTreeMap::new();

    // Process all function references - fail fast on first error
    for (table_name, func_ref) in refs {
        match func_ref {
            // Skip bare functions - they are assumed to be built-in functions (Amp or DataFusion)
            FunctionReference::Bare { function: _ } => {
                continue;
            }
            FunctionReference::Qualified { schema, function } => {
                // Match on schema type: DepAlias (external dependency) or SelfRef (same-dataset function)
                match schema.as_ref() {
                    DepAliasOrSelfRef::DepAlias(dep_alias) => {
                        // External dependency reference - lookup in dependencies map
                        let dataset_ref = manifest_deps.get(dep_alias).ok_or_else(|| {
                            ResolveUdfsError::DependencyAliasNotFound {
                                table_name: table_name.clone(),
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

                        // Only load dataset if UDF not already resolved (cached by dataset_store)
                        let dataset =
                            dataset_store
                                .get_dataset(dataset_ref)
                                .await
                                .map_err(|err| ResolveUdfsError::GetDataset {
                                    table_name: table_name.clone(),
                                    reference: dataset_ref.clone(),
                                    source: err,
                                })?;

                        // Get the UDF for this function reference
                        let udf = if function.as_ref() == ETH_CALL_FUNCTION_NAME {
                            dataset_store
                                .eth_call_for_dataset(&schema.to_string(), dataset.as_ref())
                                .await
                                .map_err(|err| ResolveUdfsError::EthCallUdfCreation {
                                    table_name: table_name.clone(),
                                    reference: dataset_ref.clone(),
                                    source: err,
                                })?
                                .ok_or_else(|| ResolveUdfsError::EthCallNotAvailable {
                                    table_name: table_name.clone(),
                                    reference: dataset_ref.clone(),
                                })?
                        } else {
                            dataset
                                .downcast_ref::<DerivedDataset>()
                                .and_then(|d| {
                                    d.function_by_name(
                                        schema.to_string(),
                                        function,
                                        IsolatePool::dummy(),
                                    )
                                })
                                .ok_or_else(|| ResolveUdfsError::FunctionNotFoundInDataset {
                                    table_name: table_name.clone(),
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
                                table_name: table_name.clone(),
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
                            Some(SELF_REF_KEYWORD.to_string()), // Schema = "self"
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

    // Flatten to Vec<ScalarUDF>
    Ok(self_udfs
        .into_values()
        .chain(udfs.into_values().flat_map(|map| map.into_values()))
        .collect())
}

#[derive(Debug, thiserror::Error)]
pub enum ResolveTablesError {
    /// Table is not qualified with a schema/dataset name
    #[error(
        "In table '{table_name}': Unqualified table '{table_ref}', all tables must be qualified with a dataset"
    )]
    UnqualifiedTable {
        /// The table being processed when the error occurred
        table_name: TableName,
        /// The unqualified table reference string
        table_ref: String,
    },

    /// Dependency alias not found when processing table reference
    #[error(
        "In table '{table_name}': Dependency alias '{alias}' referenced in table but not provided in dependencies"
    )]
    DependencyAliasNotFound {
        /// The table being processed when the error occurred
        table_name: TableName,
        /// The dependency alias that was not found in the dependencies map
        alias: DepAlias,
    },

    /// Failed to retrieve dataset from store when loading dataset for table reference
    #[error("In table '{table_name}': Failed to retrieve dataset '{reference}'")]
    GetDataset {
        /// The table being processed when the error occurred
        table_name: TableName,
        /// The hash reference of the dataset that failed to load
        reference: HashReference,
        #[source]
        source: BoxError,
    },

    /// Table not found in dataset
    #[error(
        "In table '{table_name}': Table '{referenced_table_name}' not found in dataset '{reference}'"
    )]
    TableNotFoundInDataset {
        /// The table being processed when the error occurred
        table_name: TableName,
        /// The name of the table that was not found in the dataset
        referenced_table_name: TableName,
        /// The hash reference of the dataset where the table was not found
        reference: HashReference,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum ResolveUdfsError {
    /// Dependency alias not found when processing function reference
    #[error(
        "In table '{table_name}': Dependency alias '{alias}' referenced in function but not provided in dependencies"
    )]
    DependencyAliasNotFound {
        /// The table being processed when the error occurred
        table_name: TableName,
        /// The dependency alias that was not found in the dependencies map
        alias: DepAlias,
    },

    /// Failed to retrieve dataset from store when loading dataset for function
    #[error("In table '{table_name}': Failed to retrieve dataset '{reference}' for function")]
    GetDataset {
        /// The table being processed when the error occurred
        table_name: TableName,
        /// The hash reference of the dataset that failed to load
        reference: HashReference,
        #[source]
        source: BoxError,
    },

    /// Failed to create ETH call UDF for dataset referenced in function name
    #[error(
        "In table '{table_name}': Failed to create ETH call UDF for dataset '{reference}' for function"
    )]
    EthCallUdfCreation {
        /// The table being processed when the error occurred
        table_name: TableName,
        /// The hash reference of the dataset for which the eth_call UDF creation failed
        reference: HashReference,
        #[source]
        source: BoxError,
    },

    /// eth_call function not available for dataset
    #[error("In table '{table_name}': Function 'eth_call' not available for dataset '{reference}'")]
    EthCallNotAvailable {
        /// The table being processed when the error occurred
        table_name: TableName,
        /// The hash reference of the dataset that does not support eth_call
        reference: HashReference,
    },

    /// Function not found in dataset
    #[error(
        "In table '{table_name}': Function '{function_name}' not found in dataset '{reference}'"
    )]
    FunctionNotFoundInDataset {
        /// The table being processed when the error occurred
        table_name: TableName,
        /// The name of the function that was not found
        function_name: FuncName,
        /// The hash reference of the dataset where the function was not found
        reference: HashReference,
    },

    /// Self-referenced function not found in manifest's functions map.
    #[error(
        "In table '{table_name}': Self-referenced function '{function_name}' not found in manifest functions"
    )]
    SelfReferencedFunctionNotFound {
        /// The table containing the SQL query with the invalid reference
        table_name: TableName,
        /// The function name that was referenced but not defined
        function_name: FuncName,
    },
}

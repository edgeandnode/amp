//! Catalog functions for derived datasets
//!
//! This module contains catalog creation functions that handle derived dataset dependencies.
//! These functions work with `DepAlias`, `DepReference`, and `FuncName` types that are specific
//! to derived datasets.
//!
//! ## SQL Catalog Functions
//!
//! SQL catalog functions provide catalog creation functions that work with pre-resolved
//! dataset dependencies (DepAlias → Hash mappings) for deterministic, reproducible
//! derived dataset execution.

use std::{
    collections::{BTreeMap, btree_map::Entry},
    sync::Arc,
};

use common::{
    BoxError, PlanningContext, ResolvedTable,
    catalog::{
        dataset_access::DatasetAccess,
        logical::LogicalCatalog,
        physical::{Catalog, PhysicalTable},
    },
    js_udf::JsUdf,
    query_context::QueryEnv,
    sql::{
        FunctionReference, ResolveFunctionReferencesError, ResolveTableReferencesError,
        TableReference, resolve_function_references, resolve_table_references,
    },
};
use datafusion::{
    logical_expr::{ScalarUDF, async_udf::AsyncScalarUDF},
    sql::parser::Statement,
};
use datasets_common::{
    deps::alias::{DepAlias, DepAliasError, DepAliasOrSelfRef, DepAliasOrSelfRefError},
    func_name::{ETH_CALL_FUNCTION_NAME, FuncName},
    hash::Hash,
    hash_reference::HashReference,
    table_name::TableName,
};
use js_runtime::isolate_pool::IsolatePool;
use metadata_db::MetadataDb;

use crate::manifest::Function;

pub async fn catalog_for_sql_with_deps(
    store: &impl DatasetAccess,
    metadata_db: &MetadataDb,
    query: &Statement,
    env: QueryEnv,
    dependencies: BTreeMap<DepAlias, HashReference>,
    functions: BTreeMap<FuncName, Function>,
) -> Result<Catalog, CatalogForSqlWithDepsError> {
    let table_refs = resolve_table_references::<DepAlias>(query)
        .map_err(CatalogForSqlWithDepsError::TableReferenceResolution)?;
    let func_refs = resolve_function_references::<DepAliasOrSelfRef>(query)
        .map_err(CatalogForSqlWithDepsError::FunctionReferenceResolution)?;

    get_physical_catalog_with_deps(
        store,
        metadata_db,
        table_refs,
        func_refs,
        &env,
        &dependencies,
        functions,
    )
    .await
    .map_err(CatalogForSqlWithDepsError::GetPhysicalCatalogWithDeps)
}

async fn get_physical_catalog_with_deps(
    store: &impl DatasetAccess,
    metadata_db: &MetadataDb,
    table_refs: impl IntoIterator<Item = TableReference<DepAlias>>,
    function_refs: impl IntoIterator<Item = FunctionReference<DepAliasOrSelfRef>>,
    env: &QueryEnv,
    dependencies: &BTreeMap<DepAlias, HashReference>,
    functions: BTreeMap<FuncName, Function>,
) -> Result<Catalog, GetPhysicalCatalogWithDepsError> {
    let logical_catalog = get_logical_catalog_with_deps_and_funcs(
        store,
        table_refs,
        function_refs,
        &env.isolate_pool,
        dependencies,
        functions,
    )
    .await
    .map_err(GetPhysicalCatalogWithDepsError::GetLogicalCatalogWithDepsAndFuncs)?;

    let mut tables = Vec::new();
    for table in &logical_catalog.tables {
        let physical_table = PhysicalTable::get_active(table, metadata_db.clone())
            .await
            .map_err(
                |err| GetPhysicalCatalogWithDepsError::PhysicalTableRetrieval {
                    table: table.to_string(),
                    source: err,
                },
            )?
            .ok_or(GetPhysicalCatalogWithDepsError::TableNotSynced {
                table: table.to_string(),
            })?;
        tables.push(physical_table.into());
    }
    Ok(Catalog::new(tables, logical_catalog))
}

/// Internal helper that builds a logical catalog from table references and function names.
///
/// This function resolves dataset references, loads dataset metadata, and creates UDFs
/// for the referenced datasets. It builds the logical layer of the catalog without
/// accessing physical data locations.
///
/// ## Where Used
///
/// Called internally by:
/// - `get_physical_catalog` (which is called by `catalog_for_sql`)
async fn get_logical_catalog_with_deps_and_funcs(
    store: &impl DatasetAccess,
    table_refs: impl IntoIterator<Item = TableReference<DepAlias>>,
    func_refs: impl IntoIterator<Item = FunctionReference<DepAliasOrSelfRef>>,
    isolate_pool: &IsolatePool,
    dependencies: &BTreeMap<DepAlias, HashReference>,
    functions: BTreeMap<FuncName, Function>,
) -> Result<LogicalCatalog, GetLogicalCatalogWithDepsAndFuncsError> {
    let table_refs = table_refs.into_iter().collect::<Vec<_>>();
    let func_refs = func_refs.into_iter().collect::<Vec<_>>();

    // Use hash-based map to deduplicate datasets and collect resolved tables
    // Inner map: table_ref -> ResolvedTable (deduplicates table references)
    let mut tables: BTreeMap<Hash, BTreeMap<TableReference, ResolvedTable>> = BTreeMap::new();
    // Track UDFs from external dependencies - outer key: dataset hash, inner key: function reference
    // Inner map ensures deduplication: multiple function references to the same UDF share one instance
    let mut udfs: BTreeMap<Hash, BTreeMap<FunctionReference, ScalarUDF>> = BTreeMap::new();
    // Track UDFs defined in this manifest (bare functions and self-references) - separate from dependency functions
    // Ensures deduplication: multiple references to the same function share one instance
    let mut self_udfs: BTreeMap<FunctionReference<DepAliasOrSelfRef>, ScalarUDF> = BTreeMap::new();

    // Part 1: Process table references
    for table_ref in table_refs {
        match &table_ref {
            TableReference::Bare { .. } => {
                return Err(GetLogicalCatalogWithDepsAndFuncsError::UnqualifiedTable {
                    table_ref: table_ref.to_string(),
                });
            }
            TableReference::Partial { schema, table } => {
                // Schema is already parsed as DepAlias, lookup in dependencies map
                let hash_ref = dependencies.get(schema.as_ref()).ok_or_else(|| {
                    GetLogicalCatalogWithDepsAndFuncsError::DependencyAliasNotFoundForTableRef {
                        alias: schema.as_ref().clone(),
                    }
                })?;

                // Convert table_ref to use String schema for internal data structures
                let table_ref_string =
                    TableReference::partial(schema.to_string(), table.as_ref().clone());

                // Skip if table reference is already resolved (optimization to avoid redundant dataset loading)
                let Entry::Vacant(entry) = tables
                    .entry(hash_ref.hash().clone())
                    .or_default()
                    .entry(table_ref_string.clone())
                else {
                    continue;
                };

                // Load dataset by hash (cached by store)
                let dataset = store
                    .get_dataset_by_hash(hash_ref.hash())
                    .await
                    .map_err(
                        |err| GetLogicalCatalogWithDepsAndFuncsError::GetDatasetForTableRef {
                            reference: hash_ref.clone(),
                            source: err,
                        },
                    )?
                    .ok_or_else(|| {
                        GetLogicalCatalogWithDepsAndFuncsError::DatasetNotFoundForTableRef {
                            reference: hash_ref.clone(),
                        }
                    })?;

                // Find table in dataset
                let dataset_table = dataset
                    .tables
                    .iter()
                    .find(|t| t.name() == table)
                    .ok_or_else(|| {
                        GetLogicalCatalogWithDepsAndFuncsError::TableNotFoundInDataset {
                            table_name: table.as_ref().clone(),
                            reference: hash_ref.clone(),
                        }
                    })?;

                // Create ResolvedTable with the converted string-based table reference
                let resolved_table = ResolvedTable::new(
                    table_ref_string.clone(),
                    dataset_table.clone(),
                    dataset.clone(),
                );

                // Insert into vacant entry
                entry.insert(resolved_table);
            }
        }
    }

    // Part 2: Process function references (load datasets for qualified UDFs, create UDFs for bare functions)
    for func_ref in func_refs {
        match &func_ref {
            // Skip bare functions - they are assumed to be built-in functions (Amp or DataFusion)
            FunctionReference::Bare { function: _ } => continue,
            FunctionReference::Qualified { schema, function } => {
                // Match on schema type: DepAlias (external dependency) or SelfRef (same-dataset function)
                match schema.as_ref() {
                    datasets_common::deps::alias::DepAliasOrSelfRef::DepAlias(dep_alias) => {
                        // External dependency reference - lookup in dependencies map
                        let hash_ref = dependencies.get(dep_alias).ok_or_else(|| {
                            GetLogicalCatalogWithDepsAndFuncsError::DependencyAliasNotFoundForFunctionRef {
                                alias: dep_alias.clone(),
                            }
                        })?;

                        // Load dataset by hash (cached by store)
                        let dataset = store
                            .get_dataset_by_hash(hash_ref.hash())
                            .await
                            .map_err(|err| {
                                GetLogicalCatalogWithDepsAndFuncsError::GetDatasetForFunction {
                                    reference: hash_ref.clone(),
                                    source: err,
                                }
                            })?
                            .ok_or_else(|| {
                                GetLogicalCatalogWithDepsAndFuncsError::DatasetNotFoundForFunction {
                                    reference: hash_ref.clone(),
                                }
                            })?;

                        // Convert func_ref to use String schema for internal data structures
                        let func_ref_string = FunctionReference::qualified(
                            schema.to_string(),
                            function.as_ref().clone(),
                        );

                        // Skip if function reference is already resolved (optimization to avoid redundant UDF creation)
                        let Entry::Vacant(entry) = udfs
                            .entry(hash_ref.hash().clone())
                            .or_default()
                            .entry(func_ref_string)
                        else {
                            continue;
                        };

                        // Get the UDF for this function reference
                        let udf = if function.as_ref() == ETH_CALL_FUNCTION_NAME {
                            store
                                .eth_call_for_dataset(&schema.to_string(), &dataset)
                                .await
                                .map_err(|err| {
                                    GetLogicalCatalogWithDepsAndFuncsError::EthCallUdfCreationForFunction {
                                        reference: hash_ref.clone(),
                                        source: err,
                                    }
                                })?
                                .ok_or_else(|| GetLogicalCatalogWithDepsAndFuncsError::EthCallNotAvailable {
                                    reference: hash_ref.clone(),
                                })?
                        } else {
                            dataset
                                    .function_by_name(schema.to_string(), function, isolate_pool.clone())
                                    .ok_or_else(|| {
                                        GetLogicalCatalogWithDepsAndFuncsError::FunctionNotFoundInDataset {
                                            function_name: func_ref.to_string(),
                                            reference: hash_ref.clone(),
                                        }
                                    })?
                        };

                        entry.insert(udf);
                    }
                    datasets_common::deps::alias::DepAliasOrSelfRef::SelfRef => {
                        // Same-dataset function reference (self.function_name)
                        // Look up function in the functions map (defined in this dataset)
                        if let Some(func_def) = functions.get(function) {
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
                        // If function not in functions map, it's an error (self.function should always be defined)
                        // TODO: Add proper error variant for this case
                    }
                }
            }
        }
    }

    Ok(LogicalCatalog {
        tables: tables
            .into_values()
            .flat_map(|map| map.into_values())
            .collect(),
        udfs: self_udfs
            .into_values()
            .chain(udfs.into_values().flat_map(|map| map.into_values()))
            .collect(),
    })
}

/// Type alias for the table references map used in multi-table validation
///
/// Maps table names to their SQL references (table refs and function refs) using dependency aliases or self-references.
type TableReferencesMap = BTreeMap<
    TableName,
    (
        Vec<TableReference<DepAlias>>,
        Vec<FunctionReference<DepAliasOrSelfRef>>,
    ),
>;

/// Creates a planning context for multi-table schema validation with pre-resolved dependencies.
///
/// This function validates dataset manifests by building logical catalogs for multiple
/// tables simultaneously, using dependencies that have been pre-resolved with aliases
/// by the caller.
///
/// ## Where Used
///
/// This function is used in three manifest validation paths:
///
/// 1. **Schema Endpoint** (`crates/services/admin-api/src/handlers/schema.rs`):
///    - Called via `POST /schema` endpoint from TypeScript CLI (`amp register`)
///    - Validates SQL in dataset manifests during interactive schema generation
///    - Returns schemas for manifest generation without accessing physical data
///
/// 2. **Manifest Registration** (`crates/services/admin-api/src/handlers/manifests/register.rs`):
///    - Called via `POST /manifests` endpoint during content-addressable manifest registration
///    - Validates derived dataset manifests via `datasets_derived::validate()`
///    - Ensures all SQL queries, dependencies, and table references are valid
///    - Stores validated manifests in content-addressable storage without dataset linking
///
/// 3. **Dataset Registration** (`crates/services/admin-api/src/handlers/datasets/register.rs`):
///    - Called via `POST /datasets` endpoint during dataset registration
///    - Validates derived dataset manifests via `datasets_derived::validate()`
///    - Ensures all SQL queries, dependencies, and table references are valid
///    - Prevents invalid manifests from being registered and linked to dataset versions
///
/// ## Implementation
///
/// Unlike `planning_ctx_for_sql`, this function:
/// 1. Accepts pre-resolved dependencies with aliases from the API request
/// 2. Processes multiple tables simultaneously (batch validation)
/// 3. Maps table references to user-provided dependency aliases
/// 4. Builds a unified logical catalog for all tables
/// 5. Returns planning context for schema validation only
///
/// This function does not access physical parquet files or the metadata database,
/// making it suitable for fast manifest validation during dataset registration.
///
/// ## Function Handling
///
/// Bare (unqualified) function references are handled as follows:
/// - If the function is defined in the `functions` parameter, a UDF is created for it
/// - If the function is not defined, it's assumed to be a built-in function (logged as debug)
/// - TODO: Add validation against DataFusion built-in functions to catch typos
pub async fn planning_ctx_for_sql_tables_with_deps_and_funcs(
    store: &impl DatasetAccess,
    references: TableReferencesMap,
    dependencies: BTreeMap<DepAlias, HashReference>,
    functions: BTreeMap<FuncName, Function>,
    isolate_pool: IsolatePool,
) -> Result<PlanningContext, PlanningCtxForSqlTablesWithDepsError> {
    // Use hash-based map to deduplicate datasets across ALL tables
    // Inner map: table_ref -> ResolvedTable (deduplicates table references)
    let mut tables: BTreeMap<Hash, BTreeMap<TableReference, ResolvedTable>> = BTreeMap::new();
    // Track UDFs from external dependencies - outer key: dataset hash, inner key: function reference
    // Inner map ensures deduplication: multiple function references to the same UDF share one instance
    let mut udfs: BTreeMap<Hash, BTreeMap<FunctionReference, ScalarUDF>> = BTreeMap::new();
    // Track UDFs defined in this manifest (bare functions and self-references) - separate from dependency functions
    // Ensures deduplication: multiple references to the same function share one instance
    let mut self_udfs: BTreeMap<FunctionReference<DepAliasOrSelfRef>, ScalarUDF> = BTreeMap::new();

    // Process all tables - fail fast on first error
    for (table_name, (table_refs, func_refs)) in references {
        // Part 1: Process table references for this table
        for table_ref in table_refs {
            match &table_ref {
                TableReference::Bare { .. } => {
                    return Err(PlanningCtxForSqlTablesWithDepsError::UnqualifiedTable {
                        table_name: table_name.clone(),
                        table_ref: table_ref.to_string(),
                    });
                }
                TableReference::Partial { schema, table } => {
                    // Schema is already parsed as DepAlias, lookup in dependencies map
                    let hash_ref = dependencies.get(schema.as_ref()).ok_or_else(|| {
                        PlanningCtxForSqlTablesWithDepsError::DependencyAliasNotFoundForTableRef {
                            table_name: table_name.clone(),
                            alias: schema.as_ref().clone(),
                        }
                    })?;

                    // Skip if table reference is already resolved (optimization to avoid redundant dataset loading)
                    let Entry::Vacant(entry) = tables
                        .entry(hash_ref.hash().clone())
                        .or_default()
                        .entry(table_ref.to_string_reference())
                    else {
                        continue;
                    };

                    // Load dataset by hash (cached by store)
                    let dataset = store
                        .get_dataset_by_hash(hash_ref.hash())
                        .await
                        .map_err(|err| {
                            PlanningCtxForSqlTablesWithDepsError::GetDatasetForTableRef {
                                table_name: table_name.clone(),
                                reference: hash_ref.clone(),
                                source: err,
                            }
                        })?
                        .ok_or_else(|| {
                            PlanningCtxForSqlTablesWithDepsError::DatasetNotFoundForTableRef {
                                table_name: table_name.clone(),
                                reference: hash_ref.clone(),
                            }
                        })?;

                    // Find table in dataset
                    let dataset_table = dataset
                        .tables
                        .iter()
                        .find(|t| t.name() == table)
                        .ok_or_else(|| {
                            PlanningCtxForSqlTablesWithDepsError::TableNotFoundInDataset {
                                table_name: table_name.clone(),
                                referenced_table_name: table.as_ref().clone(),
                                reference: hash_ref.clone(),
                            }
                        })?;

                    // Create ResolvedTable with the converted string-based table reference
                    let resolved_table = ResolvedTable::new(
                        table_ref.into_string_reference(),
                        dataset_table.clone(),
                        dataset.clone(),
                    );

                    // Insert into vacant entry
                    entry.insert(resolved_table);
                }
            }
        }

        // Part 2: Process function references for this table (load datasets for qualified UDFs only)
        for func_ref in func_refs {
            match &func_ref {
                // Skip bare functions - they are assumed to be built-in functions (Amp or DataFusion)
                FunctionReference::Bare { function: _ } => {
                    continue;
                }
                FunctionReference::Qualified { schema, function } => {
                    // Match on schema type: DepAlias (external dependency) or SelfRef (same-dataset function)
                    match schema.as_ref() {
                        DepAliasOrSelfRef::DepAlias(dep_alias) => {
                            // External dependency reference - lookup in dependencies map
                            let hash_ref = dependencies.get(dep_alias).ok_or_else(|| {
                                PlanningCtxForSqlTablesWithDepsError::DependencyAliasNotFoundForFunctionRef {
                                    table_name: table_name.clone(),
                                    alias: dep_alias.clone(),
                                }
                            })?;

                            // Load dataset by hash (cached by store)
                            let dataset = store
                                .get_dataset_by_hash(hash_ref.hash())
                                .await
                                .map_err(|err| {
                                    PlanningCtxForSqlTablesWithDepsError::GetDatasetForFunction {
                                        table_name: table_name.clone(),
                                        reference: hash_ref.clone(),
                                        source: err,
                                    }
                                })?
                                .ok_or_else(|| {
                                    PlanningCtxForSqlTablesWithDepsError::DatasetNotFoundForFunction {
                                        table_name: table_name.clone(),
                                        reference: hash_ref.clone(),
                                    }
                                })?;

                            // Skip if function reference is already resolved (optimization to avoid redundant UDF creation)
                            let Entry::Vacant(entry) = udfs
                                .entry(hash_ref.hash().clone())
                                .or_default()
                                .entry(func_ref.to_string_reference())
                            else {
                                continue;
                            };

                            // Get the UDF for this function reference
                            let udf = if function.as_ref() == ETH_CALL_FUNCTION_NAME {
                                store
                                    .eth_call_for_dataset(&schema.to_string(), &dataset)
                                    .await
                                    .map_err(|err| {
                                        PlanningCtxForSqlTablesWithDepsError::EthCallUdfCreationForFunction {
                                            table_name: table_name.clone(),
                                            reference: hash_ref.clone(),
                                            source: err,
                                        }
                                    })?
                                    .ok_or_else(|| {
                                        PlanningCtxForSqlTablesWithDepsError::EthCallNotAvailable {
                                            table_name: table_name.clone(),
                                            reference: hash_ref.clone(),
                                        }
                                    })?
                            } else {
                                dataset
                                    .function_by_name(schema.to_string(), function, IsolatePool::dummy())
                                    .ok_or_else(|| {
                                        PlanningCtxForSqlTablesWithDepsError::FunctionNotFoundInDataset {
                                            table_name: table_name.clone(),
                                            function_name: func_ref.to_string(),
                                            reference: hash_ref.clone(),
                                        }
                                    })?
                            };

                            entry.insert(udf);
                        }
                        DepAliasOrSelfRef::SelfRef => {
                            // Same-dataset function reference (self.function_name)
                            // Look up function in the functions map (defined in this dataset)
                            if let Some(func_def) = functions.get(function) {
                                // Skip if function reference is already resolved (optimization)
                                let Entry::Vacant(entry) = self_udfs.entry(func_ref.clone()) else {
                                    continue;
                                };

                                // Create UDF from Function definition using JsUdf
                                // Use "self" as schema qualifier to preserve case sensitivity
                                let udf = AsyncScalarUDF::new(Arc::new(JsUdf::new(
                                    isolate_pool.clone(),
                                    Some(
                                        datasets_common::deps::alias::SELF_REF_KEYWORD.to_string(),
                                    ), // Schema = "self"
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
                            } else {
                                // Function not in functions map - this is an error for self-references
                                tracing::error!(
                                    table=%table_name,
                                    function=%function.as_ref(),
                                    "Self-referenced function not defined in functions map"
                                );
                                // TODO: Add proper error variant for this case
                            }
                        }
                    }
                }
            }
        }
    }

    // Flatten to Vec<ResolvedTable> and create single unified planning context
    // Extract values from nested BTreeMap structure
    Ok(PlanningContext::new(LogicalCatalog {
        tables: tables
            .into_values()
            .flat_map(|map| map.into_values())
            .collect(),
        udfs: self_udfs
            .into_values()
            .chain(udfs.into_values().flat_map(|map| map.into_values()))
            .collect(),
    }))
}

// ================================================================================================
// Error Types
// ================================================================================================
//
// Error types for derived dataset catalog operations.
// These error types were moved from `common::catalog::errors` to break the circular
// dependency between `common` and `datasets-derived`.

#[derive(Debug, thiserror::Error)]
pub enum PlanningCtxForSqlTablesWithDepsError {
    /// Table is not qualified with a schema/dataset name.
    ///
    /// All tables must be qualified with a dataset reference in the schema portion.
    /// Unqualified tables (e.g., just `table_name`) are not allowed.
    #[error(
        "In table '{table_name}': Unqualified table '{table_ref}', all tables must be qualified with a dataset"
    )]
    UnqualifiedTable {
        table_name: TableName,
        table_ref: String,
    },

    /// Dataset reference could not be found when loading dataset for table reference.
    ///
    /// This occurs when loading a dataset referenced in a table reference fails
    /// because the dataset does not exist in the store.
    #[error("In table '{table_name}': Dataset reference '{reference}' not found")]
    DatasetNotFoundForTableRef {
        table_name: TableName,
        reference: HashReference,
    },

    /// Failed to retrieve dataset from store when loading dataset for table reference.
    ///
    /// This occurs when loading a dataset definition fails:
    /// - Dataset manifest is invalid or corrupted
    /// - Unsupported dataset kind
    /// - Storage backend errors when reading the dataset
    #[error("In table '{table_name}': Failed to retrieve dataset '{reference}'")]
    GetDatasetForTableRef {
        table_name: TableName,
        reference: HashReference,
        #[source]
        source: BoxError,
    },

    /// Dependency alias not found when processing table reference.
    ///
    /// This occurs when a table reference uses an alias that was not provided
    /// in the dependencies map.
    #[error(
        "In table '{table_name}': Dependency alias '{alias}' referenced in table but not provided in dependencies"
    )]
    DependencyAliasNotFoundForTableRef {
        table_name: TableName,
        alias: DepAlias,
    },

    /// Dependency alias not found when processing function reference.
    ///
    /// This occurs when a function reference uses an alias that was not provided
    /// in the dependencies map.
    #[error(
        "In table '{table_name}': Dependency alias '{alias}' referenced in function but not provided in dependencies"
    )]
    DependencyAliasNotFoundForFunctionRef {
        table_name: TableName,
        alias: DepAlias,
    },

    /// Dataset reference could not be found when loading dataset for function.
    ///
    /// This occurs when loading a dataset referenced in a function name fails
    /// because the dataset does not exist in the store.
    #[error("In table '{table_name}': Dataset reference '{reference}' not found for function")]
    DatasetNotFoundForFunction {
        table_name: TableName,
        reference: HashReference,
    },

    /// Failed to retrieve dataset from store when loading dataset for function.
    ///
    /// This occurs when loading a dataset definition for a function fails:
    /// - Dataset manifest is invalid or corrupted
    /// - Unsupported dataset kind
    /// - Storage backend errors when reading the dataset
    #[error("In table '{table_name}': Failed to retrieve dataset '{reference}' for function")]
    GetDatasetForFunction {
        table_name: TableName,
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
    #[error(
        "In table '{table_name}': Failed to create ETH call UDF for dataset '{reference}' for function"
    )]
    EthCallUdfCreationForFunction {
        table_name: TableName,
        reference: HashReference,
        #[source]
        source: BoxError,
    },

    /// Function not found in dataset.
    ///
    /// This occurs when a function is referenced in the SQL query but the
    /// dataset does not contain a function with that name.
    #[error(
        "In table '{table_name}': Function '{function_name}' not found in dataset '{reference}'"
    )]
    FunctionNotFoundInDataset {
        table_name: TableName,
        function_name: String,
        reference: HashReference,
    },

    /// eth_call function not available for dataset.
    ///
    /// This occurs when the eth_call function is referenced in SQL but the
    /// dataset does not support eth_call (not an EVM RPC dataset or no provider configured).
    #[error("In table '{table_name}': Function 'eth_call' not available for dataset '{reference}'")]
    EthCallNotAvailable {
        table_name: TableName,
        reference: HashReference,
    },

    /// Table not found in dataset.
    ///
    /// This occurs when the table name is referenced in the SQL query but the
    /// dataset does not contain a table with that name.
    #[error(
        "In table '{table_name}': Table '{referenced_table_name}' not found in dataset '{reference}'"
    )]
    TableNotFoundInDataset {
        table_name: TableName,
        referenced_table_name: TableName,
        reference: HashReference,
    },
}

/// Errors specific to planning_ctx_for_sql operations
///
/// This error type is used exclusively by `planning_ctx_for_sql()` to create
/// a planning context for SQL queries without requiring physical data to exist.

#[derive(Debug, thiserror::Error)]
pub enum CatalogForSqlWithDepsError {
    /// Failed to resolve table references from the SQL statement.
    ///
    /// This occurs when:
    /// - Table references contain invalid identifiers
    /// - Table references have unsupported format (not 1-3 parts)
    /// - Table names don't conform to identifier rules
    /// - Schema portion fails to parse as DepAlias
    #[error("Failed to resolve table references from SQL")]
    TableReferenceResolution(#[source] ResolveTableReferencesError<DepAliasError>),

    /// Failed to extract function names from the SQL statement.
    ///
    /// This occurs when:
    /// - The SQL statement contains DML operations (CreateExternalTable, CopyTo)
    /// - An EXPLAIN statement wraps an unsupported statement type
    /// - Schema portion fails to parse as DepAlias or self reference
    #[error("Failed to resolve function references from SQL")]
    FunctionReferenceResolution(#[source] ResolveFunctionReferencesError<DepAliasOrSelfRefError>),

    /// Failed to get the physical catalog for the resolved tables and functions.
    ///
    /// This wraps errors from `get_physical_catalog_with_deps`, which can occur when:
    /// - Dataset retrieval fails
    /// - Physical table metadata cannot be retrieved
    /// - Tables have not been synced
    /// - Dependency aliases are invalid or not found
    #[error("Failed to get physical catalog with dependencies: {0}")]
    GetPhysicalCatalogWithDeps(#[source] GetPhysicalCatalogWithDepsError),
}

/// Errors specific to get_physical_catalog_with_deps operations

#[derive(Debug, thiserror::Error)]
pub enum GetPhysicalCatalogWithDepsError {
    /// Failed to get the logical catalog with dependencies and functions.
    ///
    /// This wraps errors from `get_logical_catalog_with_deps_and_funcs`, which can occur when:
    /// - Dataset names cannot be extracted from table references or function names
    /// - Dataset retrieval fails
    /// - UDF creation fails
    /// - Dependency aliases are invalid or not found
    #[error("Failed to get logical catalog with dependencies and functions: {0}")]
    GetLogicalCatalogWithDepsAndFuncs(#[source] GetLogicalCatalogWithDepsAndFuncsError),

    /// Failed to retrieve physical table metadata from the metadata database.
    ///
    /// This occurs when querying the metadata database for the active physical
    /// location of a table fails due to database connection issues, query errors,
    /// or other database-related problems.
    #[error("Failed to retrieve physical table metadata for table '{table}': {source}")]
    PhysicalTableRetrieval { table: String, source: BoxError },

    /// Table has not been synced and no physical location exists.
    ///
    /// This occurs when attempting to load a physical catalog for a table that
    /// has been defined but has not yet been dumped/synced to storage. The table
    /// exists in the dataset definition but has no physical parquet files.
    #[error("Table '{table}' has not been synced")]
    TableNotSynced { table: String },
}

/// Errors specific to get_logical_catalog_with_deps_and_funcs operations
#[derive(Debug, thiserror::Error)]
#[allow(clippy::large_enum_variant)]
pub enum GetLogicalCatalogWithDepsAndFuncsError {
    /// Table is not qualified with a schema/dataset name.
    ///
    /// All tables must be qualified with a dataset reference in the schema portion.
    /// Unqualified tables (e.g., just `table_name`) are not allowed.
    #[error("Unqualified table '{table_ref}', all tables must be qualified with a dataset")]
    UnqualifiedTable { table_ref: String },

    /// Dependency alias not found when processing table reference.
    ///
    /// This occurs when a table reference uses an alias that was not provided
    /// in the dependencies map.
    #[error(
        "Dependency alias '{alias}' referenced in table reference but not provided in dependencies"
    )]
    DependencyAliasNotFoundForTableRef { alias: DepAlias },

    /// Dataset reference could not be found when loading dataset for table reference.
    ///
    /// This occurs when loading a dataset referenced in a table reference fails
    /// because the dataset does not exist in the store.
    #[error("Dataset reference '{reference}' not found for table reference")]
    DatasetNotFoundForTableRef { reference: HashReference },

    /// Failed to retrieve dataset from store when loading dataset for table reference.
    ///
    /// This occurs when loading a dataset definition fails:
    /// - Dataset manifest is invalid or corrupted
    /// - Unsupported dataset kind
    /// - Storage backend errors when reading the dataset
    #[error("Failed to retrieve dataset '{reference}' for table reference")]
    GetDatasetForTableRef {
        reference: HashReference,
        #[source]
        source: BoxError,
    },

    /// Dependency alias not found when processing function reference.
    ///
    /// This occurs when a function reference uses an alias that was not provided
    /// in the dependencies map.
    #[error(
        "Dependency alias '{alias}' referenced in function reference but not provided in dependencies"
    )]
    DependencyAliasNotFoundForFunctionRef { alias: DepAlias },

    /// Dataset reference could not be found when loading dataset for function.
    ///
    /// This occurs when loading a dataset referenced in a function name fails
    /// because the dataset does not exist in the store.
    #[error("Dataset reference '{reference}' not found for function reference")]
    DatasetNotFoundForFunction { reference: HashReference },

    /// Failed to retrieve dataset from store when loading dataset for function.
    ///
    /// This occurs when loading a dataset definition for a function fails:
    /// - Dataset manifest is invalid or corrupted
    /// - Unsupported dataset kind
    /// - Storage backend errors when reading the dataset
    #[error("Failed to retrieve dataset '{reference}' for function reference")]
    GetDatasetForFunction {
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
    EthCallUdfCreationForFunction {
        reference: HashReference,
        #[source]
        source: BoxError,
    },

    /// eth_call function not available for dataset.
    ///
    /// This occurs when the eth_call function is referenced in SQL but the
    /// dataset does not support eth_call (not an EVM RPC dataset or no provider configured).
    #[error("Function 'eth_call' not available for dataset '{reference}'")]
    EthCallNotAvailable { reference: HashReference },

    /// Function not found in dataset.
    ///
    /// This occurs when a function is referenced in the SQL query but the
    /// dataset does not contain a function with that name.
    #[error("Function '{function_name}' not found in dataset '{reference}'")]
    FunctionNotFoundInDataset {
        function_name: String,
        reference: HashReference,
    },

    /// Table not found in dataset.
    ///
    /// This occurs when the table name is referenced in the SQL query but the
    /// dataset does not contain a table with that name.
    #[error("Table '{table_name}' not found in dataset '{reference}'")]
    TableNotFoundInDataset {
        table_name: TableName,
        reference: HashReference,
    },
}

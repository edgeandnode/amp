//! SQL catalog creation and query planning functions.
//!
//! This module provides functions for building catalogs and planning contexts from SQL queries.
//! Each function serves a specific data path in the Amp architecture, with clear separation
//! between validation, planning, and execution operations.
//!
//! # Function-to-Data-Path Mapping
//!
//! | Function                                            | Schema Endpoint  | Manifest Validation | Query Planning   | Query Execution  | Derived Dataset  | Raw Dataset |
//! |-----------------------------------------------------|------------------|---------------------|------------------|------------------|------------------|-------------|
//! | [`planning_ctx_for_sql_tables_with_deps_and_funcs`] | ✅               | ✅                  | ❌               | ❌               | ❌               | ❌          |
//! | [`planning_ctx_for_sql`]                            | ❌               | ❌                  | ✅ **EXCLUSIVE** | ❌               | ❌               | ❌          |
//! | [`catalog_for_sql`]                                 | ❌               | ❌                  | ❌               | ✅ **EXCLUSIVE** | ❌               | ❌          |
//! | [`catalog_for_sql_with_deps`]                       | ❌               | ❌                  | ❌               | ❌               | ✅ **EXCLUSIVE** | ❌          |
//! | [`get_logical_catalog`]                             | ❌               | ❌                  | ❌               | ✅ (indirect)    | ❌               | ❌          |
//! | [`get_logical_catalog_with_deps_and_funcs`]         | ❌               | ❌                  | ❌               | ❌               | ✅ (indirect)    | ❌          |
//!
//! # Data Paths
//!
//! ## 1. Manifest Validation Path
//!
//! - **Purpose**: Validate dataset manifests without data access
//! - **Function**: [`planning_ctx_for_sql_tables_with_deps`]
//! - **Entry Points**:
//!   - `POST /schema` endpoint (`crates/services/admin-api/src/handlers/schema.rs`)
//!   - `POST /manifests` endpoint via manifest validation (`crates/services/admin-api/src/handlers/manifests/register.rs`)
//!   - `POST /datasets` endpoint via manifest validation (`crates/services/admin-api/src/handlers/datasets/register.rs`)
//! - **Characteristics**: Multi-table validation, pre-resolved dependencies, no physical data
//!
//! ## 2. Query Planning Path
//!
//! - **Purpose**: Generate query plans and schemas without execution
//! - **Function**: [`planning_ctx_for_sql`]
//! - **Entry**: Arrow Flight `GetFlightInfo` (`crates/services/server/src/flight.rs`)
//! - **Characteristics**: Fast schema response, logical catalog only, dynamic dataset resolution
//!
//! ## 3. Query Execution Path (Arrow Flight)
//!
//! - **Purpose**: Execute user queries via Arrow Flight
//! - **Function**: [`catalog_for_sql`] (calls [`get_logical_catalog`] internally)
//! - **Entry**: Arrow Flight `DoGet` (`crates/services/server/src/flight.rs`)
//! - **Characteristics**: Full catalog with physical parquet locations, dynamic dataset resolution, streaming results
//! - **Resolution Strategy**: Resolves dataset references to hashes at query time (supports "latest" and version tags)
//!
//! ## 4. Derived Dataset Execution Path
//!
//! - **Purpose**: Execute SQL to create derived datasets during extraction
//! - **Function**: [`catalog_for_sql_with_deps`] (calls [`get_logical_catalog_with_deps_and_funcs`] internally)
//! - **Entry**: Worker-based extraction for SQL datasets (`crates/core/dump/src/core/sql_dump.rs`)
//! - **Characteristics**: Full catalog with physical parquet locations, pre-resolved dependencies, writes parquet files
//! - **Resolution Strategy**: Uses locked dataset hashes from manifest dependencies (deterministic, reproducible)
//!
//! # Key Insights
//!
//! - **Clean separation**: Each public function serves exactly one primary data path
//! - **Dependency handling**: Two parallel execution paths with different resolution strategies:
//!   - **Dynamic resolution** (`catalog_for_sql`): For user queries that reference datasets by name/version
//!   - **Pre-resolved dependencies** (`catalog_for_sql_with_deps`): For derived datasets with locked dependencies
//! - **Function duplication**: `*_with_deps` variants avoid dual-mode logic and maintain clear boundaries
//! - **Lazy UDF loading**: All functions implement lazy UDF loading for optimal performance
//! - **No raw dataset overlap**: Raw dataset dumps don't use these planning functions
//! - **Two-phase resolution**: Functions without deps use a two-phase pattern:
//!   1. **Resolution phase**: `Reference` → `resolve_revision()` → `HashReference`
//!   2. **Retrieval phase**: `HashReference` → `get_dataset()` → `Dataset`
//! - **Pre-resolved deps**: Functions with deps receive `HashReference` and skip phase 1

use std::{
    collections::{BTreeMap, btree_map::Entry},
    sync::Arc,
};

use amp_data_store::DataStore;
use datafusion::{logical_expr::ScalarUDF, sql::parser::Statement};
use datasets_common::{
    func_name::ETH_CALL_FUNCTION_NAME, hash::Hash, partial_reference::PartialReference,
    reference::Reference,
};
use js_runtime::isolate_pool::IsolatePool;

use super::{
    dataset_access::DatasetAccess,
    errors::{CatalogForSqlError, GetLogicalCatalogError, PlanningCtxForSqlError},
    logical::LogicalCatalog,
    physical::{Catalog, PhysicalTable},
};
use crate::{
    PlanningContext, ResolvedTable,
    query_context::QueryEnv,
    sql::{
        FunctionReference, TableReference, resolve_function_references, resolve_table_references,
    },
};
/// Creates a full catalog with physical data access for SQL query execution.
///
/// This function builds a complete catalog that includes both logical schemas and physical
/// parquet file locations, enabling actual query execution with DataFusion.
///
/// ## Where Used
///
/// This function is used exclusively in the **Query Execution Path**:
///
/// - **Arrow Flight DoGet** (`crates/services/server/src/flight.rs`):
///   - Called during Arrow Flight `DoGet` phase to execute user queries
///   - Provides physical catalog for streaming query results to clients
///
/// For derived dataset execution, use [`catalog_for_sql_with_deps`] instead.
///
/// ## Implementation
///
/// The function:
/// 1. Extracts table references and function names from the query
/// 2. Calls [`get_logical_catalog`] to resolve datasets and build the logical catalog
/// 3. Queries metadata database for physical parquet locations
/// 4. Constructs physical catalog for query execution
pub async fn catalog_for_sql(
    dataset_store: &impl DatasetAccess,
    data_store: &DataStore,
    query: &Statement,
    env: QueryEnv,
) -> Result<Catalog, CatalogForSqlError> {
    let table_refs = resolve_table_references::<PartialReference>(query)
        .map_err(CatalogForSqlError::TableReferenceResolution)?;
    let func_refs = resolve_function_references::<PartialReference>(query)
        .map_err(CatalogForSqlError::FunctionReferenceResolution)?;

    let logical_catalog =
        get_logical_catalog(dataset_store, table_refs, func_refs, &env.isolate_pool)
            .await
            .map_err(CatalogForSqlError::GetLogicalCatalog)?;

    let mut tables = Vec::new();
    for table in &logical_catalog.tables {
        let dataset_ref = table.dataset_reference();

        let revision = data_store
            .get_table_active_revision(dataset_ref, table.name())
            .await
            .map_err(|source| CatalogForSqlError::PhysicalTableRetrieval {
                dataset: dataset_ref.clone(),
                table: table.name().clone(),
                source,
            })?
            .ok_or_else(|| CatalogForSqlError::TableNotSynced {
                dataset: dataset_ref.clone(),
                table: table.name().clone(),
            })?;

        let physical_table = PhysicalTable::from_active_revision(
            data_store.clone(),
            table.dataset_reference().clone(),
            table.dataset_start_block(),
            table.table().clone(),
            revision,
            table.sql_table_ref_schema().to_string(),
        );
        tables.push(Arc::new(physical_table));
    }

    Ok(Catalog::new(tables, logical_catalog))
}

/// Creates a full catalog with physical data access for SQL query execution with pre-resolved dependencies.
///
/// This function builds a complete catalog that includes both logical schemas and physical
/// parquet file locations, enabling actual query execution with DataFusion. Unlike
/// `catalog_for_sql`, this function accepts pre-resolved dependencies from a derived dataset
/// manifest, ensuring that SQL schema references map to the exact dataset versions specified
/// in the manifest's dependencies.
///
/// ## Where Used
///
/// This function is used exclusively in the **Derived Dataset Execution Path**:
///
/// - **`crates/core/dump/src/core/sql_dump.rs`**:
///   - Called during worker-based extraction to execute SQL-defined derived datasets
///   - Uses dependencies from the derived dataset manifest
///   - Writes query results as parquet files to object storage
///
/// ## Implementation
///
/// The function analyzes the SQL query to:
/// 1. Extract table references and function names from the query
/// 2. Map SQL schema references to pre-resolved dependency aliases
/// 3. Load datasets by their locked hashes from the manifest dependencies
/// 4. Build logical catalog with schemas and UDFs
/// 5. Query metadata database for physical parquet locations
/// 6. Construct physical catalog for query execution
///
/// ## Dependencies Parameter
///
/// The `dependencies` parameter is a map from dependency aliases (as used in SQL schema qualifiers)
/// to fully-qualified dataset names and their content hashes. This ensures that:
/// - SQL queries use the exact dataset versions declared in the manifest
/// - No dynamic version resolution occurs during execution
/// - Derived datasets are reproducible and deterministic
///
/// Creates a planning context for SQL query planning without physical data access.
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
/// 4. Return planning context for DataFusion query planning
///
/// Unlike `catalog_for_sql`, this does not query the metadata database for physical
/// parquet locations, making it faster for planning-only operations.
pub async fn planning_ctx_for_sql(
    store: &impl DatasetAccess,
    query: &Statement,
) -> Result<PlanningContext, PlanningCtxForSqlError> {
    // Get table and function references from the SQL query
    let table_refs = resolve_table_references::<PartialReference>(query)
        .map_err(PlanningCtxForSqlError::TableReferenceResolution)?;
    let func_refs = resolve_function_references::<PartialReference>(query)
        .map_err(PlanningCtxForSqlError::FunctionReferenceResolution)?;

    // Use hash-based map to deduplicate datasets and collect resolved tables
    // Inner map: table_ref -> ResolvedTable (deduplicates table references)
    let mut tables: BTreeMap<Hash, BTreeMap<TableReference, ResolvedTable>> = BTreeMap::new();
    // Track UDFs separately from datasets - outer key: dataset hash, inner key: function reference
    // Inner map ensures deduplication: multiple function references to the same UDF share one instance
    let mut udfs: BTreeMap<Hash, BTreeMap<FunctionReference, ScalarUDF>> = BTreeMap::new();

    // Part 1: Process table references
    for table_ref in table_refs {
        match &table_ref {
            TableReference::Bare { .. } => {
                return Err(PlanningCtxForSqlError::UnqualifiedTable {
                    table_ref: table_ref.to_string(),
                });
            }
            TableReference::Partial { schema, table } => {
                // Schema is already parsed as PartialReference, convert to Reference
                let reference: Reference = schema.as_ref().clone().into();

                // Resolve reference to hash reference
                let hash_ref = store
                    .resolve_revision(&reference)
                    .await
                    .map_err(|err| PlanningCtxForSqlError::ResolveDatasetReference {
                        reference: reference.clone(),
                        source: err,
                    })?
                    .ok_or_else(|| PlanningCtxForSqlError::ResolveDatasetReference {
                        reference: reference.clone(),
                        source: format!("Dataset '{}' not found", reference).into(),
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

                // Load dataset by hash reference (cached by store)
                let dataset = store.get_dataset(&hash_ref).await.map_err(|err| {
                    PlanningCtxForSqlError::LoadDataset {
                        reference: hash_ref.clone(),
                        source: err,
                    }
                })?;

                // Find table in dataset
                let dataset_table = dataset
                    .tables
                    .iter()
                    .find(|t| t.name() == table)
                    .ok_or_else(|| PlanningCtxForSqlError::TableNotFoundInDataset {
                        table_name: table.as_ref().clone(),
                        reference: hash_ref.clone(),
                    })?;

                // Create ResolvedTable
                let resolved_table = ResolvedTable::new(
                    dataset_table.clone(),
                    schema.to_string(),
                    hash_ref.clone(),
                    dataset.start_block,
                );

                // Insert into vacant entry
                entry.insert(resolved_table);
            }
        }
    }

    // Part 2: Process function names (load datasets for UDFs only)
    for func_ref in func_refs {
        match &func_ref {
            FunctionReference::Bare { .. } => continue, // Built-in DataFusion function
            FunctionReference::Qualified { schema, function } => {
                // Schema is already parsed as PartialReference, convert to Reference
                let reference: Reference = schema.as_ref().clone().into();

                // Resolve reference to hash reference
                let hash_ref = store
                    .resolve_revision(&reference)
                    .await
                    .map_err(|err| PlanningCtxForSqlError::ResolveDatasetReference {
                        reference: reference.clone(),
                        source: err,
                    })?
                    .ok_or_else(|| PlanningCtxForSqlError::ResolveDatasetReference {
                        reference: reference.clone(),
                        source: format!("Dataset '{}' not found", reference).into(),
                    })?;

                // Load dataset by hash reference (cached by store)
                let dataset = store.get_dataset(&hash_ref).await.map_err(|err| {
                    PlanningCtxForSqlError::LoadDataset {
                        reference: hash_ref.clone(),
                        source: err,
                    }
                })?;

                // Convert func_ref to use String schema for internal data structures
                let func_ref_string =
                    FunctionReference::qualified(schema.to_string(), function.as_ref().clone());

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
                        .map_err(|err| PlanningCtxForSqlError::EthCallUdfCreation {
                            reference: hash_ref.clone(),
                            source: err,
                        })?
                        .ok_or_else(|| PlanningCtxForSqlError::EthCallNotAvailable {
                            reference: hash_ref.clone(),
                        })?
                } else {
                    dataset
                        .function_by_name(schema.to_string(), function, IsolatePool::dummy())
                        .ok_or_else(|| PlanningCtxForSqlError::FunctionNotFoundInDataset {
                            function_name: func_ref.to_string(),
                            reference: hash_ref,
                        })?
                };

                entry.insert(udf);
            }
        }
    }

    Ok(PlanningContext::new(LogicalCatalog {
        tables: tables
            .into_values()
            .flat_map(|map| map.into_values())
            .collect(),
        udfs: udfs
            .into_values()
            .flat_map(|map| map.into_values())
            .collect(),
    }))
}

/// Internal helper that builds a logical catalog from table and function references.
///
/// This function resolves dataset references, loads datasets, and constructs a logical
/// catalog containing resolved tables and UDFs for query planning and execution.
///
/// ## Where Used
///
/// Called internally by [`catalog_for_sql`] to build the logical catalog before
/// retrieving physical parquet locations from the metadata database.
async fn get_logical_catalog(
    store: &impl DatasetAccess,
    table_refs: impl IntoIterator<Item = TableReference<PartialReference>>,
    func_refs: impl IntoIterator<Item = FunctionReference<PartialReference>>,
    isolate_pool: &IsolatePool,
) -> Result<LogicalCatalog, GetLogicalCatalogError> {
    let table_refs = table_refs.into_iter().collect::<Vec<_>>();
    let func_refs = func_refs.into_iter().collect::<Vec<_>>();

    // Use hash-based map to deduplicate datasets and collect resolved tables
    // Inner map: table_ref -> ResolvedTable (deduplicates table references)
    let mut tables: BTreeMap<Hash, BTreeMap<TableReference, ResolvedTable>> = BTreeMap::new();
    // Track UDFs separately from datasets - outer key: dataset hash, inner key: function reference
    // Inner map ensures deduplication: multiple function references to the same UDF share one instance
    let mut udfs: BTreeMap<Hash, BTreeMap<FunctionReference, ScalarUDF>> = BTreeMap::new();

    // Part 1: Process table references
    for table_ref in table_refs {
        match &table_ref {
            TableReference::Bare { .. } => {
                return Err(GetLogicalCatalogError::UnqualifiedTable {
                    table_ref: table_ref.to_string(),
                });
            }
            TableReference::Partial { schema, table } => {
                // Schema is already parsed as PartialReference, convert to Reference
                let reference: Reference = schema.as_ref().clone().into();

                // Resolve reference to hash reference
                let hash_ref = store
                    .resolve_revision(&reference)
                    .await
                    .map_err(|err| GetLogicalCatalogError::ResolveDatasetReference {
                        reference: reference.clone(),
                        source: err,
                    })?
                    .ok_or_else(|| GetLogicalCatalogError::ResolveDatasetReference {
                        reference: reference.clone(),
                        source: format!("Dataset '{}' not found", reference).into(),
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

                // Load dataset by hash reference (cached by store)
                let dataset = store.get_dataset(&hash_ref).await.map_err(|err| {
                    GetLogicalCatalogError::LoadDataset {
                        reference: hash_ref.clone(),
                        source: err,
                    }
                })?;

                // Find table in dataset
                let dataset_table = dataset
                    .tables
                    .iter()
                    .find(|t| t.name() == table)
                    .ok_or_else(|| GetLogicalCatalogError::TableNotFoundInDataset {
                        table_name: table.as_ref().clone(),
                        reference: hash_ref.clone(),
                    })?;

                // Create ResolvedTable
                let resolved_table = ResolvedTable::new(
                    dataset_table.clone(),
                    schema.to_string(),
                    hash_ref.clone(),
                    dataset.start_block,
                );

                // Insert into vacant entry
                entry.insert(resolved_table);
            }
        }
    }

    // Part 2: Process function names (load datasets for UDFs only)
    for func_ref in func_refs {
        match &func_ref {
            FunctionReference::Bare { .. } => continue, // Built-in DataFusion function
            FunctionReference::Qualified { schema, function } => {
                // Schema is already parsed as PartialReference, convert to Reference
                let reference: Reference = schema.as_ref().clone().into();

                // Resolve reference to hash reference
                let hash_ref = store
                    .resolve_revision(&reference)
                    .await
                    .map_err(|err| GetLogicalCatalogError::ResolveDatasetReference {
                        reference: reference.clone(),
                        source: err,
                    })?
                    .ok_or_else(|| GetLogicalCatalogError::ResolveDatasetReference {
                        reference: reference.clone(),
                        source: format!("Dataset '{}' not found", reference).into(),
                    })?;

                // Load dataset by hash reference (cached by store)
                let dataset = store.get_dataset(&hash_ref).await.map_err(|err| {
                    GetLogicalCatalogError::LoadDataset {
                        reference: hash_ref.clone(),
                        source: err,
                    }
                })?;

                // Convert func_ref to use String schema for internal data structures
                let func_ref_string =
                    FunctionReference::qualified(schema.to_string(), function.as_ref().clone());

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
                        .map_err(|err| GetLogicalCatalogError::EthCallUdfCreation {
                            reference: hash_ref.clone(),
                            source: err,
                        })?
                        .ok_or_else(|| GetLogicalCatalogError::EthCallNotAvailable {
                            reference: hash_ref.clone(),
                        })?
                } else {
                    dataset
                        .function_by_name(schema.to_string(), function, isolate_pool.clone())
                        .ok_or_else(|| GetLogicalCatalogError::FunctionNotFoundInDataset {
                            function_name: func_ref.to_string(),
                            reference: hash_ref,
                        })?
                };

                entry.insert(udf);
            }
        }
    }

    Ok(LogicalCatalog {
        tables: tables
            .into_values()
            .flat_map(|map| map.into_values())
            .collect(),
        udfs: udfs
            .into_values()
            .flat_map(|map| map.into_values())
            .collect(),
    })
}

//! SQL catalog creation and query planning functions.
//!
//! This module provides functions for building catalogs and planning contexts from SQL queries.
//! Each function serves a specific data path in the Amp architecture, with clear separation
//! between validation, planning, and execution operations.
//!
//! # Function-to-Data-Path Mapping
//!
//! | Function                                  | Schema Endpoint  | Query Planning   | Query Execution  | Derived Dataset  | Raw Dataset |
//! |-------------------------------------------|------------------|------------------|------------------|------------------|-------------|
//! | [`planning_ctx_for_sql_tables_with_deps`] | ✅ **EXCLUSIVE** | ❌               | ❌               | ❌               | ❌          |
//! | [`planning_ctx_for_sql`]                  | ❌               | ✅ **EXCLUSIVE** | ❌               | ❌               | ❌          |
//! | [`catalog_for_sql`]                       | ❌               | ❌               | ✅ **PRIMARY**   | ✅ **PRIMARY**   | ❌          |
//! | [`get_logical_catalog`]                   | ❌               | ❌               | ✅ (indirect)    | ✅ (indirect)    | ❌          |
//!
//! # Data Paths
//!
//! ## 1. Schema Endpoint Path
//!
//! - **Purpose**: Validate dataset manifests without data access
//! - **Function**: [`planning_ctx_for_sql_tables_with_deps`]
//! - **Entry**: `POST /schema` via admin API (`crates/services/admin-api/src/handlers/schema.rs`)
//! - **Characteristics**: Multi-table validation, pre-resolved dependencies, no physical data
//!
//! ## 2. Query Planning Path
//!
//! - **Purpose**: Generate query plans and schemas without execution
//! - **Function**: [`planning_ctx_for_sql`]
//! - **Entry**: Arrow Flight `GetFlightInfo` (`crates/services/server/src/flight.rs`)
//! - **Characteristics**: Fast schema response, logical catalog only
//!
//! ## 3. Query Execution Path
//!
//! - **Purpose**: Execute user queries via Arrow Flight
//! - **Function**: [`catalog_for_sql`] (calls [`get_logical_catalog`] internally)
//! - **Entry**: Arrow Flight `DoGet` (`crates/services/server/src/flight.rs`)
//! - **Characteristics**: Full catalog with physical parquet locations, streaming results
//!
//! ## 4. Derived Dataset Execution Path
//!
//! - **Purpose**: Execute SQL to create derived datasets during dumps
//! - **Function**: [`catalog_for_sql`] (calls [`get_logical_catalog`] internally)
//! - **Entry**: `ampd dump` for SQL datasets (`crates/core/dump/src/sql_dump.rs`)
//! - **Characteristics**: Shares execution logic with query path, writes parquet files
//!
//! # Key Insights
//!
//! - **Clean separation**: Each public function serves exactly one primary data path
//! - **Shared execution**: Query and derived dataset paths use the same catalog logic
//! - **Lazy UDF loading**: All functions implement lazy UDF loading for optimal performance
//! - **No raw dataset overlap**: Raw dataset dumps don't use these planning functions

use std::{collections::BTreeMap, sync::Arc};

use datafusion::{
    logical_expr::ScalarUDF,
    sql::{TableReference, parser::Statement, resolve::resolve_table_references},
};
use datasets_common::{
    fqn::FullyQualifiedName, hash::Hash, partial_reference::PartialReference, reference::Reference,
    revision::Revision, table_name::TableName,
};
use js_runtime::isolate_pool::IsolatePool;
use metadata_db::MetadataDb;

use super::{
    dataset_access::DatasetAccess,
    errors::{
        CatalogForSqlError, GetLogicalCatalogError, GetPhysicalCatalogError,
        PlanningCtxForSqlError, PlanningCtxForSqlTablesWithDepsError,
    },
    logical::LogicalCatalog,
    physical::{Catalog, PhysicalTable},
};
use crate::{PlanningContext, ResolvedTable, query_context::QueryEnv};

/// Creates a full catalog with physical data access for SQL query execution.
///
/// This function builds a complete catalog that includes both logical schemas and physical
/// parquet file locations, enabling actual query execution with DataFusion.
///
/// ## Where Used
///
/// This function is used in two data paths that require actual SQL execution against physical data:
///
/// 1. **Query Execution Path** (`crates/services/server/src/flight.rs`):
///    - Called during Arrow Flight `DoGet` phase to execute user queries
///    - Provides physical catalog for streaming query results to clients
///
/// 2. **Derived Dataset Execution Path** (`crates/core/dump/src/sql_dump.rs`):
///    - Called during `ampd dump` to execute SQL-defined derived datasets
///    - Writes query results as parquet files to object storage
///
/// ## Implementation
///
/// The function analyzes the SQL query to:
/// 1. Extract table references and function names from the query
/// 2. Resolve dataset names to hashes via the dataset store
/// 3. Build logical catalog with schemas and UDFs
/// 4. Query metadata database for physical parquet locations
/// 5. Construct physical catalog for query execution
pub async fn catalog_for_sql(
    store: &impl DatasetAccess,
    metadata_db: &MetadataDb,
    query: &Statement,
    env: QueryEnv,
) -> Result<Catalog, CatalogForSqlError> {
    let (tables, _) = resolve_table_references(query, true)
        .map_err(|err| CatalogForSqlError::TableReferenceResolution(err.into()))?;
    let function_refs = resolve_function_references(query)
        .map_err(CatalogForSqlError::FunctionReferenceResolution)?;

    get_physical_catalog(store, metadata_db, tables, function_refs, &env)
        .await
        .map_err(CatalogForSqlError::GetPhysicalCatalog)
}

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
    let (table_refs, _) = resolve_table_references(query, true)
        .map_err(PlanningCtxForSqlError::TableReferenceResolution)?;
    let function_refs = resolve_function_references(query)
        .map_err(PlanningCtxForSqlError::FunctionReferenceResolution)?;

    // Use hash-based map to deduplicate datasets and collect resolved tables
    // Inner map: table_ref string -> ResolvedTable (deduplicates table references)
    let mut tables: BTreeMap<Hash, BTreeMap<String, ResolvedTable>> = BTreeMap::new();
    // Track UDFs separately from datasets - outer key: dataset hash, inner key: qualified UDF name
    // Inner map ensures deduplication: multiple function references to the same UDF share one instance
    let mut udfs: BTreeMap<Hash, BTreeMap<String, ScalarUDF>> = BTreeMap::new();

    // Part 1: Process table references
    for table_ref in &table_refs {
        // Check if table reference is catalog-qualified (not supported)
        if table_ref.catalog().is_some() {
            return Err(PlanningCtxForSqlError::CatalogQualifiedTable {
                table_ref: table_ref.to_string(),
            });
        }

        // Check if schema is present
        let schema_str =
            table_ref
                .schema()
                .ok_or_else(|| PlanningCtxForSqlError::UnqualifiedTable {
                    table_ref: table_ref.to_string(),
                })?;

        // Validate table name is a valid TableName
        let table_name: TableName =
            table_ref
                .table()
                .parse()
                .map_err(|err| PlanningCtxForSqlError::InvalidTableName {
                    table_name: table_ref.table().to_string(),
                    table_ref: table_ref.to_string(),
                    source: err,
                })?;

        // Parse schema to PartialReference -> Reference
        let partial_ref: PartialReference =
            schema_str
                .parse()
                .map_err(|err| PlanningCtxForSqlError::InvalidSchemaReference {
                    schema: schema_str.to_string(),
                    source: err,
                })?;

        let reference: Reference = partial_ref.into();

        // Resolve reference to hash
        let hash = store
            .resolve_dataset_reference(&reference)
            .await
            .map_err(|err| PlanningCtxForSqlError::ResolveHash {
                reference: reference.clone(),
                source: err,
            })?
            .ok_or_else(|| {
                tracing::error!(
                    reference = %reference,
                    "Dataset not found"
                );
                PlanningCtxForSqlError::DatasetNotFound {
                    reference: reference.clone(),
                }
            })?;

        // Use the full reference with hash revision
        let reference = {
            let (namespace, name, _) = reference.into_parts();
            Reference::new(namespace, name, Revision::Hash(hash.clone()))
        };

        // Load dataset by hash (cached by store)
        let dataset = store
            .get_dataset_by_hash(&hash)
            .await
            .map_err(|err| PlanningCtxForSqlError::GetDataset {
                reference: reference.clone(),
                source: err,
            })?
            .ok_or_else(|| PlanningCtxForSqlError::DatasetNotFound {
                reference: reference.clone(),
            })?;

        // Get or create entry for this dataset's tables
        let resolved_tables = tables.entry(hash).or_default();

        // Find table in dataset and create ResolvedTable
        let table = dataset
            .tables
            .iter()
            .find(|t| t.name() == &table_name)
            .ok_or_else(|| PlanningCtxForSqlError::TableNotFoundInDataset {
                table_name,
                reference,
            })?;

        // Use the original schema string from SQL as the schema name
        let table_ref = TableReference::partial(schema_str, table.name().as_str());
        let resolved_table = ResolvedTable::new(table.clone(), dataset.clone(), table_ref.clone());

        // Insert into map - automatically deduplicates by full table reference
        resolved_tables.insert(table_ref.to_string(), resolved_table);
    }

    // Part 2: Process function names (load datasets for UDFs only)
    for func_ref in &function_refs {
        match func_ref {
            FunctionReference::Bare { .. } => continue, // Built-in DataFusion function
            FunctionReference::Qualified { schema, function } => {
                // Parse schema part to PartialReference -> Reference
                let reference: Reference = schema
                    .parse::<PartialReference>()
                    .map_err(|err| PlanningCtxForSqlError::InvalidFunctionReference {
                        function: func_ref.to_string(),
                        source: err,
                    })?
                    .into();

                // Resolve reference to hash
                let hash = store
                    .resolve_dataset_reference(&reference)
                    .await
                    .map_err(|err| PlanningCtxForSqlError::ResolveHash {
                        reference: reference.clone(),
                        source: err,
                    })?
                    .ok_or_else(|| PlanningCtxForSqlError::DatasetNotFound {
                        reference: reference.clone(),
                    })?;

                // Use the full reference with hash revision
                let reference = {
                    let (namespace, name, _) = reference.into_parts();
                    Reference::new(namespace, name, Revision::Hash(hash.clone()))
                };

                // Load dataset by hash (cached by store)
                let dataset = store
                    .get_dataset_by_hash(&hash)
                    .await
                    .map_err(|err| PlanningCtxForSqlError::GetDataset {
                        reference: reference.clone(),
                        source: err,
                    })?
                    .ok_or_else(|| PlanningCtxForSqlError::DatasetNotFound {
                        reference: reference.clone(),
                    })?;

                // Get or create entry for this dataset's UDFs
                let resolved_udfs = udfs.entry(hash).or_default();

                // Get the UDF for this function reference
                let udf = if function.as_ref() == "eth_call" {
                    store
                        .eth_call_for_dataset(schema, &dataset)
                        .await
                        .map_err(|err| PlanningCtxForSqlError::EthCallUdfCreation {
                            reference: reference.clone(),
                            source: err,
                        })?
                        .ok_or_else(|| PlanningCtxForSqlError::EthCallNotAvailable {
                            reference: reference.clone(),
                        })?
                } else {
                    dataset
                        .function_by_name(schema.to_string(), function, IsolatePool::dummy())
                        .ok_or_else(|| PlanningCtxForSqlError::FunctionNotFoundInDataset {
                            function_name: func_ref.to_string(),
                            reference: reference.clone(),
                        })?
                };

                resolved_udfs.insert(func_ref.to_string(), udf);
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

/// Internal helper that converts logical catalog to physical catalog with data locations.
///
/// This function queries the metadata database to retrieve physical parquet file locations
/// for all tables in the logical catalog, enabling actual query execution.
///
/// ## Where Used
///
/// Called internally by `catalog_for_sql` as part of building the full physical catalog
/// for query execution.
async fn get_physical_catalog(
    store: &impl DatasetAccess,
    metadata_db: &MetadataDb,
    table_refs: impl IntoIterator<Item = TableReference>,
    function_refs: impl IntoIterator<Item = FunctionReference>,
    env: &QueryEnv,
) -> Result<Catalog, GetPhysicalCatalogError> {
    let logical_catalog = get_logical_catalog(store, table_refs, function_refs, &env.isolate_pool)
        .await
        .map_err(GetPhysicalCatalogError::GetLogicalCatalog)?;

    let mut tables = Vec::new();
    for table in &logical_catalog.tables {
        let physical_table = PhysicalTable::get_active(table, metadata_db.clone())
            .await
            .map_err(|err| GetPhysicalCatalogError::PhysicalTableRetrieval {
                table: table.to_string(),
                source: err,
            })?
            .ok_or(GetPhysicalCatalogError::TableNotSynced {
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
///
/// This function is part of the catalog construction pipeline for query execution and
/// derived dataset dumps.
async fn get_logical_catalog(
    store: &impl DatasetAccess,
    table_refs: impl IntoIterator<Item = TableReference>,
    func_refs: impl IntoIterator<Item = FunctionReference>,
    isolate_pool: &IsolatePool,
) -> Result<LogicalCatalog, GetLogicalCatalogError> {
    let table_refs = table_refs.into_iter().collect::<Vec<_>>();
    let function_refs = func_refs.into_iter().collect::<Vec<_>>();

    // Use hash-based map to deduplicate datasets and collect resolved tables
    // Inner map: table_ref string -> ResolvedTable (deduplicates table references)
    let mut tables: BTreeMap<Hash, BTreeMap<String, ResolvedTable>> = BTreeMap::new();
    // Track UDFs separately from datasets - outer key: dataset hash, inner key: qualified UDF name
    // Inner map ensures deduplication: multiple function references to the same UDF share one instance
    let mut udfs: BTreeMap<Hash, BTreeMap<String, ScalarUDF>> = BTreeMap::new();

    // Part 1: Process table references
    for table_ref in &table_refs {
        // Check if table reference is catalog-qualified (not supported)
        if table_ref.catalog().is_some() {
            return Err(GetLogicalCatalogError::CatalogQualifiedTable {
                table_ref: table_ref.to_string(),
            });
        }

        // Check if schema is present
        let schema_str =
            table_ref
                .schema()
                .ok_or_else(|| GetLogicalCatalogError::UnqualifiedTable {
                    table_ref: table_ref.to_string(),
                })?;

        // Validate table name is a valid TableName
        let table_name: TableName =
            table_ref
                .table()
                .parse()
                .map_err(|err| GetLogicalCatalogError::InvalidTableName {
                    table_name: table_ref.table().to_string(),
                    table_ref: table_ref.to_string(),
                    source: err,
                })?;

        // Parse schema to PartialReference -> Reference
        let partial_ref: PartialReference =
            schema_str
                .parse()
                .map_err(|err| GetLogicalCatalogError::InvalidSchemaReference {
                    schema: schema_str.to_string(),
                    source: err,
                })?;

        let reference: Reference = partial_ref.into();

        // Resolve reference to hash
        let hash = store
            .resolve_dataset_reference(&reference)
            .await
            .map_err(|err| GetLogicalCatalogError::ResolveHash {
                reference: reference.clone(),
                source: err,
            })?
            .ok_or_else(|| {
                tracing::error!(
                    reference = %reference,
                    "Dataset not found"
                );
                GetLogicalCatalogError::DatasetNotFound {
                    reference: reference.clone(),
                }
            })?;

        // Use the full reference with hash revision
        let reference = {
            let (namespace, name, _) = reference.into_parts();
            Reference::new(namespace, name, Revision::Hash(hash.clone()))
        };

        // Load dataset by hash (cached by store)
        let dataset = store
            .get_dataset_by_hash(&hash)
            .await
            .map_err(|err| GetLogicalCatalogError::GetDataset {
                reference: reference.clone(),
                source: err,
            })?
            .ok_or_else(|| GetLogicalCatalogError::DatasetNotFound {
                reference: reference.clone(),
            })?;

        // Get or create entry for this dataset's tables
        let resolved_tables = tables.entry(hash).or_default();

        // Find table in dataset and create ResolvedTable
        if let Some(table) = dataset.tables.iter().find(|t| t.name() == &table_name) {
            // Use the original schema string from SQL as the schema name
            let table_ref = TableReference::partial(schema_str, table.name().as_str());
            let resolved_table =
                ResolvedTable::new(table.clone(), dataset.clone(), table_ref.clone());

            // Insert into map - automatically deduplicates by full table reference
            resolved_tables.insert(table_ref.to_string(), resolved_table);
        }
    }

    // Part 2: Process function names (load datasets for UDFs only)
    for func_ref in &function_refs {
        match func_ref {
            FunctionReference::Bare { .. } => continue, // Built-in DataFusion function
            FunctionReference::Qualified { schema, function } => {
                // Parse schema part to PartialReference -> Reference
                let reference: Reference = schema
                    .parse::<PartialReference>()
                    .map_err(|err| GetLogicalCatalogError::InvalidFunctionReference {
                        function: func_ref.to_string(),
                        source: err,
                    })?
                    .into();

                // Resolve reference to hash
                let hash = store
                    .resolve_dataset_reference(&reference)
                    .await
                    .map_err(|err| GetLogicalCatalogError::ResolveHash {
                        reference: reference.clone(),
                        source: err,
                    })?
                    .ok_or_else(|| GetLogicalCatalogError::DatasetNotFound {
                        reference: reference.clone(),
                    })?;

                // Use the full reference with hash revision
                let reference = {
                    let (namespace, name, _) = reference.into_parts();
                    Reference::new(namespace, name, Revision::Hash(hash.clone()))
                };

                // Load dataset by hash (cached by store)
                let dataset = store
                    .get_dataset_by_hash(&hash)
                    .await
                    .map_err(|err| GetLogicalCatalogError::GetDataset {
                        reference: reference.clone(),
                        source: err,
                    })?
                    .ok_or_else(|| GetLogicalCatalogError::DatasetNotFound {
                        reference: reference.clone(),
                    })?;

                // Get or create entry for this dataset's UDFs
                let resolved_udfs = udfs.entry(hash).or_default();

                // Get the UDF for this function reference
                let udf = if function.as_ref() == "eth_call" {
                    store
                        .eth_call_for_dataset(schema, &dataset)
                        .await
                        .map_err(|err| GetLogicalCatalogError::EthCallUdfCreation {
                            reference: reference.clone(),
                            source: err,
                        })?
                        .ok_or_else(|| GetLogicalCatalogError::EthCallNotAvailable {
                            reference: reference.clone(),
                        })?
                } else {
                    dataset
                        .function_by_name(schema.to_string(), function, isolate_pool.clone())
                        .ok_or_else(|| GetLogicalCatalogError::FunctionNotFoundInDataset {
                            function_name: func_ref.to_string(),
                            reference: reference.clone(),
                        })?
                };

                resolved_udfs.insert(func_ref.to_string(), udf);
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

/// Creates a planning context for multi-table schema validation with pre-resolved dependencies.
///
/// This function validates dataset manifests by building logical catalogs for multiple
/// tables simultaneously, using dependencies that have been pre-resolved with aliases
/// by the caller.
///
/// ## Where Used
///
/// This function is used exclusively in the **Schema Endpoint Path**:
///
/// - **Admin API Schema Handler** (`crates/services/admin-api/src/handlers/schema.rs`):
///   - Called via `POST /schema` endpoint from TypeScript CLI (`amp register`)
///   - Validates SQL in dataset manifests before registration
///   - Handles multiple tables with shared dependencies in a single request
///   - Returns schemas for manifest generation without accessing physical data
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
pub async fn planning_ctx_for_sql_tables_with_deps(
    store: &impl DatasetAccess,
    references: BTreeMap<TableName, (Vec<TableReference>, Vec<FunctionReference>)>,
    dependencies: BTreeMap<String, (FullyQualifiedName, Hash)>,
) -> Result<PlanningContext, PlanningCtxForSqlTablesWithDepsError> {
    // Use hash-based map to deduplicate datasets across ALL tables
    // Inner map: table_ref string -> ResolvedTable (deduplicates table references)
    let mut tables: BTreeMap<Hash, BTreeMap<String, ResolvedTable>> = BTreeMap::new();
    // Track UDFs separately from datasets - outer key: dataset hash, inner key: qualified UDF name
    // Inner map ensures deduplication: multiple function references to the same UDF share one instance
    let mut udfs: BTreeMap<Hash, BTreeMap<String, ScalarUDF>> = BTreeMap::new();

    // Process all tables - fail fast on first error
    for (table_name, (table_refs, function_refs)) in references {
        // Part 1: Process table references for this table
        for table_ref in &table_refs {
            // Check if table reference is catalog-qualified (not supported)
            if table_ref.catalog().is_some() {
                return Err(
                    PlanningCtxForSqlTablesWithDepsError::CatalogQualifiedTable {
                        table_name,
                        table_ref: table_ref.to_string(),
                    },
                );
            }

            // Check if schema is present (schema = alias name)
            let schema_str = table_ref.schema().ok_or_else(|| {
                PlanningCtxForSqlTablesWithDepsError::UnqualifiedTable {
                    table_name: table_name.clone(),
                    table_ref: table_ref.to_string(),
                }
            })?;

            // Validate table name is a valid TableName
            let referenced_table_name: TableName = table_ref.table().parse().map_err(|err| {
                PlanningCtxForSqlTablesWithDepsError::InvalidTableName {
                    table_name: table_name.clone(),
                    invalid_table_name: table_ref.table().to_string(),
                    table_ref: table_ref.to_string(),
                    source: err,
                }
            })?;

            // Lookup alias in dependencies map (schema_str = alias)
            let (fqn, hash) = dependencies.get(schema_str).ok_or_else(|| {
                PlanningCtxForSqlTablesWithDepsError::DependencyAliasNotFoundForTableRef {
                    table_name: table_name.clone(),
                    alias: schema_str.to_string(),
                }
            })?;

            // Build reference from FQN and Hash (already resolved by handler)
            let reference = Reference::new(
                fqn.namespace().clone(),
                fqn.name().clone(),
                Revision::Hash(hash.clone()),
            );

            // Load dataset by hash (cached by store)
            let dataset = store
                .get_dataset_by_hash(hash)
                .await
                .map_err(
                    |err| PlanningCtxForSqlTablesWithDepsError::GetDatasetForTableRef {
                        table_name: table_name.clone(),
                        reference: reference.clone(),
                        source: err,
                    },
                )?
                .ok_or_else(|| {
                    PlanningCtxForSqlTablesWithDepsError::DatasetNotFoundForTableRef {
                        table_name: table_name.clone(),
                        reference: reference.clone(),
                    }
                })?;

            // Get or create entry for this dataset's tables
            let resolved_tables = tables.entry(hash.clone()).or_default();

            // Find table in dataset and create ResolvedTable
            let table = dataset
                .tables
                .iter()
                .find(|t| t.name() == &referenced_table_name)
                .ok_or_else(
                    || PlanningCtxForSqlTablesWithDepsError::TableNotFoundInDataset {
                        table_name: table_name.clone(),
                        referenced_table_name,
                        reference,
                    },
                )?;

            // Use the original alias as the schema name
            let table_ref = TableReference::partial(schema_str, table.name().as_str());
            let resolved_table =
                ResolvedTable::new(table.clone(), dataset.clone(), table_ref.clone());

            // Insert into map - automatically deduplicates by full table reference
            resolved_tables.insert(table_ref.to_string(), resolved_table);
        }

        // Part 2: Process function references for this table (load datasets for UDFs only)
        for func_ref in &function_refs {
            match func_ref {
                FunctionReference::Bare { .. } => continue, // Built-in DataFusion function
                FunctionReference::Qualified { schema, function } => {
                    // Lookup alias in dependencies map (schema_str = alias)
                    let (fqn, hash) = dependencies.get(schema.as_ref()).ok_or_else(|| {
                        PlanningCtxForSqlTablesWithDepsError::DependencyAliasNotFoundForFunction {
                            table_name: table_name.clone(),
                            alias: schema.to_string(),
                        }
                    })?;

                    // Build reference from FQN and Hash
                    let reference = Reference::new(
                        fqn.namespace().clone(),
                        fqn.name().clone(),
                        Revision::Hash(hash.clone()),
                    );

                    // Load dataset by hash (cached by store)
                    let dataset = store
                        .get_dataset_by_hash(hash)
                        .await
                        .map_err(|err| {
                            PlanningCtxForSqlTablesWithDepsError::GetDatasetForFunction {
                                table_name: table_name.clone(),
                                reference: reference.clone(),
                                source: err,
                            }
                        })?
                        .ok_or_else(|| {
                            PlanningCtxForSqlTablesWithDepsError::DatasetNotFoundForFunction {
                                table_name: table_name.clone(),
                                reference: reference.clone(),
                            }
                        })?;

                    // Get or create entry for this dataset's UDFs
                    let resolved_udfs = udfs.entry(hash.clone()).or_default();

                    // Get the UDF for this function reference
                    let udf = if function.as_ref() == "eth_call" {
                        store
                            .eth_call_for_dataset(schema, &dataset)
                            .await
                            .map_err(|err| {
                                PlanningCtxForSqlTablesWithDepsError::EthCallUdfCreationForFunction {
                                    table_name: table_name.clone(),
                                    reference: reference.clone(),
                                    source: err,
                                }
                            })?
                            .ok_or_else(|| {
                                PlanningCtxForSqlTablesWithDepsError::EthCallNotAvailable {
                                    table_name: table_name.clone(),
                                    reference: reference.clone(),
                                }
                            })?
                    } else {
                        dataset
                            .function_by_name(schema.to_string(), function, IsolatePool::dummy())
                            .ok_or_else(|| {
                                PlanningCtxForSqlTablesWithDepsError::FunctionNotFoundInDataset {
                                    table_name: table_name.clone(),
                                    function_name: func_ref.to_string(),
                                    reference: reference.clone(),
                                }
                            })?
                    };

                    resolved_udfs.insert(func_ref.to_string(), udf);
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
        udfs: udfs
            .into_values()
            .flat_map(|map| map.into_values())
            .collect(),
    }))
}

/// Resolves all function names from a SQL statement into structured [`FunctionReference`]s.
///
/// Extracts function calls and resolves them into typed [`FunctionReference`] variants.
/// Fails fast if any function name has an invalid format (more than 2 parts).
///
/// Supported formats:
/// - `function` → [`FunctionReference::Bare`] (built-in DataFusion functions)
/// - `schema.function` → [`FunctionReference::Qualified`] (dataset UDFs)
/// - `catalog.schema.function` → Error (not supported)
pub fn resolve_function_references(
    stmt: &Statement,
) -> Result<Vec<FunctionReference>, ResolveFunctionReferencesError> {
    let function_names = all_function_names(stmt)?;

    let mut result = Vec::with_capacity(function_names.len());
    for func_name in function_names {
        let parts: Vec<_> = func_name.split('.').collect();
        let func_ref = match parts.as_slice() {
            [function] => FunctionReference::bare(function.to_string()),
            [schema, function] => {
                FunctionReference::qualified(schema.to_string(), function.to_string())
            }
            _ => {
                return Err(ResolveFunctionReferencesError::InvalidFunctionFormat {
                    function: func_name,
                });
            }
        };
        result.push(func_ref);
    }

    Ok(result)
}

/// A reference to a function that may be bare (unqualified) or qualified with a schema.
///
/// This enum provides a type-safe representation of function references extracted from SQL queries,
/// similar to DataFusion's [`TableReference`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FunctionReference {
    /// An unqualified function reference, e.g., "count", "sum"
    ///
    /// These typically refer to built-in DataFusion functions.
    Bare {
        /// The function name
        function: Arc<str>,
    },
    /// A schema-qualified function reference, e.g., "schema.function"
    ///
    /// These refer to user-defined functions (UDFs) from specific datasets.
    Qualified {
        /// The schema (dataset reference) containing the function
        schema: Arc<str>,
        /// The function name
        function: Arc<str>,
    },
}

impl FunctionReference {
    /// Creates a bare (unqualified) function reference.
    pub fn bare(function: impl Into<Arc<str>>) -> Self {
        Self::Bare {
            function: function.into(),
        }
    }

    /// Creates a qualified function reference.
    pub fn qualified(schema: impl Into<Arc<str>>, function: impl Into<Arc<str>>) -> Self {
        Self::Qualified {
            schema: schema.into(),
            function: function.into(),
        }
    }

    /// Returns the function name, regardless of qualification.
    pub fn function(&self) -> &str {
        match self {
            Self::Bare { function } => function,
            Self::Qualified { function, .. } => function,
        }
    }

    /// Returns the schema name if qualified, `None` otherwise.
    pub fn schema(&self) -> Option<&str> {
        match self {
            Self::Bare { .. } => None,
            Self::Qualified { schema, .. } => Some(schema),
        }
    }
}

impl std::fmt::Display for FunctionReference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bare { function } => write!(f, "{}", function),
            Self::Qualified { schema, function } => write!(f, "{}.{}", schema, function),
        }
    }
}

/// Errors that occur when resolving function references from SQL statements
///
/// This error type is used by [`resolve_function_references`].
#[derive(Debug, thiserror::Error)]
pub enum ResolveFunctionReferencesError {
    /// Failed to extract function names from SQL statement
    ///
    /// This occurs when the underlying function name extraction fails,
    /// typically due to unsupported statement types.
    #[error("Failed to resolve function references: {0}")]
    FunctionReferenceResolution(#[from] AllFunctionNamesError),

    /// Function reference has invalid format (more than 2 parts)
    ///
    /// This occurs when a function name contains 3 or more dot-separated parts,
    /// such as `"catalog.schema.function"`. Currently, only bare functions
    /// (1 part) and qualified functions (2 parts) are supported.
    ///
    /// # Examples
    ///
    /// - Valid: `"count"`, `"eth_mainnet.decode_log"`
    /// - Invalid: `"catalog.schema.function"`, `"a.b.c.d"`
    #[error("Invalid function format (expected 1 or 2 parts, got more): {function}")]
    InvalidFunctionFormat { function: String },
}

/// Returns a list of all function names in the SQL statement.
///
/// Errors in case of some DML statements.
///
/// ## Note
///
/// This is an internal helper function. Use [`parse_function_names`] instead,
/// which provides structured [`FunctionReference`] types.
fn all_function_names(stmt: &Statement) -> Result<Vec<String>, AllFunctionNamesError> {
    use std::ops::ControlFlow;

    use datafusion::sql::sqlparser::ast::{Expr, Function, ObjectNamePart, Visit, Visitor};
    use itertools::Itertools;

    struct FunctionCollector {
        functions: Vec<Function>,
    }

    impl Visitor for FunctionCollector {
        type Break = ();

        fn pre_visit_expr(&mut self, function: &Expr) -> ControlFlow<()> {
            if let Expr::Function(f) = function {
                self.functions.push(f.clone());
            }
            ControlFlow::Continue(())
        }
    }

    let mut collector = FunctionCollector {
        functions: Vec::new(),
    };
    let stmt = match stmt {
        Statement::Statement(statement) => statement,
        Statement::CreateExternalTable(_) | Statement::CopyTo(_) => {
            return Err(AllFunctionNamesError::DmlNotSupported);
        }
        Statement::Explain(explain) => match explain.statement.as_ref() {
            Statement::Statement(statement) => statement,
            _ => return Err(AllFunctionNamesError::UnsupportedStatementInExplain),
        },
    };

    let c = stmt.visit(&mut collector);
    assert!(c.is_continue());

    Ok(collector
        .functions
        .into_iter()
        .map(|f| {
            f.name
                .0
                .into_iter()
                .filter_map(|s| match s {
                    ObjectNamePart::Identifier(ident) => Some(ident.value),
                    ObjectNamePart::Function(_) => None,
                })
                .join(".")
        })
        .collect())
}

/// Errors that occur when extracting function names from SQL statements
///
/// This error type is used by [`all_function_names`].
#[derive(Debug, thiserror::Error)]
pub enum AllFunctionNamesError {
    /// DML statements are not supported for function name extraction
    ///
    /// This occurs when attempting to extract function names from DML statements
    /// like `CreateExternalTable` or `CopyTo`. These statement types are not
    /// supported because they represent data manipulation operations rather than
    /// queryable SQL statements.
    ///
    /// Function name extraction is only meaningful for query statements (SELECT)
    /// and their variants (e.g., within EXPLAIN).
    #[error("DML statements (CreateExternalTable, CopyTo) are not supported")]
    DmlNotSupported,

    /// Unsupported statement type within EXPLAIN
    ///
    /// This occurs when an EXPLAIN statement contains a nested statement type
    /// that is not supported for function name extraction. Only regular SQL
    /// statements (SELECT, etc.) are supported within EXPLAIN.
    ///
    /// Common causes:
    /// - EXPLAIN wrapping a DML statement (CreateExternalTable, CopyTo)
    /// - EXPLAIN wrapping another EXPLAIN statement
    #[error("Unsupported statement type in EXPLAIN")]
    UnsupportedStatementInExplain,
}

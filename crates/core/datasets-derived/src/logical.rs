//! Derived dataset utilities
//!
//! This module provides utilities for working with derived datasets, including:
//! - Converting manifest representations to logical datasets
//! - Validating derived dataset manifests
//! - Sorting tables by their SQL dependencies
//!
//! ## Key Functions
//!
//! - [`dataset()`] - Convert a derived dataset manifest into a logical dataset
//! - [`validate()`] - Validate a derived dataset manifest with comprehensive checks
//! - [`sort_tables_by_dependencies()`] - Sort tables by their SQL dependencies

use std::collections::{BTreeMap, BTreeSet};

use common::{
    BlockNum, BoxError, Dataset, Table as LogicalTable,
    catalog::{
        dataset_access::DatasetAccess,
        logical::{Function as LogicalFunction, FunctionSource as LogicalFunctionSource},
    },
    query_context::Error as QueryContextErr,
    sql::{
        FunctionReference, ResolveFunctionReferencesError, ResolveTableReferencesError,
        TableReference, resolve_function_references, resolve_table_references,
    },
    utils::dfs,
};
use datafusion::sql::parser;
use datasets_common::{
    deps::alias::{DepAlias, DepAliasError, DepAliasOrSelfRef, DepAliasOrSelfRefError},
    hash::Hash,
    hash_reference::HashReference,
    table_name::TableName,
};
use js_runtime::isolate_pool::IsolatePool;

use crate::{
    DerivedDatasetKind, Manifest,
    catalog::{
        PlanningCtxForSqlTablesWithDepsError, planning_ctx_for_sql_tables_with_deps_and_funcs,
    },
    manifest::{TableInput, View},
};

/// Convert a derived dataset manifest into a logical dataset representation.
///
/// This function transforms a derived dataset manifest with its tables, functions, and metadata
/// into the internal `Dataset` structure used by the query engine. Dataset identity (namespace,
/// name, version, manifest_hash) must be provided externally as they are not part of the manifest.
pub fn dataset(manifest_hash: Hash, manifest: Manifest) -> Result<Dataset, DatasetError> {
    let queries = {
        let mut queries = BTreeMap::new();
        for (table_name, table) in &manifest.tables {
            let TableInput::View(query) = &table.input;
            let query = common::sql::parse(&query.sql).map_err(|err| DatasetError::ParseSql {
                table_name: table_name.clone(),
                source: err,
            })?;
            queries.insert(table_name.clone(), query);
        }
        queries
    };

    // Convert manifest tables into logical tables
    let unsorted_tables: Vec<LogicalTable> = manifest
        .tables
        .into_iter()
        .map(|(name, table)| {
            LogicalTable::new(name, table.schema.arrow.into(), table.network, vec![])
        })
        .collect();
    let tables = sort_tables_by_dependencies(unsorted_tables, &queries)
        .map_err(DatasetError::SortTableDependencies)?;

    // Convert manifest functions into logical functions
    let functions = manifest
        .functions
        .into_iter()
        .map(|(name, f)| LogicalFunction {
            name: name.into_inner(),
            input_types: f.input_types.into_iter().map(|dt| dt.0).collect(),
            output_type: f.output_type.0,
            source: LogicalFunctionSource {
                source: f.source.source,
                filename: f.source.filename,
            },
        })
        .collect();

    Ok(Dataset {
        manifest_hash,
        dependencies: manifest.dependencies,
        kind: DerivedDatasetKind.to_string(),
        network: None,
        start_block: manifest.start_block,
        finalized_blocks_only: false,
        tables,
        functions,
    })
}

/// Errors that occur when converting a derived dataset manifest to a logical dataset
///
/// This error type is used by the `dataset()` function.
#[derive(Debug, thiserror::Error)]
pub enum DatasetError {
    /// Failed to parse SQL query in table definition
    ///
    /// This occurs when:
    /// - The provided SQL query has invalid syntax
    /// - Multiple statements provided (only single statement allowed)
    /// - Query parsing fails for other reasons
    ///
    /// Common causes:
    /// - Malformed SQL syntax
    /// - Unsupported SQL features
    /// - Multiple semicolon-separated statements (not allowed)
    #[error("Failed to parse SQL query for table '{table_name}'")]
    ParseSql {
        table_name: TableName,
        #[source]
        source: common::sql::ParseSqlError,
    },

    /// Failed to sort tables by their SQL dependencies
    ///
    /// This occurs when:
    /// - Circular dependencies detected between tables
    /// - Table dependency resolution fails
    /// - Invalid table references in dependency graph
    ///
    /// This is typically caused by circular table references in SQL queries
    /// or invalid table names in the dependency graph.
    #[error("Failed to sort tables by dependencies")]
    SortTableDependencies(#[source] SortTablesByDependenciesError),
}

/// Sort tables by their SQL dependencies using topological ordering.
///
/// Analyzes table queries to determine dependencies and returns tables in dependency order.
/// Tables with no dependencies come first, followed by tables that depend on them.
pub fn sort_tables_by_dependencies(
    tables: Vec<LogicalTable>,
    queries: &BTreeMap<TableName, parser::Statement>,
) -> Result<Vec<LogicalTable>, SortTablesByDependenciesError> {
    // Map of table name -> Table
    let table_map = tables
        .into_iter()
        .map(|t| (t.name().clone(), t))
        .collect::<BTreeMap<TableName, LogicalTable>>();

    // Dependency map: table -> [tables it depends on]
    let mut deps: BTreeMap<TableName, Vec<TableName>> = BTreeMap::new();

    // Initialize empty deps with all tables
    for table_name in table_map.keys() {
        deps.insert(table_name.clone(), Vec::new());
    }

    for (table_name, query) in queries {
        let table_refs = resolve_table_references::<String>(query).map_err(|err| {
            SortTablesByDependenciesError::ResolveTableReferences {
                table_name: table_name.clone(),
                source: err,
            }
        })?;

        // Filter to only include dependencies within the same dataset
        let mut table_deps: Vec<TableName> = vec![];
        for table_ref in table_refs {
            match &table_ref {
                TableReference::Bare { table } if table.as_ref() != table_name => {
                    // Unqualified reference is assumed to be to a table in the same dataset
                    if table_map.contains_key(table.as_ref()) {
                        table_deps.push(table.as_ref().clone());
                    }
                }
                _ => {
                    // Reference to external dataset or self-reference, ignore
                }
            }
        }

        // Update the existing entry with dependencies
        if let Some(existing_deps) = deps.get_mut(table_name) {
            *existing_deps = table_deps;
        }
    }

    let sorted_names =
        table_dependency_sort(deps).map_err(SortTablesByDependenciesError::TopologicalSort)?;

    let mut sorted_tables = Vec::new();
    for name in sorted_names {
        if let Some(table) = table_map.get(&name) {
            sorted_tables.push(table.clone());
        }
    }

    Ok(sorted_tables)
}

/// Errors that occur when sorting tables by their SQL dependencies
///
/// This error type is used by the `sort_tables_by_dependencies()` function.
#[derive(Debug, thiserror::Error)]
pub enum SortTablesByDependenciesError {
    /// Failed to resolve table references from SQL query
    ///
    /// This occurs when:
    /// - The SQL query contains invalid table references
    /// - Table names don't conform to SQL identifier rules
    /// - Catalog-qualified tables are used (not supported)
    ///
    /// Common causes:
    /// - Invalid table name format (e.g., special characters, too long)
    /// - Three-part table names (catalog.schema.table)
    /// - Malformed SQL syntax in table references
    #[error("Failed to resolve table references in table '{table_name}': {source}")]
    ResolveTableReferences {
        table_name: TableName,
        #[source]
        source: ResolveTableReferencesError,
    },

    /// Failed to perform topological sort on table dependencies
    ///
    /// This occurs when:
    /// - Circular dependencies exist between tables
    /// - The dependency graph cannot be ordered topologically
    ///
    /// The most common cause is circular table dependencies in SQL queries.
    #[error("Failed to perform topological sort on table dependencies")]
    TopologicalSort(#[source] TableDependencySortError),
}

/// Topological sort for table dependencies.
///
/// Uses depth-first search to order tables such that each table comes after
/// all tables it depends on. Detects circular dependencies.
fn table_dependency_sort(
    deps: BTreeMap<TableName, Vec<TableName>>,
) -> Result<Vec<TableName>, TableDependencySortError> {
    let nodes: BTreeSet<&TableName> = deps.keys().collect();
    let mut ordered: Vec<TableName> = Vec::new();
    let mut visited: BTreeSet<&TableName> = BTreeSet::new();
    let mut visiting: BTreeSet<&TableName> = BTreeSet::new();

    for node in nodes {
        if !visited.contains(node) {
            dfs(node, &deps, &mut ordered, &mut visited, &mut visiting).map_err(|err| {
                TableDependencySortError {
                    table_name: err.node,
                }
            })?;
        }
    }

    Ok(ordered)
}

/// Error when circular dependency is detected during topological sorting
///
/// This occurs when tables have circular references in their SQL queries,
/// creating a dependency cycle that cannot be resolved. For example:
/// - Table A depends on Table B
/// - Table B depends on Table C
/// - Table C depends on Table A (creates cycle)
///
/// The topological sort algorithm detects this during depth-first search
/// traversal when it encounters a node that is currently being visited.
///
/// To resolve this error:
/// - Review table SQL queries to identify the circular reference chain
/// - Refactor queries to break the dependency cycle
/// - Consider splitting tables or using different query patterns
#[derive(Debug, thiserror::Error)]
#[error("Circular dependency detected in table definitions at table '{table_name}'")]
pub struct TableDependencySortError {
    /// The table where the circular dependency was detected
    pub table_name: TableName,
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

/// Validates a derived dataset manifest with comprehensive checks.
///
/// This function performs deep validation of a manifest by:
/// 1. Parsing SQL queries from all table definitions
/// 2. Resolving all dependency references to their content hashes
/// 3. Extracting table and function references from SQL
/// 4. Building a planning context with actual dataset schemas
/// 5. Validating that all referenced tables and functions exist
///
/// Validation checks include:
/// - SQL syntax validity for all table queries
/// - All referenced datasets exist in the store
/// - All tables used in SQL exist in their datasets
/// - All functions used in SQL exist in their datasets
/// - Table schemas are compatible with SQL queries
pub async fn validate(
    manifest: &Manifest,
    store: &impl DatasetAccess,
) -> Result<(), ManifestValidationError> {
    // Step 1: Resolve all dependencies to HashReference
    // This must happen first to ensure all dependencies exist before parsing SQL
    let mut dependencies: BTreeMap<DepAlias, HashReference> = BTreeMap::new();

    for (alias, dep_reference) in &manifest.dependencies {
        // Convert DepReference to Reference for resolution
        let reference = dep_reference.to_reference();

        // Resolve reference to its manifest hash reference
        // This handles all revision types (Version, Hash)
        let reference = store
            .resolve_revision(&reference)
            .await
            .map_err(|err| ManifestValidationError::DependencyResolution {
                alias: alias.to_string(),
                reference: reference.to_string(),
                source: err,
            })?
            .ok_or_else(|| ManifestValidationError::DependencyResolution {
                alias: alias.to_string(),
                reference: reference.to_string(),
                source: format!("Dependency '{}' not found", reference).into(),
            })?;

        dependencies.insert(alias.clone(), reference);
    }

    // Check if the start block is before the earliest available block of the dependencies
    if let Some(dataset_start_block) = &manifest.start_block {
        for (alias, hash_ref) in &dependencies {
            let dataset = store.get_dataset(hash_ref).await.map_err(|err| {
                ManifestValidationError::FetchDependencyDataset {
                    alias: alias.to_string(),
                    source: err,
                }
            })?;

            if let Some(dep_start_block) = dataset.start_block
                && *dataset_start_block < dep_start_block
            {
                return Err(ManifestValidationError::StartBlockBeforeDependencies {
                    dataset_start_block: *dataset_start_block,
                    dependency_earliest_block: dep_start_block,
                });
            }
        }
    }

    // Step 2: Parse all SQL queries and extract references
    let mut references: TableReferencesMap = BTreeMap::new();

    for (table_name, table) in &manifest.tables {
        let TableInput::View(View { sql }) = &table.input;

        // Parse SQL (validates single statement)
        let stmt =
            common::sql::parse(sql).map_err(|err| ManifestValidationError::InvalidTableSql {
                table_name: table_name.clone(),
                source: err,
            })?;

        // Extract table references
        let table_refs = resolve_table_references::<DepAlias>(&stmt).map_err(|err| match &err {
            ResolveTableReferencesError::InvalidTableName { .. } => {
                ManifestValidationError::InvalidTableName(err)
            }
            ResolveTableReferencesError::CatalogQualifiedTable { .. } => {
                ManifestValidationError::CatalogQualifiedTableInSql {
                    table_name: table_name.clone(),
                    source: err,
                }
            }
            _ => ManifestValidationError::TableReferenceResolution {
                table_name: table_name.clone(),
                source: err,
            },
        })?;

        // Extract function references (supports both external deps and self-references)
        let func_refs = resolve_function_references::<DepAliasOrSelfRef>(&stmt).map_err(|err| {
            ManifestValidationError::FunctionReferenceResolution {
                table_name: table_name.clone(),
                source: err,
            }
        })?;

        references.insert(table_name.clone(), (table_refs, func_refs));
    }

    // Step 3: Create planning context to validate all table and function references
    // This validates:
    // - All table references resolve to existing tables in dependencies
    // - All function references resolve to existing functions in dependencies
    // - Bare function references can be created as UDFs or are assumed to be built-ins
    // - Table references use valid dataset aliases from dependencies
    // - Schema compatibility across dependencies
    let planning_ctx = planning_ctx_for_sql_tables_with_deps_and_funcs(
        store,
        references,
        dependencies,
        manifest.functions.clone(),
        IsolatePool::dummy(), // For manifest validation only (no JS execution)
    )
    .await
    .map_err(|err| match &err {
        PlanningCtxForSqlTablesWithDepsError::UnqualifiedTable { .. } => {
            ManifestValidationError::UnqualifiedTable(err)
        }
        PlanningCtxForSqlTablesWithDepsError::GetDatasetForTableRef { .. } => {
            ManifestValidationError::GetDataset(err)
        }
        PlanningCtxForSqlTablesWithDepsError::GetDatasetForFunction { .. } => {
            ManifestValidationError::GetDataset(err)
        }
        PlanningCtxForSqlTablesWithDepsError::EthCallUdfCreationForFunction { .. } => {
            ManifestValidationError::EthCallUdfCreation(err)
        }
        PlanningCtxForSqlTablesWithDepsError::DependencyAliasNotFoundForTableRef { .. } => {
            ManifestValidationError::DependencyAliasNotFound(err)
        }
        PlanningCtxForSqlTablesWithDepsError::DependencyAliasNotFoundForFunctionRef { .. } => {
            ManifestValidationError::DependencyAliasNotFound(err)
        }
        PlanningCtxForSqlTablesWithDepsError::TableNotFoundInDataset { .. } => {
            ManifestValidationError::TableNotFoundInDataset(err)
        }
        PlanningCtxForSqlTablesWithDepsError::FunctionNotFoundInDataset { .. } => {
            ManifestValidationError::FunctionNotFoundInDataset(err)
        }
        PlanningCtxForSqlTablesWithDepsError::EthCallNotAvailable { .. } => {
            ManifestValidationError::EthCallNotAvailable(err)
        }
    })?;

    // Step 4: Validate that all table SQL queries are incremental
    // Incremental processing is required for derived datasets to efficiently update
    // as new blocks arrive. This check ensures no non-incremental operations are used.
    for (table_name, table) in &manifest.tables {
        let TableInput::View(View { sql }) = &table.input;

        // Parse SQL (already validated in Step 2, but need Statement for planning)
        let stmt =
            common::sql::parse(sql).map_err(|err| ManifestValidationError::InvalidTableSql {
                table_name: table_name.clone(),
                source: err,
            })?;

        // Plan the SQL query to a logical plan
        let plan = planning_ctx.plan_sql(stmt).await.map_err(|err| {
            ManifestValidationError::SqlPlanningError {
                table_name: table_name.clone(),
                source: err,
            }
        })?;

        // Validate that the plan can be processed incrementally
        // This checks for non-incremental operations like aggregations, sorts, limits, outer joins, etc.
        plan.is_incremental()
            .map_err(|err| ManifestValidationError::NonIncrementalSql {
                table_name: table_name.clone(),
                source: err,
            })?;
    }

    Ok(())
}

/// Errors that occur during derived dataset manifest validation
#[derive(Debug, thiserror::Error)]
pub enum ManifestValidationError {
    /// Invalid SQL query in table definition
    ///
    /// This occurs when:
    /// - The provided SQL query has invalid syntax
    /// - Multiple statements provided (only single statement allowed)
    /// - Query parsing fails for other reasons
    #[error("Invalid SQL query for table '{table_name}': {source}")]
    InvalidTableSql {
        table_name: TableName,
        #[source]
        source: common::sql::ParseSqlError,
    },

    /// Failed to resolve table references from SQL query
    #[error("Failed to resolve table references in table '{table_name}': {source}")]
    TableReferenceResolution {
        table_name: TableName,
        #[source]
        source: ResolveTableReferencesError<DepAliasError>,
    },

    /// Failed to resolve function references from SQL query
    #[error("Failed to resolve function references in table '{table_name}': {source}")]
    FunctionReferenceResolution {
        table_name: TableName,
        #[source]
        source: ResolveFunctionReferencesError<DepAliasOrSelfRefError>,
    },

    /// Failed to resolve dependency reference to hash
    ///
    /// This occurs when resolving a dependency reference fails:
    /// - Invalid reference format
    /// - Dependency not found in the dataset store
    /// - Storage backend errors when reading the dependency
    #[error("Failed to resolve dependency '{alias}' ({reference}): {source}")]
    DependencyResolution {
        alias: String,
        reference: String,
        #[source]
        source: BoxError,
    },

    /// Failed to fetch dependency dataset for start_block validation
    ///
    /// This occurs when fetching the dataset definition for a dependency fails during
    /// start_block validation. The dataset reference was resolved successfully, but
    /// loading the actual dataset from the store failed.
    #[error("Failed to fetch dependency '{alias}' for start_block validation: {source}")]
    FetchDependencyDataset {
        alias: String,
        #[source]
        source: BoxError,
    },

    /// Catalog-qualified table reference in SQL query
    ///
    /// Only dataset-qualified tables are supported (e.g., `dataset.table`).
    /// Catalog-qualified tables (e.g., `catalog.schema.table`) are not supported.
    /// This error occurs during SQL parsing when a 3-part table reference is detected.
    #[error("Catalog-qualified table reference in table '{table_name}': {source}")]
    CatalogQualifiedTableInSql {
        table_name: TableName,
        #[source]
        source: ResolveTableReferencesError<DepAliasError>,
    },

    /// Unqualified table reference
    ///
    /// All tables must be qualified with a dataset reference in the schema portion.
    /// Unqualified tables (e.g., just `table_name`) are not allowed.
    #[error("Unqualified table reference: {0}")]
    UnqualifiedTable(#[source] PlanningCtxForSqlTablesWithDepsError),

    /// Invalid table name
    ///
    /// Table name does not conform to SQL identifier rules (must start with letter/underscore,
    /// contain only alphanumeric/underscore/dollar, and be <= 63 bytes).
    #[error("Invalid table name in SQL query: {0}")]
    InvalidTableName(#[source] ResolveTableReferencesError<DepAliasError>),

    /// Dataset reference not found
    ///
    /// The referenced dataset does not exist in the store.
    #[error("Dataset not found: {0}")]
    DatasetNotFound(#[source] PlanningCtxForSqlTablesWithDepsError),

    /// Failed to retrieve dataset from store
    ///
    /// This occurs when loading a dataset definition fails due to:
    /// - Invalid or corrupted manifest
    /// - Unsupported dataset kind
    /// - Storage backend errors
    #[error("Failed to retrieve dataset from store: {0}")]
    GetDataset(#[source] PlanningCtxForSqlTablesWithDepsError),

    /// Failed to create ETH call UDF
    ///
    /// This occurs when creating the eth_call user-defined function fails.
    #[error("Failed to create ETH call UDF: {0}")]
    EthCallUdfCreation(#[source] PlanningCtxForSqlTablesWithDepsError),

    /// Table not found in dataset
    ///
    /// The referenced table does not exist in the dataset.
    #[error("Table not found in dataset: {0}")]
    TableNotFoundInDataset(#[source] PlanningCtxForSqlTablesWithDepsError),

    /// Function not found in dataset
    ///
    /// The referenced function does not exist in the dataset.
    #[error("Function not found in dataset: {0}")]
    FunctionNotFoundInDataset(#[source] PlanningCtxForSqlTablesWithDepsError),

    /// eth_call function not available
    ///
    /// The eth_call function is not available for the referenced dataset.
    #[error("eth_call function not available: {0}")]
    EthCallNotAvailable(#[source] PlanningCtxForSqlTablesWithDepsError),

    /// Invalid dependency alias
    ///
    /// The dependency alias does not conform to alias rules (must start with letter,
    /// contain only alphanumeric/underscore, and be <= 63 bytes).
    #[error("Invalid dependency alias: {0}")]
    InvalidDependencyAlias(#[source] PlanningCtxForSqlTablesWithDepsError),

    /// Dependency alias not found
    ///
    /// A table reference uses an alias that was not provided in the dependencies map.
    #[error("Dependency alias not found: {0}")]
    DependencyAliasNotFound(#[source] PlanningCtxForSqlTablesWithDepsError),

    /// Non-incremental SQL operation in table query
    ///
    /// This occurs when a table's SQL query contains operations that cannot be
    /// processed incrementally (aggregations, sorts, limits, outer joins, window
    /// functions, distinct, or recursive queries).
    ///
    /// Incremental processing is required for derived datasets to efficiently
    /// update as new blocks arrive. Non-incremental operations would require
    /// recomputing the entire table on each update.
    ///
    /// Common non-incremental operations:
    /// - Aggregations: COUNT, SUM, MAX, MIN, AVG, GROUP BY
    /// - Sorting: ORDER BY
    /// - Deduplication: DISTINCT
    /// - Limiting: LIMIT, TOP
    /// - Outer joins: LEFT JOIN, RIGHT JOIN, FULL JOIN
    /// - Window functions: ROW_NUMBER, RANK, LAG, LEAD
    /// - Recursive queries: WITH RECURSIVE
    ///
    /// To resolve this error:
    /// - Remove aggregations and use raw data or pre-aggregated sources
    /// - Replace outer joins with inner joins
    /// - Remove sorting, limiting, and window functions
    /// - Ensure queries only use incremental operations (projection, filter, inner join, union)
    #[error("Table '{table_name}' contains non-incremental SQL: {source}")]
    NonIncrementalSql {
        table_name: TableName,
        #[source]
        source: BoxError,
    },

    /// SQL query planning failed
    ///
    /// This error occurs when DataFusion fails to create a logical plan from
    /// the SQL query defined for a derived dataset table. Common causes include:
    /// - Invalid SQL syntax that passed parsing but failed planning
    /// - References to non-existent tables or columns
    /// - Type mismatches in expressions
    /// - Unsupported SQL features
    #[error("Failed to plan query for table '{table_name}': {source}")]
    SqlPlanningError {
        table_name: TableName,
        #[source]
        source: QueryContextErr,
    },

    /// Start block before dependencies
    ///
    /// This occurs when the start block of the derived dataset is before
    /// the earliest block of one of the dependencies.
    #[error(
        "derived dataset start_block ({dataset_start_block}) is before dependency's earliest available block ({dependency_earliest_block})"
    )]
    StartBlockBeforeDependencies {
        dataset_start_block: BlockNum,
        dependency_earliest_block: BlockNum,
    },
}

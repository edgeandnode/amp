//! Derived dataset
//!
//! TODO: Move to datasets-derived crate

use std::collections::{BTreeMap, BTreeSet};

use datafusion::sql::{TableReference, parser, resolve::resolve_table_references};
use datasets_common::{fqn::FullyQualifiedName, hash::Hash, table_name::TableName};
use datasets_derived::{
    DerivedDatasetKind, Manifest,
    dep_alias::DepAlias,
    manifest::{TableInput, View},
};

use crate::{
    BoxError, Dataset, Table as LogicalTable,
    catalog::{
        dataset_access::DatasetAccess,
        errors::PlanningCtxForSqlTablesWithDepsError,
        logical::{Function as LogicalFunction, FunctionSource as LogicalFunctionSource},
        sql::{
            FunctionReference, ResolveFunctionReferencesError,
            planning_ctx_for_sql_tables_with_deps, resolve_function_references,
        },
    },
    query_context::{self, parse_sql},
    utils::dfs,
};

/// Extract all SQL queries from table views.
pub fn queries(
    manifest: &Manifest,
) -> Result<BTreeMap<TableName, parser::Statement>, query_context::Error> {
    let mut queries = BTreeMap::new();
    for (table_name, table) in &manifest.tables {
        let TableInput::View(query) = &table.input;
        let query = parse_sql(&query.sql)?;
        queries.insert(table_name.clone(), query);
    }
    Ok(queries)
}

/// Convert a derived dataset manifest into a logical dataset representation.
///
/// This function transforms a derived dataset manifest with its tables, functions, and metadata
/// into the internal `Dataset` structure used by the query engine. Dataset identity (namespace,
/// name, version, manifest_hash) must be provided externally as they are not part of the manifest.
pub fn dataset(
    manifest_hash: datasets_common::hash::Hash,
    manifest: Manifest,
) -> Result<Dataset, BoxError> {
    let queries = {
        let mut queries = BTreeMap::new();
        for (table_name, table) in &manifest.tables {
            let TableInput::View(query) = &table.input;
            let query = parse_sql(&query.sql)?;
            queries.insert(table_name.clone(), query);
        }
        queries
    };

    // Convert manifest tables into logical tables
    let unsorted_tables: Result<Vec<_>, _> = manifest
        .tables
        .into_iter()
        .map(|(name, table)| {
            LogicalTable::new(name, table.schema.arrow.into(), table.network, vec![])
        })
        .collect();
    let unsorted_tables = unsorted_tables?;
    let tables = sort_tables_by_dependencies(unsorted_tables, &queries)?;

    // Convert manifest functions into logical functions
    let functions = manifest
        .functions
        .into_iter()
        .map(|(name, f)| LogicalFunction {
            name,
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
        start_block: None,
        finalized_blocks_only: false,
        tables,
        functions,
    })
}

/// Sort tables by their SQL dependencies using topological ordering.
///
/// Analyzes table queries to determine dependencies and returns tables in dependency order.
/// Tables with no dependencies come first, followed by tables that depend on them.
pub fn sort_tables_by_dependencies(
    tables: Vec<LogicalTable>,
    queries: &BTreeMap<TableName, parser::Statement>,
) -> Result<Vec<LogicalTable>, BoxError> {
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
        let (table_refs, _) = resolve_table_references(query, true)?;

        // Filter to only include dependencies within the same dataset
        let mut table_deps: Vec<TableName> = vec![];
        for table_ref in table_refs {
            match (table_ref.schema(), table_ref.table()) {
                (None, table) if table != table_name => {
                    // Unqualified reference is assumed to be to a table in the same dataset
                    #[expect(clippy::collapsible_if)]
                    if let Ok(dep_name) = table.parse::<TableName>() {
                        if table_map.contains_key(&dep_name) {
                            table_deps.push(dep_name);
                        }
                    }
                }
                _ => {
                    // Reference to external dataset, ignore
                }
            }
        }

        // Update the existing entry with dependencies
        if let Some(existing_deps) = deps.get_mut(table_name) {
            *existing_deps = table_deps;
        }
    }

    let sorted_names = table_dependency_sort(deps)?;

    let mut sorted_tables = Vec::new();
    for name in sorted_names {
        if let Some(table) = table_map.get(&name) {
            sorted_tables.push(table.clone());
        }
    }

    Ok(sorted_tables)
}

/// Topological sort for table dependencies.
///
/// Uses depth-first search to order tables such that each table comes after
/// all tables it depends on. Detects circular dependencies.
fn table_dependency_sort(
    deps: BTreeMap<TableName, Vec<TableName>>,
) -> Result<Vec<TableName>, BoxError> {
    let nodes: BTreeSet<&TableName> = deps.keys().collect();
    let mut ordered: Vec<TableName> = Vec::new();
    let mut visited: BTreeSet<&TableName> = BTreeSet::new();
    let mut visiting: BTreeSet<&TableName> = BTreeSet::new();

    for node in nodes {
        if !visited.contains(node) {
            dfs(node, &deps, &mut ordered, &mut visited, &mut visiting)?;
        }
    }

    Ok(ordered)
}

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
    // Step 1: Resolve all dependencies to (FQN, Hash) pairs
    // This must happen first to ensure all dependencies exist before parsing SQL
    let mut dependencies: BTreeMap<DepAlias, (FullyQualifiedName, Hash)> = BTreeMap::new();

    for (alias, dep_reference) in &manifest.dependencies {
        // Convert DepReference to Reference for resolution
        let reference = dep_reference.to_reference();

        // Resolve reference to its manifest hash
        // This handles all revision types (Version, Hash)
        let hash = store
            .resolve_dataset_reference(&reference)
            .await
            .map_err(|err| ManifestValidationError::DependencyResolution {
                alias: alias.to_string(),
                reference: reference.to_string(),
                source: err,
            })?
            .ok_or_else(|| ManifestValidationError::DependencyNotFound {
                alias: alias.to_string(),
                reference: reference.to_string(),
            })?;

        let fqn = reference.into_fqn();
        dependencies.insert(alias.clone(), (fqn, hash));
    }

    // Step 2: Parse all SQL queries and extract references
    let mut references: BTreeMap<TableName, (Vec<TableReference>, Vec<FunctionReference>)> =
        BTreeMap::new();

    for (table_name, table) in &manifest.tables {
        let TableInput::View(View { sql }) = &table.input;

        // Parse SQL (validates single statement)
        let stmt = parse_sql(sql).map_err(|err| ManifestValidationError::InvalidTableSql {
            table_name: table_name.clone(),
            source: err,
        })?;

        // Extract table references
        let (table_refs, _) = resolve_table_references(&stmt, true).map_err(|err| {
            ManifestValidationError::TableReferenceResolution {
                table_name: table_name.clone(),
                source: err,
            }
        })?;

        // Extract function references
        let func_refs = resolve_function_references(&stmt).map_err(|err| {
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
    // - Table references use valid dataset aliases from dependencies
    // - Schema compatibility across dependencies
    planning_ctx_for_sql_tables_with_deps(store, references, dependencies)
        .await
        .map_err(|err| match &err {
            PlanningCtxForSqlTablesWithDepsError::CatalogQualifiedTable { .. } => {
                ManifestValidationError::CatalogQualifiedTable(err)
            }
            PlanningCtxForSqlTablesWithDepsError::UnqualifiedTable { .. } => {
                ManifestValidationError::UnqualifiedTable(err)
            }
            PlanningCtxForSqlTablesWithDepsError::InvalidTableName { .. } => {
                ManifestValidationError::InvalidTableName(err)
            }
            PlanningCtxForSqlTablesWithDepsError::InvalidDependencyAliasForTableRef { .. } => {
                ManifestValidationError::InvalidDependencyAliasForTableRef(err)
            }
            PlanningCtxForSqlTablesWithDepsError::InvalidDependencyAliasForFunctionRef {
                ..
            } => ManifestValidationError::InvalidDependencyAliasForFunctionRef(err),
            PlanningCtxForSqlTablesWithDepsError::DatasetNotFoundForTableRef { .. } => {
                ManifestValidationError::DatasetNotFound(err)
            }
            PlanningCtxForSqlTablesWithDepsError::DatasetNotFoundForFunction { .. } => {
                ManifestValidationError::DatasetNotFound(err)
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
            PlanningCtxForSqlTablesWithDepsError::DependencyAliasNotFoundForFunctionRef {
                ..
            } => ManifestValidationError::DependencyAliasNotFound(err),
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
        source: query_context::Error,
    },

    /// Failed to resolve table references from SQL query
    #[error("Failed to resolve table references in table '{table_name}': {source}")]
    TableReferenceResolution {
        table_name: TableName,
        #[source]
        source: datafusion::error::DataFusionError,
    },

    /// Failed to resolve function references from SQL query
    #[error("Failed to resolve function references in table '{table_name}': {source}")]
    FunctionReferenceResolution {
        table_name: TableName,
        #[source]
        source: ResolveFunctionReferencesError,
    },

    /// Dependency declared in manifest but not found in store
    #[error("Dependency '{alias}' ({reference}) not found in dataset store")]
    DependencyNotFound { alias: String, reference: String },

    /// Failed to resolve dependency reference to hash
    #[error("Failed to resolve dependency '{alias}' ({reference}): {source}")]
    DependencyResolution {
        alias: String,
        reference: String,
        #[source]
        source: BoxError,
    },

    /// Catalog-qualified table reference not supported
    ///
    /// Only dataset-qualified tables are supported (e.g., `dataset.table`).
    /// Catalog-qualified tables (e.g., `catalog.schema.table`) are not supported.
    #[error("Catalog-qualified table reference not supported: {0}")]
    CatalogQualifiedTable(#[source] PlanningCtxForSqlTablesWithDepsError),

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
    #[error("Invalid table name: {0}")]
    InvalidTableName(#[source] PlanningCtxForSqlTablesWithDepsError),

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

    /// Invalid dependency alias in table reference
    ///
    /// The dependency alias in a table reference does not conform to alias rules
    /// (must start with letter, contain only alphanumeric/underscore, and be <= 63 bytes).
    #[error("Invalid dependency alias in table reference: {0}")]
    InvalidDependencyAliasForTableRef(#[source] PlanningCtxForSqlTablesWithDepsError),

    /// Invalid dependency alias in function reference
    ///
    /// The dependency alias in a function reference does not conform to alias rules
    /// (must start with letter, contain only alphanumeric/underscore, and be <= 63 bytes).
    #[error("Invalid dependency alias in function reference: {0}")]
    InvalidDependencyAliasForFunctionRef(#[source] PlanningCtxForSqlTablesWithDepsError),

    /// Dependency alias not found
    ///
    /// A table reference uses an alias that was not provided in the dependencies map.
    #[error("Dependency alias not found: {0}")]
    DependencyAliasNotFound(#[source] PlanningCtxForSqlTablesWithDepsError),
}

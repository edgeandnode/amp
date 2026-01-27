//! Derived dataset utilities
//!
//! This module provides utilities for working with derived datasets, including:
//! - Converting manifest representations to logical datasets
//! - Sorting tables by their SQL dependencies
//!
//! ## Key Functions
//!
//! - [`dataset()`] - Convert a derived dataset manifest into a logical dataset
//! - [`sort_tables_by_dependencies()`] - Sort tables by their SQL dependencies

use std::collections::{BTreeMap, BTreeSet};

use common::{
    catalog::logical::{Function as LogicalFunction, FunctionSource as LogicalFunctionSource},
    sql::{ResolveTableReferencesError, TableReference, resolve_table_references},
};
use datafusion::sql::parser;
use datasets_common::{
    dataset::Table as LogicalTable, hash_reference::HashReference, table_name::TableName,
};

use crate::{DerivedDatasetKind, Manifest, deps, manifest::TableInput};

/// Convert a derived dataset manifest into a logical dataset representation.
///
/// This function transforms a derived dataset manifest with its tables, functions, and metadata
/// into the internal `Dataset` structure used by the query engine. Dataset identity (namespace,
/// name, version, hash reference) must be provided externally as they are not part of the manifest.
pub fn dataset(
    reference: HashReference,
    manifest: Manifest,
) -> Result<crate::dataset::Dataset, DatasetError> {
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

    Ok(crate::dataset::Dataset {
        reference,
        dependencies: manifest.dependencies,
        kind: DerivedDatasetKind,
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
            deps::dfs(node, &deps, &mut ordered, &mut visited, &mut visiting).map_err(|err| {
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

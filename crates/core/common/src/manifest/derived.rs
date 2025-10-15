//! Derived dataset
//!
//! This module provides derived datasets that transform and combine data from existing datasets using SQL queries.
//! Derived datasets replace the legacy SQL dataset format, providing versioned, dependency-aware dataset
//! definitions with explicit schemas and functions.

use std::collections::{BTreeMap, BTreeSet};

use datafusion::sql::{parser, resolve::resolve_table_references};
use datasets_derived::{DerivedDatasetKind, Manifest, manifest::TableInput};

use crate::{
    BoxError, Dataset, Table as LogicalTable,
    catalog::logical::{Function as LogicalFunction, FunctionSource as LogicalFunctionSource},
    query_context::{self, parse_sql},
    utils::dfs,
};

/// Extract all SQL queries from table views.
pub fn queries(
    manifest: &Manifest,
) -> Result<BTreeMap<String, parser::Statement>, query_context::Error> {
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
/// into the internal `Dataset` structure used by the query engine.
pub fn dataset(manifest: Manifest) -> Result<Dataset, BoxError> {
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
    let unsorted_tables = manifest
        .tables
        .into_iter()
        .map(|(name, table)| {
            LogicalTable::new(name, table.schema.arrow.into(), table.network, vec![])
        })
        .collect();
    let tables = sort_tables_by_dependencies(&manifest.name, unsorted_tables, &queries)?;

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
        kind: DerivedDatasetKind.to_string(),
        network: None,
        name: manifest.name,
        version: Some(manifest.version),
        start_block: None,
        tables,
        functions,
    })
}

/// Sort tables by their SQL dependencies using topological ordering.
///
/// Analyzes table queries to determine dependencies and returns tables in dependency order.
/// Tables with no dependencies come first, followed by tables that depend on them.
pub fn sort_tables_by_dependencies(
    dataset_name: &str,
    tables: Vec<LogicalTable>,
    queries: &BTreeMap<String, parser::Statement>,
) -> Result<Vec<LogicalTable>, BoxError> {
    // Map of table name -> Table
    let table_map: BTreeMap<String, LogicalTable> = tables
        .into_iter()
        .map(|t| (t.name().to_string(), t))
        .collect();

    // Dependency map: table -> [tables it depends on]
    let mut deps: BTreeMap<String, Vec<String>> = BTreeMap::new();

    // Initialize empty deps with all tables
    for table_name in table_map.keys() {
        deps.insert(table_name.clone(), Vec::new());
    }

    for (table_name, query) in queries {
        let (table_refs, _) = resolve_table_references(query, true)?;

        // Filter to only include dependencies within the same dataset
        let mut table_deps: Vec<String> = vec![];
        for table_ref in table_refs {
            match (table_ref.schema(), table_ref.table()) {
                (Some(schema), table) if schema == dataset_name => {
                    // Reference to a table in the same dataset
                    if table != table_name && table_map.contains_key(table) {
                        table_deps.push(table.to_string());
                    }
                }
                (None, _) => {
                    // Unqualified reference
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
fn table_dependency_sort(deps: BTreeMap<String, Vec<String>>) -> Result<Vec<String>, BoxError> {
    let nodes: BTreeSet<&String> = deps.keys().collect();
    let mut ordered: Vec<String> = Vec::new();
    let mut visited: BTreeSet<&String> = BTreeSet::new();
    let mut visiting: BTreeSet<&String> = BTreeSet::new();

    for node in nodes {
        if !visited.contains(node) {
            dfs(node, &deps, &mut ordered, &mut visited, &mut visiting)?;
        }
    }

    Ok(ordered)
}

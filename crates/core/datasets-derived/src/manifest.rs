//! Derived dataset
//!
//! This module provides derived datasets that transform and combine data from existing datasets using SQL queries.
//! Derived datasets replace the legacy SQL dataset format, providing versioned, dependency-aware dataset
//! definitions with explicit schemas and functions.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

// Re-export schema types from datasets-common
pub use datasets_common::manifest::{ArrowSchema, Field, TableSchema};
use datasets_common::{manifest::DataType, reference::Reference, table_name::TableName};

use crate::dataset_kind::DerivedDatasetKind;

/// Complete manifest definition for a derived dataset.
///
/// A manifest defines a derived dataset with explicit dependencies, tables, and functions.
/// Derived datasets transform and combine data from existing datasets using SQL queries.
/// This is the replacement for the legacy SQL dataset format, providing better
/// versioning, dependency management, and schema validation.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Manifest {
    /// Dataset kind, must be `manifest`
    pub kind: DerivedDatasetKind,

    /// External dataset dependencies with version requirements
    #[serde(default)]
    pub dependencies: BTreeMap<String, Reference>,
    /// Table definitions mapped by table name
    #[serde(default)]
    pub tables: BTreeMap<TableName, Table>,
    /// User-defined function definitions mapped by function name
    #[serde(default)]
    pub functions: BTreeMap<String, Function>,
}

/// Table definition within a derived dataset.
///
/// Defines a table with its input source, schema, and network.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Table {
    /// Table input source (currently only Views are supported)
    pub input: TableInput,
    /// Arrow schema definition for the table
    pub schema: TableSchema,
    /// Network this table belongs to
    pub network: String,
}

/// User-defined function specification.
///
/// Defines a custom function with input/output types and implementation source.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct Function {
    // TODO: Support SQL type names, see https://datafusion.apache.org/user-guide/sql/data_types.html
    /// Arrow data types for function input parameters
    pub input_types: Vec<DataType>,
    /// Arrow data type for function return value
    pub output_type: DataType,
    /// Function implementation source code and metadata
    pub source: FunctionSource,
}

/// Source code and metadata for a user-defined function.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct FunctionSource {
    /// Function implementation source code
    pub source: Arc<str>,
    /// Filename where the function is defined
    pub filename: String,
}

/// Input source for a table definition.
///
/// Currently only SQL views are supported as table inputs.
// TODO: Add support for other input types with proper tagging
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(untagged)]
pub enum TableInput {
    /// SQL view as table input
    View(View),
}

/// SQL view definition for table input.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct View {
    /// SQL query defining the view
    pub sql: String,
}

/// Errors that occur during derived dataset dependency validation
///
/// This validation ensures that all datasets referenced in SQL queries
/// are properly declared in the manifest's dependencies list.
#[derive(Debug, Clone, thiserror::Error)]
pub enum DependencyValidationError {
    /// SQL query is syntactically invalid or structurally malformed
    ///
    /// This occurs when:
    /// - SQL syntax is incorrect
    /// - Multiple SQL statements are provided (only single statements allowed)
    /// - Table references cannot be resolved from the query
    /// - DataFusion parser/resolver encounters an error
    #[error("invalid SQL query: {0}")]
    InvalidSql(String),

    /// SQL query references datasets not declared in the dependencies list
    ///
    /// This occurs when:
    /// - A SQL query references a schema (dataset) via `schema.table`
    /// - That schema name is not found in the manifest's `dependencies` map
    ///
    /// The error contains a list of all undeclared dataset names that need to be added
    /// to the manifest's dependencies section.
    #[error("undeclared dependencies of SQL query: {0:?}")]
    Missing(Vec<String>),
}

impl Manifest {
    pub fn validate_dependencies(&self) -> Result<(), DependencyValidationError> {
        let mut sql_deps: BTreeSet<String> = Default::default();
        for table in self.tables.values() {
            let sql = match &table.input {
                TableInput::View(view) => &view.sql,
            };
            let statements = datafusion::sql::parser::DFParser::parse_sql(sql)
                .map_err(|err| DependencyValidationError::InvalidSql(err.to_string()))?;
            let statement = match statements {
                _ if statements.len() == 1 => &statements[0],
                _ => {
                    return Err(DependencyValidationError::InvalidSql(format!(
                        "a single SQL statement is expected, found {}",
                        statements.len()
                    )));
                }
            };
            let (references, _) =
                datafusion::sql::resolve::resolve_table_references(statement, true)
                    .map_err(|err| DependencyValidationError::InvalidSql(err.to_string()))?;
            for reference in references.iter().filter_map(|r| r.schema()) {
                sql_deps.insert(reference.to_string());
            }
        }

        // Build a set of all valid dataset references that can be used in SQL queries.
        // This includes:
        let mut declared_deps: BTreeSet<String> = BTreeSet::new();
        // 1. Dependency keys (aliases like "eth_mainnet")
        declared_deps.extend(self.dependencies.keys().cloned());
        // 2. Full reference strings (like "edgeandnode/ethereum_mainnet@0.0.1")
        declared_deps.extend(self.dependencies.values().map(|dep| dep.to_string()));
        // 3. Dataset names from references (like "ethereum_mainnet" from "edgeandnode/ethereum_mainnet@0.0.1")
        declared_deps.extend(self.dependencies.values().map(|dep| dep.name().to_string()));

        let missing_deps: Vec<String> = sql_deps.difference(&declared_deps).cloned().collect();
        if !missing_deps.is_empty() {
            return Err(DependencyValidationError::Missing(missing_deps));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper function to create a minimal test manifest
    fn create_test_manifest(dependencies: BTreeMap<String, Reference>, sql: &str) -> Manifest {
        let mut tables = BTreeMap::new();
        tables.insert(
            "test_table".parse().expect("valid table name"),
            Table {
                input: TableInput::View(View {
                    sql: sql.to_string(),
                }),
                schema: TableSchema {
                    arrow: ArrowSchema { fields: vec![] },
                },
                network: "mainnet".to_string(),
            },
        );

        Manifest {
            kind: DerivedDatasetKind,
            dependencies,
            tables,
            functions: BTreeMap::new(),
        }
    }

    // Test 1: Existing behavior - alias matching works
    #[test]
    fn validate_dependencies_with_alias_in_sql_succeeds() {
        //* Given
        let mut deps = BTreeMap::new();
        deps.insert(
            "eth_mainnet".to_string(),
            "edgeandnode/ethereum_mainnet@0.0.1"
                .parse()
                .expect("valid reference"),
        );
        let manifest = create_test_manifest(deps, "SELECT * FROM eth_mainnet.logs LIMIT 10");

        //* When
        let result = manifest.validate_dependencies();

        //* Then
        assert!(
            result.is_ok(),
            "validation should succeed when SQL uses dependency alias, got error: {:?}",
            result.err()
        );
    }

    // Test 2: NEW behavior - fully qualified reference matching (CURRENTLY FAILS)
    #[test]
    fn validate_dependencies_with_fully_qualified_reference_in_sql_succeeds() {
        //* Given
        let mut deps = BTreeMap::new();
        deps.insert(
            "eth_mainnet".to_string(),
            "edgeandnode/ethereum_mainnet@0.0.1"
                .parse()
                .expect("valid reference"),
        );
        let manifest = create_test_manifest(
            deps,
            r#"SELECT * FROM "edgeandnode/ethereum_mainnet@0.0.1".logs LIMIT 10"#,
        );

        //* When
        let result = manifest.validate_dependencies();

        //* Then
        assert!(
            result.is_ok(),
            "validation should succeed when SQL uses fully qualified reference from dependency value"
        );
    }

    // Test 3: Dependency key as full reference
    #[test]
    fn validate_dependencies_with_full_reference_as_key_succeeds() {
        //* Given
        let mut deps = BTreeMap::new();
        let ref_str = "edgeandnode/ethereum_mainnet@0.0.1";
        deps.insert(
            ref_str.to_string(),
            ref_str.parse().expect("valid reference"),
        );
        let manifest = create_test_manifest(
            deps,
            &format!(r#"SELECT * FROM "{}".logs LIMIT 10"#, ref_str),
        );

        //* When
        let result = manifest.validate_dependencies();

        //* Then
        assert!(
            result.is_ok(),
            "validation should succeed when dependency key matches SQL reference exactly"
        );
    }

    // Test 4: Mixed usage - both alias and full reference in same SQL
    #[test]
    fn validate_dependencies_with_mixed_references_in_sql_succeeds() {
        //* Given
        let mut deps = BTreeMap::new();
        deps.insert(
            "eth_mainnet".to_string(),
            "edgeandnode/ethereum_mainnet@0.0.1"
                .parse()
                .expect("valid reference"),
        );
        deps.insert(
            "uniswap".to_string(),
            "defi/uniswap_v3@2.0.0".parse().expect("valid reference"),
        );
        let manifest = create_test_manifest(
            deps,
            r#"
                SELECT e.*, u.amount
                FROM eth_mainnet.logs e
                JOIN "defi/uniswap_v3@2.0.0".swaps u ON e.tx_hash = u.tx_hash
            "#,
        );

        //* When
        let result = manifest.validate_dependencies();

        //* Then
        assert!(
            result.is_ok(),
            "validation should succeed when SQL mixes aliases and full references"
        );
    }

    // Test 5: Undeclared dependency still fails
    #[test]
    fn validate_dependencies_with_undeclared_dependency_fails() {
        //* Given
        let deps = BTreeMap::new(); // Empty dependencies
        let manifest = create_test_manifest(deps, "SELECT * FROM eth_mainnet.logs LIMIT 10");

        //* When
        let result = manifest.validate_dependencies();

        //* Then
        assert!(
            result.is_err(),
            "validation should fail when SQL references undeclared dependency"
        );
        match result {
            Err(DependencyValidationError::Missing(missing)) => {
                assert!(
                    missing.contains(&"eth_mainnet".to_string()),
                    "error should list the missing dependency"
                );
            }
            _ => panic!("expected Missing error variant"),
        }
    }

    // Test 6: Multiple undeclared dependencies
    #[test]
    fn validate_dependencies_with_multiple_undeclared_dependencies_fails() {
        //* Given
        let deps = BTreeMap::new();
        let manifest = create_test_manifest(
            deps,
            "SELECT * FROM eth_mainnet.logs JOIN uniswap.swaps ON logs.tx_hash = swaps.tx_hash",
        );

        //* When
        let result = manifest.validate_dependencies();

        //* Then
        assert!(
            result.is_err(),
            "validation should fail when SQL references multiple undeclared dependencies"
        );
        match result {
            Err(DependencyValidationError::Missing(missing)) => {
                assert_eq!(
                    missing.len(),
                    2,
                    "error should list both missing dependencies"
                );
                assert!(
                    missing.contains(&"eth_mainnet".to_string()),
                    "should include eth_mainnet"
                );
                assert!(
                    missing.contains(&"uniswap".to_string()),
                    "should include uniswap"
                );
            }
            _ => panic!("expected Missing error variant"),
        }
    }

    // Test 7: Invalid SQL syntax
    #[test]
    fn validate_dependencies_with_invalid_sql_fails() {
        //* Given
        let deps = BTreeMap::new();
        let manifest = create_test_manifest(deps, "SELECT * FROM"); // Invalid SQL

        //* When
        let result = manifest.validate_dependencies();

        //* Then
        assert!(
            result.is_err(),
            "validation should fail with invalid SQL syntax"
        );
        match result {
            Err(DependencyValidationError::InvalidSql(_)) => {
                // Expected
            }
            _ => panic!("expected InvalidSql error variant"),
        }
    }

    // Test 8: Multiple SQL statements (not allowed)
    #[test]
    fn validate_dependencies_with_multiple_statements_fails() {
        //* Given
        let mut deps = BTreeMap::new();
        deps.insert(
            "eth".to_string(),
            "ns/eth@1.0.0".parse().expect("valid reference"),
        );
        let manifest =
            create_test_manifest(deps, "SELECT * FROM eth.logs; SELECT * FROM eth.blocks");

        //* When
        let result = manifest.validate_dependencies();

        //* Then
        assert!(
            result.is_err(),
            "validation should fail with multiple SQL statements"
        );
        match result {
            Err(DependencyValidationError::InvalidSql(msg)) => {
                assert!(
                    msg.contains("single SQL statement"),
                    "error message should mention single statement requirement"
                );
            }
            _ => panic!("expected InvalidSql error variant"),
        }
    }

    // Test 9: Empty dependencies and SQL without table references
    #[test]
    fn validate_dependencies_with_no_tables_succeeds() {
        //* Given
        let deps = BTreeMap::new();
        let manifest = create_test_manifest(deps, "SELECT 1 AS constant");

        //* When
        let result = manifest.validate_dependencies();

        //* Then
        assert!(
            result.is_ok(),
            "validation should succeed for SQL without table references"
        );
    }

    // Test 10: Using dataset name from reference in SQL (not alias or full reference)
    #[test]
    fn validate_dependencies_with_dataset_name_in_sql_succeeds() {
        //* Given
        let mut deps = BTreeMap::new();
        deps.insert(
            "raw_mainnet".to_string(),
            "_/eth_firehose@0.0.1".parse().expect("valid reference"),
        );
        let manifest = create_test_manifest(
            deps,
            "SELECT block_num, miner, hash FROM eth_firehose.blocks",
        );

        //* When
        let result = manifest.validate_dependencies();

        //* Then
        assert!(
            result.is_ok(),
            "validation should succeed when SQL uses dataset name from reference (not alias or full reference), got error: {:?}",
            result.err()
        );
    }
}

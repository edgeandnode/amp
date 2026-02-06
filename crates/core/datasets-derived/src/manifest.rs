//! Derived dataset
//!
//! This module provides derived datasets that transform and combine data from existing datasets using SQL queries.
//! Derived datasets replace the legacy SQL dataset format, providing versioned, dependency-aware dataset
//! definitions with explicit schemas and functions.

use std::collections::BTreeMap;

// Re-export schema types from datasets-common
pub use datasets_common::manifest::{ArrowSchema, Field, Function, FunctionSource, TableSchema};
use datasets_common::{block_num::BlockNum, network_id::NetworkId, table_name::TableName};

use crate::{
    dataset_kind::DerivedDatasetKind,
    deps::{DepAlias, DepReference},
    func_name::FuncName,
    sql_str::SqlStr,
};

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

    /// Dataset start block
    #[serde(default)]
    pub start_block: Option<BlockNum>,

    /// External dataset dependencies with version requirements
    #[serde(default)]
    pub dependencies: BTreeMap<DepAlias, DepReference>,
    /// Table definitions mapped by table name
    #[serde(default)]
    pub tables: BTreeMap<TableName, Table>,
    /// User-defined function definitions mapped by function name
    #[serde(default)]
    pub functions: BTreeMap<FuncName, Function>,
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
    pub network: NetworkId,
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
    pub sql: SqlStr,
}

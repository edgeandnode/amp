//! Dependency manifest types for dataset authoring.
//!
//! These types capture the minimal manifest shape needed for dependency
//! resolution and schema inference across derived and raw dataset kinds.

use std::collections::BTreeMap;

use datasets_common::{
    dataset_kind_str::DatasetKindStr,
    manifest::{Function, TableSchema},
    network_id::NetworkId,
    table_name::TableName,
};
use datasets_derived::{
    deps::{DepAlias, DepReference},
    func_name::FuncName,
};

/// Minimal manifest representation for dependency resolution.
///
/// This intentionally captures only the fields required for planning:
/// - `kind` to identify dataset type
/// - `dependencies` for transitive resolution
/// - `tables` for schema inference and network inference
/// - `functions` for validating dependency UDFs
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DependencyManifest {
    /// Dataset kind (e.g., "manifest", "evm-rpc", "firehose", "solana").
    pub kind: DatasetKindStr,

    /// External dataset dependencies with version/hash requirements.
    #[serde(default)]
    pub dependencies: BTreeMap<DepAlias, DepReference>,

    /// Table definitions mapped by table name.
    #[serde(default)]
    pub tables: BTreeMap<TableName, DependencyTable>,

    /// Function definitions mapped by function name.
    #[serde(default)]
    pub functions: BTreeMap<FuncName, Function>,
}

/// Minimal table definition for dependency planning.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DependencyTable {
    /// Arrow schema for the table.
    pub schema: TableSchema,
    /// Network for the table.
    pub network: NetworkId,
}

//! User-facing dataset definition structures.
//!
//! This module defines the simple format that developers write in their nozzle.config files,
//! before schema inference and transformation to the full internal Manifest.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// User-facing dataset definition (what developers write in nozzle.config.ts)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetDefinition {
    /// Dataset name
    pub name: String,
    /// Dataset version, e.g., `1.0.0`
    #[serde(default = "default_version")]
    pub version: String,
    /// Network name, e.g., "mainnet", "sepolia"
    pub network: String,
    /// External dataset dependencies
    #[serde(default)]
    pub dependencies: BTreeMap<String, Dependency>,
    /// Table definitions (simple format with just SQL)
    #[serde(default)]
    pub tables: BTreeMap<String, TableDefinition>,
    /// User-defined function definitions
    #[serde(default)]
    pub functions: BTreeMap<String, FunctionDefinition>,
}

fn default_version() -> String {
    "0.1.0".to_string()
}

/// Dependency on another dataset
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dependency {
    pub owner: String,
    pub name: String,
    pub version: String,
}

/// Simple table definition with just SQL (user-facing)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDefinition {
    /// SQL query defining the table
    pub sql: String,
}

/// User-defined function definition
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FunctionDefinition {
    pub source: FunctionSource,
    pub input_types: Vec<String>,
    pub output_type: String,
}

/// Function source code
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionSource {
    pub source: String,
    pub filename: String,
}

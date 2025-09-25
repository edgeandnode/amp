//! Legacy SQL dataset definitions and utilities.
//!
//! This module provides support for the legacy SQL dataset format, which is being
//! replaced by derived datasets in [`crate::manifest::derived`].
//! New datasets should use derived datasets instead.

use std::collections::BTreeMap;

use datafusion::sql::parser;
use datasets_common::{name::Name, value::ManifestValue};

use crate::Dataset;

/// Dataset kind constant for legacy SQL datasets.
pub const DATASET_KIND: &str = "sql";

/// A legacy SQL dataset with its associated queries.
///
/// This structure represents the legacy SQL dataset format that is being phased out
/// in favor of derived datasets. It contains a dataset definition
/// and a mapping of table names to their defining SQL queries.
pub struct SqlDataset {
    /// The underlying dataset definition
    pub dataset: Dataset,
    /// Maps table names to their defining SQL queries
    pub queries: BTreeMap<String, parser::Statement>,
}

impl SqlDataset {
    /// Get the name of this SQL dataset.
    pub fn name(&self) -> &str {
        &self.dataset.name
    }
}

/// Legacy SQL dataset definition structure.
///
/// This is the basic metadata structure for SQL datasets in TOML or JSON format.
/// The actual SQL queries are stored separately in `.sql` files.
#[derive(Debug, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Manifest {
    /// Dataset name, must match the filename
    pub name: Name,
    /// Dataset kind, must be "sql"
    pub kind: String,
    /// Network name, e.g., "mainnet", "sepolia"
    pub network: String,
}

impl Manifest {
    /// Parse a dataset definition from TOML or JSON value.
    pub fn from_value(value: ManifestValue) -> Result<Self, Error> {
        match value {
            ManifestValue::Toml(value) => value.try_into().map_err(Into::into),
            ManifestValue::Json(value) => serde_json::from_value(value).map_err(Into::into),
        }
    }
}

/// Errors that can occur when parsing SQL dataset definitions.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// TOML deserialization failed when parsing a TOML dataset definition
    #[error("TOML parse error: {0}")]
    Toml(#[from] toml::de::Error),
    /// JSON deserialization failed when parsing a JSON dataset definition
    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),
}

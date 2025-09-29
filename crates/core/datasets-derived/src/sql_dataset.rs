//! Legacy SQL dataset definitions and utilities.
//!
//! This module provides support for the legacy SQL dataset format, which is being
//! replaced by derived datasets in [`crate::manifest::derived`].
//! New datasets should use derived datasets instead.

use datasets_common::{name::Name, version::Version};

mod dataset_kind;

pub use self::dataset_kind::{DATASET_KIND, SqlDatasetKind, SqlDatasetKindError};

/// Legacy SQL dataset definition structure.
///
/// This is the basic metadata structure for SQL datasets in TOML or JSON format.
/// The actual SQL queries are stored separately in `.sql` files.
#[derive(Debug, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Manifest {
    /// Dataset name
    pub name: Name,
    /// Dataset version, e.g., `1.0.0`
    #[serde(default)]
    pub version: Version,
    /// Dataset kind, must be `sql`.
    pub kind: SqlDatasetKind,
    /// Network name, e.g., "mainnet", "sepolia"
    pub network: String,
}

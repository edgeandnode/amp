//! Static dataset manifest definition.
//!
//! Defines the manifest structure for static datasets backed by data files
//! with explicit schema definitions.
//!
//! Each manifest defines one or more tables, keyed by [`TableName`], that
//! describe data files and their Arrow schemas.

use std::collections::{BTreeMap, BTreeSet};

pub use datasets_common::manifest::{ArrowSchema, Field, TableSchema};
use datasets_common::table_name::TableName;

use crate::{dataset_kind::StaticDatasetKind, file_format::FileFormat, file_path::FilePath};

/// Complete manifest definition for a static dataset.
///
/// A manifest defines one or more tables backed by static data files.
/// Table names are explicit map keys.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Manifest {
    /// Dataset kind, must be `static`.
    pub kind: StaticDatasetKind,

    /// Table definitions mapped by table name.
    #[cfg_attr(feature = "schemars", schemars(required, extend("minProperties" = 1)))]
    pub tables: BTreeMap<TableName, Table>,
}

/// Table definition within a static dataset.
///
/// Defines a table backed by a static data file with its format
/// and Arrow schema.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Table {
    /// Path to the data file relative to the provider's object store root.
    pub path: FilePath,

    /// File format and format-specific configuration.
    #[serde(flatten)]
    pub format: FileFormat,

    /// Table schema definition.
    pub schema: TableSchema,

    /// Column names the data file is pre-sorted by.
    ///
    /// Declaring sort order enables the query engine to skip redundant sorts.
    /// Defaults to empty (no ordering guarantees).
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub sorted_by: BTreeSet<String>,
}

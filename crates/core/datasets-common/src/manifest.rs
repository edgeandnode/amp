//! Dataset manifest and schema types.
//!
//! This module provides the core types for dataset manifests, schemas, and data types
//! used across different dataset definition formats.

use std::collections::HashMap;

use datafusion::arrow::datatypes::DataType as ArrowDataType;

use crate::{name::Name, version::Version};

/// Common metadata fields required by all dataset definitions.
///
/// All dataset definitions must have a kind, network and name. The name must match the filename.
/// Schema is optional for TOML dataset format.
#[derive(serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Manifest {
    /// Dataset name
    pub name: Name,
    /// Dataset version, e.g., `0.1.0`, `1.0.0`, `0.2.1-beta.3`
    #[serde(default)]
    pub version: Version,
    /// Dataset kind. See specific dataset definitions for supported values.
    ///
    /// Common values include: `manifest`, `evm-rpc`, `firehose`, `substreams`.
    pub kind: String,
    /// Network name, e.g., `mainnet`, `sepolia`
    pub network: Option<String>,
    /// Dataset schema. Lists the tables defined by this dataset.
    pub schema: Option<Schema>,
}

impl std::fmt::Debug for Manifest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Manifest")
            .field("name", &self.name)
            .field("kind", &self.kind)
            .field("version", &self.version)
            .field("network", &self.network)
            .field(
                "schema",
                if self.schema.is_some() {
                    &"Some(Schema { ... })"
                } else {
                    &"None"
                },
            )
            .finish()
    }
}

/// A serializable representation of a collection of Arrow schemas without metadata.
///
/// This structure maps table names to their field definitions, providing a way to serialize
/// and deserialize Arrow schemas while filtering out the special `SPECIAL_BLOCK_NUM` field.
///
/// Structure: table name -> field name -> data type
#[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Schema(HashMap<String, HashMap<String, DataType>>);

impl Schema {
    /// Create a new empty Schema.
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    /// Insert a table schema into this Schema.
    pub fn insert_table(&mut self, table_name: String, fields: HashMap<String, DataType>) {
        self.0.insert(table_name, fields);
    }

    /// Get the inner HashMap.
    pub fn into_inner(self) -> HashMap<String, HashMap<String, DataType>> {
        self.0
    }

    /// Get a reference to the inner HashMap.
    pub fn as_inner(&self) -> &HashMap<String, HashMap<String, DataType>> {
        &self.0
    }
}

impl Default for Schema {
    fn default() -> Self {
        Self::new()
    }
}

/// Apache Arrow data _new-type_ wrapper with JSON schema support.
///
/// This wrapper provides serialization and JSON schema generation capabilities
/// for Apache Arrow data types used in dataset table definitions. Arrow data types
/// define the structure and format of columnar data.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(transparent))]
#[cfg_attr(
    feature = "schemars",
    schemars(description = "Arrow data type, e.g. `Int32`, `Utf8`, etc.")
)]
pub struct DataType(#[cfg_attr(feature = "schemars", schemars(with = "String"))] pub ArrowDataType);

impl DataType {
    /// Returns a reference to the inner Arrow [`DataType`](ArrowDataType).
    pub fn as_arrow(&self) -> &ArrowDataType {
        &self.0
    }

    /// Consumes the [`DataType`] and returns the inner Arrow [`DataType`](ArrowDataType).
    pub fn into_arrow(self) -> ArrowDataType {
        self.0
    }
}

impl std::ops::Deref for DataType {
    type Target = ArrowDataType;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<ArrowDataType> for DataType {
    fn from(value: ArrowDataType) -> Self {
        Self(value)
    }
}

/// Semver version requirement wrapper with JSON schema support.
///
/// Used for specifying dependency version constraints in derived datasets.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(transparent))]
pub struct VersionReq(
    #[cfg_attr(feature = "schemars", schemars(with = "String"))] semver::VersionReq,
);

impl std::ops::Deref for VersionReq {
    type Target = semver::VersionReq;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq<semver::VersionReq> for VersionReq {
    fn eq(&self, other: &semver::VersionReq) -> bool {
        self.0 == *other
    }
}

impl PartialEq<VersionReq> for semver::VersionReq {
    fn eq(&self, other: &VersionReq) -> bool {
        *self == other.0
    }
}

impl std::fmt::Display for VersionReq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for VersionReq {
    type Err = semver::Error;

    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse().map(Self)
    }
}

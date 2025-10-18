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
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Manifest {
    /// Dataset name
    pub name: Name,
    /// Dataset version, e.g., `0.1.0`, `1.0.0`, `0.2.1-beta.3`
    #[serde(default)]
    pub version: Version,
    /// Dataset kind. See specific dataset definitions for supported values.
    ///
    /// Common values include: `manifest`, `evm-rpc`, `firehose`.
    pub kind: String,

    /// Network name, e.g., `mainnet`, `sepolia`
    ///
    /// Raw datasets' specific
    pub network: Option<String>,
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

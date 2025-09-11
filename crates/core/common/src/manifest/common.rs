//! Common types and utilities for dataset definitions.
//!
//! This module provides shared structures used across different dataset definition formats,
//! including serializable schema representations and common dataset metadata.

use std::collections::HashMap;

use crate::{DataTypeJsonSchema, SPECIAL_BLOCK_NUM, Table as LogicalTable};

/// Common metadata fields required by all dataset definitions.
///
/// All dataset definitions must have a kind, network and name. The name must match the filename.
/// Schema is optional for TOML dataset format.
#[derive(serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Manifest {
    /// Dataset kind. See specific dataset definitions for supported values.
    /// Common values include: "evm-rpc", "firehose", "substreams", "sql", "manifest"
    pub kind: String,
    /// Network name, e.g. "mainnet", "sepolia", "polygon"
    pub network: String,
    /// Dataset name. Must be unique within the network and match the filename
    pub name: String,
    /// Dataset schema. Lists the tables defined by this dataset.
    /// Optional for TOML format, required for JSON format
    pub schema: Option<SerializableSchema>,
}

/// A serializable representation of a collection of Arrow schemas without metadata.
///
/// This structure maps table names to their field definitions, providing a way to serialize
/// and deserialize Arrow schemas while filtering out the special `SPECIAL_BLOCK_NUM` field.
///
/// Structure: table name -> field name -> data type
#[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct SerializableSchema(HashMap<String, HashMap<String, DataTypeJsonSchema>>);

impl From<Vec<LogicalTable>> for SerializableSchema {
    fn from(tables: Vec<LogicalTable>) -> Self {
        let inner = tables
            .into_iter()
            .map(|table| {
                let inner_map = table
                    .schema()
                    .fields()
                    .iter()
                    .filter(|&field| field.name() != SPECIAL_BLOCK_NUM)
                    .fold(std::collections::HashMap::new(), |mut acc, field| {
                        acc.insert(
                            field.name().clone(),
                            DataTypeJsonSchema(field.data_type().clone()),
                        );
                        acc
                    });
                (table.name().to_string().clone(), inner_map)
            })
            .collect();

        Self(inner)
    }
}

impl From<&[LogicalTable]> for SerializableSchema {
    fn from(tables: &[LogicalTable]) -> Self {
        let inner = tables
            .iter()
            .map(|table| {
                let inner_map = table
                    .schema()
                    .fields()
                    .iter()
                    .filter(|&field| field.name() != SPECIAL_BLOCK_NUM)
                    .fold(std::collections::HashMap::new(), |mut acc, field| {
                        acc.insert(
                            field.name().clone(),
                            DataTypeJsonSchema(field.data_type().clone()),
                        );
                        acc
                    });
                (table.name().to_string().clone(), inner_map)
            })
            .collect();

        Self(inner)
    }
}

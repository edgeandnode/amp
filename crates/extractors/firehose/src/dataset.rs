use std::collections::BTreeMap;

use common::BlockNum;
// Reuse types from datasets-common for consistency
pub use datasets_common::manifest::{ArrowSchema, Field, TableSchema};

use crate::dataset_kind::FirehoseDatasetKind;

/// Table definition for raw datasets
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Table {
    /// Arrow schema for this table
    pub schema: TableSchema,
    /// Network for this table
    pub network: String,
}

impl Table {
    /// Create a new table with the given schema and network
    pub fn new(schema: TableSchema, network: String) -> Self {
        Self { schema, network }
    }
}

/// Firehose dataset manifest.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Manifest {
    /// Dataset kind, must be `firehose`.
    pub kind: FirehoseDatasetKind,

    /// Network name, e.g., `mainnet`.
    pub network: String,
    /// Dataset start block.
    #[serde(default)]
    pub start_block: BlockNum,
    /// Only include finalized block data.
    #[serde(default)]
    pub finalized_blocks_only: bool,

    /// Dataset tables. Maps table names to their definitions.
    pub tables: BTreeMap<String, Table>,
}

#[derive(Debug, serde::Deserialize)]
pub struct ProviderConfig {
    pub name: String,
    pub kind: FirehoseDatasetKind,
    pub network: String,
    pub url: String,
    pub token: Option<String>,
}

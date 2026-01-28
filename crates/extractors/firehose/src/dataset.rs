use std::collections::BTreeMap;

// Reuse types from datasets-common for consistency
pub use datasets_common::manifest::TableSchema;
use datasets_common::{
    dataset::{BlockNum, Table as DatasetTable},
    dataset_kind_str::DatasetKindStr,
    hash_reference::HashReference,
    network_id::NetworkId,
};

use crate::dataset_kind::FirehoseDatasetKind;

/// Table definition for raw datasets
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Table {
    /// Arrow schema for this table
    pub schema: TableSchema,
    /// Network for this table
    pub network: NetworkId,
}

impl Table {
    /// Create a new table with the given schema and network
    pub fn new(schema: TableSchema, network: NetworkId) -> Self {
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
    pub network: NetworkId,
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
    pub network: NetworkId,
    pub url: String,
    pub token: Option<String>,
}

pub struct Dataset {
    pub(crate) tables: Vec<DatasetTable>,
    pub(crate) start_block: Option<BlockNum>,
    pub(crate) kind: FirehoseDatasetKind,
    pub(crate) reference: HashReference,
    pub(crate) finalized_blocks_only: bool,
}

impl datasets_common::dataset::Dataset for Dataset {
    fn reference(&self) -> &HashReference {
        &self.reference
    }

    fn kind(&self) -> DatasetKindStr {
        self.kind.into()
    }

    fn tables(&self) -> &[DatasetTable] {
        &self.tables
    }

    fn start_block(&self) -> Option<BlockNum> {
        self.start_block
    }

    fn finalized_blocks_only(&self) -> bool {
        self.finalized_blocks_only
    }
}

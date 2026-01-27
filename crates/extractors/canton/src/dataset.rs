//! Canton dataset definition and provider configuration.

use std::collections::BTreeMap;

use datasets_common::{
    dataset::{BlockNum, Table as DatasetTable},
    deps::{alias::DepAlias, reference::DepReference},
    hash_reference::HashReference,
    manifest::TableSchema,
    raw_dataset_kind::RawDatasetKind,
};

use crate::dataset_kind::CantonDatasetKind;

/// Table definition for Canton datasets.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Table {
    /// Arrow schema for this table
    pub schema: TableSchema,
    /// Network for this table
    pub network: String,
}

impl Table {
    pub fn new(schema: TableSchema, network: String) -> Self {
        Self { schema, network }
    }
}

/// Canton dataset manifest.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Manifest {
    /// Dataset kind, must be `canton`.
    pub kind: CantonDatasetKind,
    /// Network name, e.g., `devnet`, `testnet`.
    pub network: String,
    /// Dataset start offset (mapped to block for AMP compatibility).
    #[serde(default)]
    pub start_block: BlockNum,
    /// Only include finalized data.
    #[serde(default)]
    pub finalized_blocks_only: bool,
    /// Dataset tables. Maps table names to their definitions.
    pub tables: BTreeMap<String, Table>,
}

/// Canton provider configuration.
#[derive(Debug, serde::Deserialize)]
pub struct ProviderConfig {
    /// Provider name.
    pub name: String,
    /// Dataset kind (must be "canton").
    pub kind: CantonDatasetKind,
    /// Network name.
    pub network: String,
    /// Canton-bridge Arrow Flight endpoint URL (e.g., "http://localhost:9092").
    pub bridge_url: String,
    /// Party ID for Canton ledger access.
    pub party_id: String,
}

/// Canton dataset implementation.
pub struct Dataset {
    pub(crate) tables: Vec<DatasetTable>,
    pub(crate) start_block: Option<BlockNum>,
    pub(crate) kind: CantonDatasetKind,
    pub(crate) dependencies: BTreeMap<DepAlias, DepReference>,
    pub(crate) reference: HashReference,
    pub(crate) network: Option<String>,
    pub(crate) finalized_blocks_only: bool,
}

impl datasets_common::dataset::Dataset for Dataset {
    fn tables(&self) -> &[DatasetTable] {
        &self.tables
    }

    fn start_block(&self) -> Option<BlockNum> {
        self.start_block
    }

    fn kind(&self) -> RawDatasetKind {
        self.kind.into()
    }

    fn dependencies(&self) -> &BTreeMap<DepAlias, DepReference> {
        &self.dependencies
    }

    fn reference(&self) -> &HashReference {
        &self.reference
    }

    fn network(&self) -> Option<&String> {
        self.network.as_ref()
    }

    fn finalized_blocks_only(&self) -> bool {
        self.finalized_blocks_only
    }

    fn as_dataset_with_functions(
        &self,
    ) -> Option<&dyn datasets_common::dataset::DatasetWithFunctions> {
        None
    }
}

/// Create a Canton dataset from a manifest.
pub fn dataset(reference: HashReference, manifest: Manifest) -> Dataset {
    let tables = crate::tables::all(&manifest.network);

    Dataset {
        tables,
        start_block: if manifest.start_block > 0 {
            Some(manifest.start_block)
        } else {
            None
        },
        kind: manifest.kind,
        dependencies: BTreeMap::new(),
        reference,
        network: Some(manifest.network),
        finalized_blocks_only: manifest.finalized_blocks_only,
    }
}

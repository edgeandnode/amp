use std::collections::BTreeMap;

use datasets_common::{block_num::BlockNum, hash_reference::HashReference, network_id::NetworkId};
use datasets_raw::dataset::Dataset as RawDataset;

mod dataset_kind;
pub mod tables;

// Reuse types from datasets-common for consistency
pub use datasets_common::manifest::{ArrowSchema, Field, TableSchema};

pub use self::dataset_kind::{EvmRpcDatasetKind, EvmRpcDatasetKindError};

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

/// EVM RPC dataset manifest.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Manifest {
    /// Dataset kind, must be `evm-rpc`
    pub kind: EvmRpcDatasetKind,

    /// Network name, e.g., `anvil`, `mainnet`
    pub network: NetworkId,
    /// Dataset start block
    #[serde(default)]
    pub start_block: BlockNum,
    /// Only include finalized block data
    #[serde(default)]
    pub finalized_blocks_only: bool,

    /// Dataset tables. Maps table names to their definitions.
    pub tables: BTreeMap<String, Table>,
}

/// Convert an EVM RPC manifest into a logical dataset representation.
///
/// Dataset identity (namespace, name, version, hash reference) must be provided externally as they
/// are not part of the manifest.
pub fn dataset(reference: HashReference, manifest: Manifest) -> RawDataset {
    let network = manifest.network;
    RawDataset::new(
        reference,
        manifest.kind.into(),
        network.clone(),
        tables::all(&network),
        Some(manifest.start_block),
        manifest.finalized_blocks_only,
    )
}

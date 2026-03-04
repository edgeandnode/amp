//! EVM block data is sufficiently complicated that there may be multiple encoding flavors, each with
//! multiple versions. There is no universal encoding, and we're not going to try to enforce one.
//! Each extraction layer can have its own data format. This `firehose` crate defines Firehose
//! dataset kinds and manifest types.

use std::collections::BTreeMap;

use datasets_common::{
    block_num::BlockNum, hash_reference::HashReference, manifest::TableSchema,
    network_id::NetworkId,
};
use datasets_raw::dataset::Dataset as RawDataset;

mod dataset_kind;
pub mod tables;

pub use self::dataset_kind::{FirehoseDatasetKind, FirehoseDatasetKindError};

/// Table definition for raw datasets.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Table {
    /// Arrow schema for this table.
    pub schema: TableSchema,
    /// Network for this table.
    pub network: NetworkId,
}

impl Table {
    /// Create a new table with the given schema and network.
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

/// Convert a Firehose manifest into a logical dataset representation.
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

use std::{collections::BTreeMap, num::NonZeroU32};

use common::{BlockNum, Dataset};
use datasets_common::hash_reference::HashReference;
use reqwest::Url;

mod block;
mod client;
mod dataset_kind;

// Reuse types from datasets-common for consistency
pub use datasets_common::manifest::{ArrowSchema, Field, TableSchema};

pub use self::{
    client::BeaconClient,
    dataset_kind::{EthBeaconDatasetKind, EthBeaconDatasetKindError},
};

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

/// Eth Beacon dataset manifest.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Manifest {
    /// Dataset kind, must be `eth-beacon`.
    pub kind: EthBeaconDatasetKind,
    /// Network name, e.g., `mainnet-beacon`.
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

#[serde_with::serde_as]
#[derive(Debug, serde::Deserialize)]
pub struct ProviderConfig {
    pub name: String,
    pub kind: EthBeaconDatasetKind,
    pub network: String,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub url: Url,
    pub concurrent_request_limit: Option<u16>,
    pub rate_limit_per_minute: Option<NonZeroU32>,
}

/// Convert an Eth Beacon manifest into a logical dataset representation.
///
/// Dataset identity (namespace, name, version, hash reference) must be provided externally as they
/// are not part of the manifest.
pub fn dataset(reference: HashReference, manifest: Manifest) -> Dataset {
    let network = manifest.network;
    Dataset {
        reference,
        dependencies: BTreeMap::new(),
        kind: manifest.kind.to_string(),
        start_block: Some(manifest.start_block),
        finalized_blocks_only: manifest.finalized_blocks_only,
        tables: all_tables(network.clone()),
        network: Some(network),
        functions: vec![],
    }
}

pub fn all_tables(network: String) -> Vec<common::Table> {
    vec![block::table(network)]
}

pub fn client(provider: ProviderConfig) -> BeaconClient {
    BeaconClient::new(
        provider.url,
        provider.network,
        provider.name,
        u16::max(1, provider.concurrent_request_limit.unwrap_or(1024)),
        provider.rate_limit_per_minute,
    )
}

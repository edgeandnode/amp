//! Solana dataset extractor.
//!
//! ### Important note - Slot vs. Block Number
//!
//! In Solana, each produced block gets placed in a "slot", which is a specific time interval
//! during which a validator can propose a block. However, not every slot results in a produced
//! block; some slots may be skipped due to various reasons such as network issues or validator
//! performance. Therefore, the slot number does not always correspond directly to a block number.
//!
//! Since [`common::BlockStreamer`] and related infrastructure generally operate on the concept of block numbers,
//! this implementation treats Solana slots as block numbers for the most part. Skipped slots are handled
//! by yielding empty rows for those slots, ensuring that the sequence of block numbers remains continuous.

use std::{collections::BTreeMap, num::NonZeroU32, path::PathBuf};

use common::{BlockNum, BoxError, Dataset};
use datasets_common::{hash_reference::HashReference, manifest::TableSchema};
use serde_with::serde_as;
use url::Url;

mod dataset_kind;
mod extractor;
mod metrics;
mod of1_client;
mod rpc_client;
pub mod tables;

pub use self::{
    dataset_kind::{SolanaDatasetKind, SolanaDatasetKindError},
    extractor::SolanaExtractor,
};

#[derive(Debug, thiserror::Error)]
#[error("RPC client error")]
pub struct Error(#[source] pub BoxError);

/// Table definition for raw datasets.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Table {
    /// Arrow schema for this table.
    pub schema: TableSchema,
    /// Network for this table.
    pub network: String,
}

impl Table {
    /// Create a new table with the given schema and network.
    pub fn new(schema: TableSchema, network: String) -> Self {
        Self { schema, network }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Manifest {
    /// Dataset kind, must be `solana`
    pub kind: SolanaDatasetKind,

    /// Network name, e.g., `mainnet`
    pub network: String,
    /// Dataset start block
    #[serde(default)]
    pub start_block: BlockNum,
    /// Only include finalized block data
    #[serde(default)]
    pub finalized_blocks_only: bool,

    /// Dataset tables. Maps table names to their definitions.
    pub tables: BTreeMap<String, Table>,
}

#[serde_as]
#[derive(Debug, serde::Deserialize)]
pub struct ProviderConfig {
    pub name: String,
    pub kind: SolanaDatasetKind,
    pub network: String,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub rpc_provider_url: Url,
    pub max_rpc_calls_per_second: Option<NonZeroU32>,
    pub of1_car_directory: PathBuf,
    #[serde(default)]
    pub keep_of1_car_files: bool,
}

/// Convert a Solana manifest into a logical dataset representation.
///
/// Dataset identity (namespace, name, version, hash reference) must be provided externally as they
/// are not part of the manifest.
pub fn dataset(reference: HashReference, manifest: Manifest) -> Dataset {
    Dataset {
        reference,
        dependencies: BTreeMap::new(),
        kind: manifest.kind.to_string(),
        start_block: Some(manifest.start_block),
        finalized_blocks_only: manifest.finalized_blocks_only,
        tables: tables::all(&manifest.network),
        network: Some(manifest.network),
        functions: vec![],
    }
}

/// Create a Solana extractor based on the provided configuration.
pub fn extractor(
    config: ProviderConfig,
    meter: Option<&monitoring::telemetry::metrics::Meter>,
) -> Result<SolanaExtractor, Error> {
    let client = match config.rpc_provider_url.scheme() {
        "http" | "https" => SolanaExtractor::new(
            config.rpc_provider_url,
            config.max_rpc_calls_per_second,
            config.network,
            config.name,
            config.of1_car_directory,
            config.keep_of1_car_files,
            meter,
        ),
        scheme => {
            let err = format!("unsupported Solana RPC provider URL scheme: {}", scheme);
            return Err(Error(err.into()));
        }
    };

    Ok(client)
}

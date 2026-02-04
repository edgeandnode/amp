//! Solana dataset extractor.
//!
//! ### Important note - Slot vs. Block Number
//!
//! In Solana, each produced block gets placed in a "slot", which is a specific time interval
//! during which a validator can propose a block. However, not every slot results in a produced
//! block; some slots may be skipped due to various reasons such as network issues or validator
//! performance. Therefore, the slot number does not always correspond directly to a block number.
//!
//! Since [`datasets_raw::client::BlockStreamer`] and related infrastructure generally operate on the concept of block numbers,
//! this implementation treats Solana slots as block numbers. Skipped slots do not produce any rows,
//! resulting in gaps in the block number sequence. Chain integrity is maintained through hash-based
//! validation where each block's parent_hash must match the previous block's hash.

use std::{collections::BTreeMap, num::NonZeroU32, path::PathBuf};

use datasets_common::{
    dataset::BlockNum, hash_reference::HashReference, manifest::TableSchema, network_id::NetworkId,
};
use serde_with::serde_as;
use url::Url;

mod dataset;
mod dataset_kind;
pub mod error;
mod extractor;
mod metrics;
pub mod of1_client;
pub mod rpc_client;
pub mod tables;

pub use self::{
    dataset::Dataset,
    dataset_kind::{SolanaDatasetKind, SolanaDatasetKindError},
    extractor::{non_empty_of1_slot, non_empty_rpc_slot, SolanaExtractor},
};
use crate::error::ExtractorError;

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

/// Archive usage mode for historical data extraction.
#[derive(Debug, Clone, Copy, Default, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UseArchive {
    /// Smart selection: use RPC for recent slots (last 10k), archive for historical data.
    Auto,
    /// Always use archive (CAR files), even for recent data.
    #[default]
    Always,
    /// Never use archive, RPC-only mode.
    Never,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Manifest {
    /// Dataset kind, must be `solana`
    pub kind: SolanaDatasetKind,

    /// Network name, e.g., `mainnet`
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

#[serde_as]
#[derive(Debug, serde::Deserialize)]
pub struct ProviderConfig {
    pub name: String,
    pub kind: SolanaDatasetKind,
    pub network: NetworkId,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub rpc_provider_url: Url,
    pub max_rpc_calls_per_second: Option<NonZeroU32>,
    pub of1_car_directory: PathBuf,
    #[serde(default)]
    pub keep_of1_car_files: bool,
    #[serde(default)]
    pub use_archive: UseArchive,
}

/// Convert a Solana manifest into a logical dataset representation.
///
/// Dataset identity (namespace, name, version, hash reference) must be provided externally as they
/// are not part of the manifest.
pub fn dataset(reference: HashReference, manifest: Manifest) -> crate::dataset::Dataset {
    crate::dataset::Dataset {
        reference,
        kind: manifest.kind,
        start_block: Some(manifest.start_block),
        finalized_blocks_only: manifest.finalized_blocks_only,
        tables: tables::all(&manifest.network),
    }
}

/// Create a Solana extractor based on the provided configuration.
pub fn extractor(
    config: ProviderConfig,
    meter: Option<&monitoring::telemetry::metrics::Meter>,
) -> Result<SolanaExtractor, ExtractorError> {
    if config.network != "mainnet" {
        let err = format!(
            "unsupported Solana network: {}. Only 'mainnet' is supported.",
            config.network
        );
        return Err(ExtractorError(err));
    }

    let client = match config.rpc_provider_url.scheme() {
        "http" | "https" => SolanaExtractor::new(
            config.rpc_provider_url,
            config.max_rpc_calls_per_second,
            config.network,
            config.name,
            config.of1_car_directory,
            config.keep_of1_car_files,
            config.use_archive,
            meter,
        ),
        scheme => {
            let err = format!("unsupported Solana RPC provider URL scheme: {}", scheme);
            return Err(ExtractorError(err));
        }
    };

    Ok(client)
}

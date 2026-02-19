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

use std::collections::BTreeMap;

use amp_providers_common::provider_name::ProviderName;
use datasets_common::{
    block_num::BlockNum, hash_reference::HashReference, manifest::TableSchema,
    network_id::NetworkId,
};

mod dataset;
mod dataset_kind;
pub mod error;
mod extractor;
mod metrics;
pub mod of1_client;
pub mod rpc_client;
pub mod tables;

pub use amp_providers_solana::config::{SolanaProviderConfig, UseArchive};

pub use self::{
    dataset::Dataset,
    dataset_kind::{SolanaDatasetKind, SolanaDatasetKindError},
    extractor::{SolanaExtractor, non_empty_of1_slot, non_empty_rpc_slot},
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

/// Convert a Solana manifest into a logical dataset representation.
///
/// Dataset identity (namespace, name, version, hash reference) must be provided externally as they
/// are not part of the manifest.
pub fn dataset(reference: HashReference, manifest: Manifest) -> Dataset {
    Dataset {
        reference,
        kind: manifest.kind,
        start_block: Some(manifest.start_block),
        finalized_blocks_only: manifest.finalized_blocks_only,
        tables: tables::all(&manifest.network),
    }
}

/// Create a Solana extractor based on the provided configuration.
pub fn extractor(
    name: ProviderName,
    config: SolanaProviderConfig,
    meter: Option<&monitoring::telemetry::metrics::Meter>,
) -> Result<SolanaExtractor, ExtractorError> {
    if config.network != "mainnet" {
        let err = format!(
            "unsupported Solana network: {}. Only 'mainnet' is supported.",
            config.network
        );
        return Err(ExtractorError(err));
    }

    // Resolve authentication configuration
    let auth = config.auth_token.map(|token| match config.auth_header {
        Some(header) => rpc_client::Auth::CustomHeader {
            name: header.into_inner(),
            value: token.into_inner(),
        },
        None => rpc_client::Auth::Bearer(token.into_inner()),
    });

    let client = match config.rpc_provider_url.scheme() {
        "http" | "https" => SolanaExtractor::new(
            config.rpc_provider_url,
            auth,
            config.max_rpc_calls_per_second,
            config.network,
            name,
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

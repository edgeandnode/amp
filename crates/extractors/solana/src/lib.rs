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
use amp_providers_solana::config::AuthHeaderName;
use datasets_common::{
    block_num::BlockNum, hash_reference::HashReference, manifest::TableSchema,
    network_id::NetworkId,
};

mod client;
mod dataset;
mod dataset_kind;
pub mod error;
pub mod metrics;
pub mod of1_client;
pub mod rpc_client;
pub mod tables;

pub use amp_providers_solana::config::{self, SolanaProviderConfig, UseArchive};
use solana_client::rpc_config::CommitmentConfig;

pub use self::{
    client::{Client, non_empty_of1_slot, non_empty_rpc_slot},
    dataset::Dataset,
    dataset_kind::{SolanaDatasetKind, SolanaDatasetKindError},
};
use crate::error::ClientError;

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

/// Create a Solana client based on the provided configuration.
pub fn client(
    name: ProviderName,
    config: SolanaProviderConfig,
    meter: Option<&monitoring::telemetry::metrics::Meter>,
) -> Result<Client, ClientError> {
    if config.network != "mainnet" {
        let err = format!(
            "unsupported Solana network: {}. Only 'mainnet' is supported.",
            config.network
        );
        return Err(ClientError(err));
    }

    match config.rpc_provider_info.url.scheme() {
        "http" | "https" => {}
        scheme => {
            let err = format!("unsupported Solana RPC provider URL scheme: {}", scheme);
            return Err(ClientError(err));
        }
    }
    match config
        .fallback_rpc_provider_info
        .as_ref()
        .map(|info| info.url.scheme())
    {
        Some("http") | Some("https") | None => {}
        Some(scheme) => {
            let err = format!(
                "unsupported Solana fallback RPC provider URL scheme: {}",
                scheme
            );
            return Err(ClientError(err));
        }
    }

    let commitment = commitment_config(config.commitment);

    // Resolve authentication configuration
    let main_rpc_provider_auth = config.rpc_provider_info.auth_token.map(|token| {
        let token = token.into_inner();
        let header = config
            .rpc_provider_info
            .auth_header
            .map(AuthHeaderName::into_inner);
        rpc_client::Auth::new(token, header)
    });

    let main_rpc_connection_info = rpc_client::RpcProviderConnectionInfo {
        url: config.rpc_provider_info.url,
        auth: main_rpc_provider_auth,
    };
    let fallback_rpc_connection_info = config.fallback_rpc_provider_info.map(|info| {
        let auth = info.auth_token.map(|token| {
            let token = token.into_inner();
            let header = info.auth_header.map(AuthHeaderName::into_inner);
            rpc_client::Auth::new(token, header)
        });
        rpc_client::RpcProviderConnectionInfo {
            url: info.url,
            auth,
        }
    });

    let client = Client::new(
        main_rpc_connection_info,
        fallback_rpc_connection_info,
        config.max_rpc_calls_per_second,
        config.network,
        name,
        config.of1_car_directory,
        config.keep_of1_car_files,
        config.use_archive,
        meter,
        commitment,
    );

    Ok(client)
}

/// Convert a [`config::CommitmentLevel`] config value into a Solana [`CommitmentConfig`].
pub fn commitment_config(level: config::CommitmentLevel) -> CommitmentConfig {
    match level {
        config::CommitmentLevel::Processed => CommitmentConfig::processed(),
        config::CommitmentLevel::Confirmed => CommitmentConfig::confirmed(),
        config::CommitmentLevel::Finalized => CommitmentConfig::finalized(),
    }
}

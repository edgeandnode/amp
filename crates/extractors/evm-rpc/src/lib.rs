use std::{collections::BTreeMap, num::NonZeroU32, path::PathBuf};

use datasets_common::{block_num::BlockNum, hash_reference::HashReference, network_id::NetworkId};
use serde_with::serde_as;
use url::Url;

mod client;
mod dataset;
mod dataset_kind;
pub mod error;
pub mod metrics;
pub mod provider;
pub mod tables;

// Reuse types from datasets-common for consistency
pub use datasets_common::manifest::{ArrowSchema, Field, TableSchema};

pub use self::{
    client::JsonRpcClient,
    dataset::Dataset,
    dataset_kind::{EvmRpcDatasetKind, EvmRpcDatasetKindError},
};
use crate::error::ProviderError;

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

#[serde_as]
#[derive(Debug, serde::Deserialize)]
pub struct ProviderConfig {
    pub name: String,
    pub kind: EvmRpcDatasetKind,
    pub network: NetworkId,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub url: Url,
    pub concurrent_request_limit: Option<u16>,
    /// Maximum number of json-rpc requests to batch together.
    #[serde(default)]
    pub rpc_batch_size: usize,
    pub rate_limit_per_minute: Option<NonZeroU32>,
    /// Whether to use `eth_getTransactionReceipt` to fetch receipts for each transaction
    /// or `eth_getBlockReceipts` to fetch all receipts for a block in one call.
    #[serde(default)]
    pub fetch_receipts_per_tx: bool,
}

/// Convert an EVM RPC manifest into a logical dataset representation.
///
/// Dataset identity (namespace, name, version, hash reference) must be provided externally as they
/// are not part of the manifest.
pub fn dataset(reference: HashReference, manifest: Manifest) -> crate::dataset::Dataset {
    let network = manifest.network;
    crate::dataset::Dataset {
        reference,
        kind: manifest.kind,
        start_block: Some(manifest.start_block),
        finalized_blocks_only: manifest.finalized_blocks_only,
        tables: tables::all(&network),
    }
}

pub async fn client(
    config: ProviderConfig,
    meter: Option<&monitoring::telemetry::metrics::Meter>,
) -> Result<JsonRpcClient, ProviderError> {
    let request_limit = u16::max(1, config.concurrent_request_limit.unwrap_or(1024));
    let client = match config.url.scheme() {
        "ipc" => {
            let path = config.url.path();
            JsonRpcClient::new_ipc(
                PathBuf::from(path),
                config.network,
                config.name,
                request_limit,
                config.rpc_batch_size,
                config.rate_limit_per_minute,
                config.fetch_receipts_per_tx,
                meter,
            )
            .await?
        }
        "ws" | "wss" => {
            JsonRpcClient::new_ws(
                config.url,
                config.network,
                config.name,
                request_limit,
                config.rpc_batch_size,
                config.rate_limit_per_minute,
                config.fetch_receipts_per_tx,
                meter,
            )
            .await?
        }
        _ => JsonRpcClient::new(
            config.url,
            config.network,
            config.name,
            request_limit,
            config.rpc_batch_size,
            config.rate_limit_per_minute,
            config.fetch_receipts_per_tx,
            meter,
        )?,
    };

    Ok(client)
}

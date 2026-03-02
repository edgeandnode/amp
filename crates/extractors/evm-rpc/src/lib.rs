use std::{collections::BTreeMap, path::PathBuf};

use amp_providers_common::provider_name::ProviderName;
use amp_providers_evm_rpc::{config::EvmRpcProviderConfig, provider::Auth};
use datasets_common::{block_num::BlockNum, hash_reference::HashReference, network_id::NetworkId};

mod client;
mod dataset_kind;
pub mod error;
pub mod metrics;
pub mod tables;

// Reuse types from datasets-common for consistency
pub use datasets_common::manifest::{ArrowSchema, Field, TableSchema};
use datasets_raw::dataset::Dataset as RawDataset;

pub use self::{
    client::Client,
    dataset_kind::{EvmRpcDatasetKind, EvmRpcDatasetKindError},
};
use crate::error::ClientError;

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

pub async fn client(
    name: ProviderName,
    config: EvmRpcProviderConfig,
    meter: Option<&monitoring::telemetry::metrics::Meter>,
) -> Result<Client, ClientError> {
    let url = config.url.into_inner();
    let auth = config.auth_token.map(|token| match config.auth_header {
        Some(header) => Auth::CustomHeader {
            name: header,
            value: token,
        },
        None => Auth::Bearer(token),
    });

    let request_limit = u16::max(1, config.concurrent_request_limit.unwrap_or(1024));
    let client = match url.scheme() {
        "ipc" => {
            let path = url.path();
            Client::new_ipc(
                PathBuf::from(path),
                config.network,
                name,
                request_limit,
                config.rpc_batch_size,
                config.rate_limit_per_minute,
                config.fetch_receipts_per_tx,
                meter,
            )
            .await?
        }
        "ws" | "wss" => {
            Client::new_ws(
                url,
                config.network,
                name,
                request_limit,
                config.rpc_batch_size,
                config.rate_limit_per_minute,
                config.fetch_receipts_per_tx,
                auth,
                meter,
            )
            .await?
        }
        _ => Client::new(
            url,
            config.network,
            name,
            request_limit,
            config.rpc_batch_size,
            config.rate_limit_per_minute,
            config.fetch_receipts_per_tx,
            config.timeout,
            auth,
            meter,
        )?,
    };

    Ok(client)
}

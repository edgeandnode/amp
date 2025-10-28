use std::{collections::BTreeMap, num::NonZeroU32, path::PathBuf};

use common::{BlockNum, BoxError, Dataset, store::StoreError};
use datasets_common::{name::Name, namespace::Namespace, version::Version};
use serde_with::serde_as;
use url::Url;

mod client;
mod dataset_kind;
pub mod metrics;
pub mod tables;

// Reuse types from datasets-common for consistency
pub use datasets_common::manifest::{ArrowSchema, Field, TableSchema};

pub use self::{
    client::JsonRpcClient,
    dataset_kind::{EvmRpcDatasetKind, EvmRpcDatasetKindError},
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

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("RPC client error: {0}")]
    Client(BoxError),
    #[error("store error: {0}")]
    StoreError(#[from] StoreError),
}

/// EVM RPC dataset manifest.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Manifest {
    /// Dataset kind, must be `evm-rpc`
    pub kind: EvmRpcDatasetKind,

    /// Network name, e.g., `anvil`, `mainnet`
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
    pub kind: EvmRpcDatasetKind,
    pub network: String,
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
/// Dataset identity (namespace, name, version) must be provided externally as they are not part
/// of the manifest.
pub fn dataset(namespace: Namespace, name: Name, version: Version, manifest: Manifest) -> Dataset {
    let network = manifest.network;
    Dataset {
        namespace,
        name,
        version: Some(version),
        kind: manifest.kind.to_string(),
        start_block: Some(manifest.start_block),
        finalized_blocks_only: manifest.finalized_blocks_only,
        tables: tables::all(&network),
        network: Some(network),
        functions: vec![],
    }
}

pub async fn client(
    config: ProviderConfig,
    meter: Option<&monitoring::telemetry::metrics::Meter>,
) -> Result<JsonRpcClient, Error> {
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
            .await
            .map_err(Error::Client)?
        }
        "ws" | "wss" => JsonRpcClient::new_ws(
            config.url,
            config.network,
            config.name,
            request_limit,
            config.rpc_batch_size,
            config.rate_limit_per_minute,
            config.fetch_receipts_per_tx,
            meter,
        )
        .await
        .map_err(Error::Client)?,
        _ => JsonRpcClient::new(
            config.url,
            config.network,
            config.name,
            request_limit,
            config.rpc_batch_size,
            config.rate_limit_per_minute,
            config.fetch_receipts_per_tx,
            meter,
        )
        .map_err(Error::Client)?,
    };

    Ok(client)
}

/// Automatically generate a README.md file with the schema whenever tests are executed.
#[tokio::test]
async fn print_schema_to_readme() {
    fs_err::write(
        "../../../docs/schemas/evm-rpc.md",
        common::catalog::schema_to_markdown(tables::all("test_network")).await,
    )
    .unwrap();
}

use std::{num::NonZeroU32, path::PathBuf};

use common::{BlockNum, BoxError, Dataset, store::StoreError};
use datasets_common::{name::Name, version::Version};
use serde_with::serde_as;
use url::Url;

mod client;
mod dataset_kind;
pub mod metrics;
pub mod tables;

pub use self::{
    client::JsonRpcClient,
    dataset_kind::{EvmRpcDatasetKind, EvmRpcDatasetKindError},
};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("RPC client error: {0}")]
    Client(BoxError),
    #[error("store error: {0}")]
    StoreError(#[from] StoreError),
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Manifest {
    /// Dataset name
    pub name: Name,
    /// Dataset version, e.g., `1.0.0`
    #[serde(default)]
    pub version: Version,
    /// Dataset kind, must be `evm-rpc`
    pub kind: EvmRpcDatasetKind,
    /// Network name, e.g., `anvil`, `mainnet`
    pub network: String,

    /// Dataset start block
    #[serde(default)]
    pub start_block: BlockNum,
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

pub fn dataset(manifest: Manifest) -> Dataset {
    Dataset {
        name: manifest.name,
        version: Some(manifest.version),
        kind: manifest.kind.to_string(),
        start_block: Some(manifest.start_block),
        tables: tables::all(&manifest.network),
        network: Some(manifest.network),
        functions: vec![],
    }
}

pub async fn client(
    config: ProviderConfig,
    final_blocks_only: bool,
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
                final_blocks_only,
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
            final_blocks_only,
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
            final_blocks_only,
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

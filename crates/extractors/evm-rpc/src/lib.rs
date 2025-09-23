mod client;
pub mod tables;

use std::{num::NonZeroU32, path::PathBuf};

use alloy::transports::http::reqwest::Url;
pub use client::JsonRpcClient;
use common::{BlockNum, BoxError, Dataset, store::StoreError};

pub const DATASET_KIND: &str = "evm-rpc";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("RPC client error: {0}")]
    Client(BoxError),
    #[error("store error: {0}")]
    StoreError(#[from] StoreError),
    #[error("TOML parse error: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct DatasetDef {
    /// Dataset kind, must be `evm-rpc`.
    pub kind: String,
    /// Dataset name.
    pub name: String,
    /// Network name, e.g., `mainnet`.
    pub network: String,
    /// Dataset start block.
    #[serde(default)]
    pub start_block: BlockNum,
}

#[serde_with::serde_as]
#[derive(Debug, serde::Deserialize)]
pub(crate) struct EvmRpcProvider {
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub url: Url,
    pub concurrent_request_limit: Option<u16>,
    /// Maximum number of json-rpc requests to batch together.
    #[serde(default = "default_rpc_batch_size")]
    pub rpc_batch_size: usize,
    pub rate_limit_per_minute: Option<NonZeroU32>,
    /// Whether to use `eth_getTransactionReceipt` to fetch receipts for each transaction
    /// or `eth_getBlockReceipts` to fetch all receipts for a block in one call.
    #[serde(default)]
    pub fetch_receipts_per_tx: bool,
}

fn default_rpc_batch_size() -> usize {
    100
}

pub fn dataset(dataset_cfg: serde_json::Value) -> Result<Dataset, Error> {
    let def: DatasetDef = serde_json::from_value(dataset_cfg)?;
    Ok(Dataset {
        kind: def.kind,
        name: def.name,
        version: None,
        start_block: Some(def.start_block),
        tables: tables::all(&def.network),
        network: def.network,
        functions: vec![],
    })
}

pub async fn client(
    provider: toml::Value,
    network: String,
    provider_name: String,
    final_blocks_only: bool,
) -> Result<JsonRpcClient, Error> {
    let provider: EvmRpcProvider = provider.try_into()?;
    let request_limit = u16::max(1, provider.concurrent_request_limit.unwrap_or(1024));
    let client = match provider.url.scheme() {
        "ipc" => {
            let path = provider.url.path();
            JsonRpcClient::new_ipc(
                PathBuf::from(path),
                network,
                provider_name,
                request_limit,
                provider.rpc_batch_size,
                provider.rate_limit_per_minute,
                provider.fetch_receipts_per_tx,
                final_blocks_only,
            )
            .await
            .map_err(Error::Client)?
        }
        "ws" | "wss" => JsonRpcClient::new_ws(
            provider.url,
            network,
            provider_name,
            request_limit,
            provider.rpc_batch_size,
            provider.rate_limit_per_minute,
            provider.fetch_receipts_per_tx,
            final_blocks_only,
        )
        .await
        .map_err(Error::Client)?,
        _ => JsonRpcClient::new(
            provider.url,
            network,
            provider_name,
            request_limit,
            provider.rpc_batch_size,
            provider.rate_limit_per_minute,
            provider.fetch_receipts_per_tx,
            final_blocks_only,
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

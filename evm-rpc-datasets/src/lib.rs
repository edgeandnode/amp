mod client;
mod tables;

use alloy::transports::http::reqwest::Url;
pub use client::JsonRpcClient;
use common::{store::StoreError, BoxError, Dataset, DatasetWithProvider};
use serde::Deserialize;
use serde_with::serde_as;

pub const DATASET_KIND: &str = "evm-rpc";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("RPC client error: {0}")]
    Client(BoxError),
    #[error("store error: {0}")]
    StoreError(#[from] StoreError),
    #[error("TOML parse error: {0}")]
    Toml(#[from] toml::de::Error),
}

#[derive(Debug, Deserialize)]
pub(crate) struct DatasetDef {
    pub kind: String,
    pub name: String,
    pub network: String,
}

#[serde_as]
#[derive(Debug, Deserialize)]
pub(crate) struct EvmRpcProvider {
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub url: Url,
    pub concurrent_request_limit: Option<u16>,
    /// Maximum number of json-rpc requests to batch together.
    #[serde(default = "default_rpc_batch_size")]
    pub rpc_batch_size: usize,
}

fn default_rpc_batch_size() -> usize {
    100
}

pub fn dataset(
    dataset_cfg: toml::Value,
    provider: toml::Value,
) -> Result<DatasetWithProvider, Error> {
    let def: DatasetDef = dataset_cfg.try_into()?;
    Ok(DatasetWithProvider {
        dataset: Dataset {
            kind: def.kind,
            name: def.name,
            tables: tables::all(&def.network),
            functions: vec![],
        },
        provider: Some(provider),
    })
}

pub async fn client(provider: toml::Value, network: String) -> Result<JsonRpcClient, Error> {
    let provider: EvmRpcProvider = provider.try_into()?;
    let request_limit = u16::max(1, provider.concurrent_request_limit.unwrap_or(1024));
    let client = JsonRpcClient::new(
        provider.url,
        network,
        request_limit,
        provider.rpc_batch_size,
    )
    .map_err(Error::Client)?;
    Ok(client)
}

/// Automatically generate a README.md file with the schema whenever tests are executed.
#[tokio::test]
async fn print_schema_to_readme() {
    fs_err::write(
        "src/README.md",
        common::catalog::schema_to_markdown(tables::all("test_network")).await,
    )
    .unwrap();
}

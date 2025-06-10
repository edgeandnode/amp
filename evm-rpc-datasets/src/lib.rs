mod client;
pub mod tables;

use alloy::transports::http::reqwest::Url;
pub use client::JsonRpcClient;
use common::{store::StoreError, BoxError, Dataset, DatasetWithProvider, Store};
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
    pub provider: String,
    pub network: String,
    pub concurrent_request_limit: Option<u16>,
}

#[serde_as]
#[derive(Debug, Deserialize)]
pub(crate) struct EvmRpcProvider {
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub url: Url,

    /// Maximum number of json-rpc requests to batch together.
    #[serde(default = "default_rpc_batch_size")]
    pub rpc_batch_size: usize,
}

fn default_rpc_batch_size() -> usize {
    100
}

pub fn dataset(dataset_cfg: toml::Value) -> Result<DatasetWithProvider, Error> {
    let def: DatasetDef = dataset_cfg.try_into()?;
    Ok(DatasetWithProvider {
        dataset: Dataset {
            kind: def.kind,
            name: def.name,
            tables: tables::all(&def.network),
            functions: vec![],
        },
        provider: Some(def.provider),
    })
}

pub async fn client(
    dataset_cfg: toml::Value,
    provider_store: &Store,
) -> Result<JsonRpcClient, Error> {
    let def: DatasetDef = dataset_cfg.try_into()?;
    let provider_string = provider_store.get_string(def.provider.as_str()).await?;
    let provider: EvmRpcProvider = toml::from_str(&provider_string)?;
    let request_limit = u16::max(1, def.concurrent_request_limit.unwrap_or(1024));
    let client = JsonRpcClient::new(
        provider.url,
        def.network,
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

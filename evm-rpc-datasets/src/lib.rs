mod client;
pub mod tables;

use std::num::NonZeroU32;

use alloy::transports::http::reqwest::Url;
pub use client::JsonRpcClient;
use common::{BoxError, Dataset, DatasetValue, store::StoreError};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json;
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
    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DatasetDef {
    /// Dataset kind, must be `evm-rpc`.
    pub kind: String,
    /// Dataset name.
    pub name: String,
    /// Network name, e.g., `mainnet`.
    pub network: String,
}

impl DatasetDef {
    fn from_value(value: common::DatasetValue) -> Result<Self, Error> {
        match value {
            DatasetValue::Toml(value) => value.try_into().map_err(From::from),
            DatasetValue::Json(value) => serde_json::from_value(value).map_err(From::from),
        }
    }
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
    pub rate_limit_per_minute: Option<NonZeroU32>,
}

fn default_rpc_batch_size() -> usize {
    100
}

pub fn dataset(dataset_cfg: common::DatasetValue) -> Result<Dataset, Error> {
    let def = DatasetDef::from_value(dataset_cfg)?;
    Ok(Dataset {
        kind: def.kind,
        name: def.name,
        tables: tables::all(&def.network),
        network: def.network,
        functions: vec![],
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
        provider.rate_limit_per_minute,
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

mod client;
mod tables;

use alloy::transports::http::reqwest::Url;
pub use client::JsonRpcClient;
use common::{store::StoreError, BoxError, Dataset, Store};
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
}

#[serde_as]
#[derive(Debug, Deserialize)]
pub(crate) struct EvmRpcProvider {
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub url: Url,
}

pub fn dataset(dataset_cfg: toml::Value) -> Result<Dataset, Error> {
    let def: DatasetDef = dataset_cfg.try_into()?;
    Ok(Dataset {
        kind: def.kind,
        name: def.name,
        tables: tables::all(&def.network),
    })
}

pub async fn client(
    dataset_cfg: toml::Value,
    provider_store: &Store,
) -> Result<JsonRpcClient, Error> {
    let def: DatasetDef = dataset_cfg.try_into()?;
    let provider_string = provider_store.get_string(def.provider.as_str()).await?;
    let provider: EvmRpcProvider = toml::from_str(&provider_string)?;
    let client = JsonRpcClient::new(provider.url, def.network).map_err(Error::Client)?;
    Ok(client)
}

/// Automatically generate a README.md file with the schema whenever tests are executed.
#[tokio::test]
async fn print_schema_to_readme() {
    fs_err::write(
        "src/README.md",
        common::catalog::schema_to_markdown(
            tables::all("test_network"),
            crate::DATASET_KIND.to_string(),
        )
        .await
        .unwrap(),
    )
    .unwrap();
}

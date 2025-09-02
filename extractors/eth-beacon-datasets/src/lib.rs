pub mod blocks;
pub mod client;

use std::num::NonZeroU32;

use common::{BoxError, Dataset, DatasetValue, store::StoreError};
use reqwest::Url;
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json;
use serde_with::serde_as;

pub use crate::client::BeaconClient;

pub const DATASET_KIND: &str = "eth-beacon";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("HTTP client error: {0}")]
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
    /// Dataset kind, must be `eth-beacon`.
    pub kind: String,
    /// Dataset name.
    pub name: String,
    /// Network name, e.g., `mainnet-beacon`.
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
pub(crate) struct EthBeaconProvider {
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub url: Url,
    pub concurrent_request_limit: Option<u16>,
    pub rate_limit_per_minute: Option<NonZeroU32>,
}

pub fn dataset(dataset_cfg: common::DatasetValue) -> Result<Dataset, Error> {
    let def = DatasetDef::from_value(dataset_cfg)?;
    Ok(Dataset {
        kind: def.kind,
        name: def.name,
        version: None,
        tables: vec![blocks::table(def.network.clone())],
        network: def.network,
        functions: vec![],
    })
}

pub fn client(
    provider: toml::Value,
    network: String,
    final_blocks_only: bool,
) -> Result<BeaconClient, Error> {
    let provider: EthBeaconProvider = provider.try_into()?;
    Ok(BeaconClient::new(network, provider.url, final_blocks_only))
}

#[tokio::test]
async fn print_schema_to_readme() {
    fs_err::write(
        "../../docs/schemas/eth-beacon.md",
        common::catalog::schema_to_markdown(vec![blocks::table("test_network".to_string())]).await,
    )
    .unwrap();
}

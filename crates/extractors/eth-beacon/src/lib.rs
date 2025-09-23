mod block;
mod client;

use std::num::NonZeroU32;

use common::BlockNum;
use reqwest::Url;

pub use crate::client::BeaconClient;

pub const DATASET_KIND: &str = "eth-beacon";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("TOML parse error: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct DatasetDef {
    /// Dataset kind, must be `eth-beacon`.
    pub kind: String,
    /// Dataset name.
    pub name: String,
    /// Network name, e.g., `mainnet-beacon`.
    pub network: String,
    /// Dataset start block.
    #[serde(default)]
    pub start_block: BlockNum,
}

#[serde_with::serde_as]
#[derive(Debug, serde::Deserialize)]
pub(crate) struct EthBeaconProvider {
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub url: Url,
    pub concurrent_request_limit: Option<u16>,
    pub rate_limit_per_minute: Option<NonZeroU32>,
}

pub fn dataset(dataset_cfg: serde_json::Value) -> Result<common::Dataset, Error> {
    let def: DatasetDef = serde_json::from_value(dataset_cfg)?;
    Ok(common::Dataset {
        kind: def.kind,
        name: def.name,
        version: None,
        start_block: Some(def.start_block),
        tables: all_tables(def.network.clone()),
        network: def.network,
        functions: vec![],
    })
}

pub fn all_tables(network: String) -> Vec<common::Table> {
    vec![block::table(network)]
}

pub fn client(
    provider: toml::Value,
    network: String,
    provider_name: String,
    final_blocks_only: bool,
) -> Result<BeaconClient, Error> {
    let provider: EthBeaconProvider = provider.try_into()?;
    Ok(BeaconClient::new(
        provider.url,
        network,
        provider_name,
        u16::max(1, provider.concurrent_request_limit.unwrap_or(1024)),
        provider.rate_limit_per_minute,
        final_blocks_only,
    ))
}

#[tokio::test]
async fn print_schema_to_readme() {
    fs_err::write(
        "../../../docs/schemas/eth-beacon.md",
        common::catalog::schema_to_markdown(all_tables("test_network".to_string())).await,
    )
    .unwrap();
}

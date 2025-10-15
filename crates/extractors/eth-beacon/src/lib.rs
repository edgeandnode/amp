use std::num::NonZeroU32;

use common::{BlockNum, Dataset};
use datasets_common::{name::Name, version::Version};
use reqwest::Url;

mod block;
mod client;
mod dataset_kind;

pub use self::{
    client::BeaconClient,
    dataset_kind::{EthBeaconDatasetKind, EthBeaconDatasetKindError},
};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Manifest {
    /// Dataset name
    pub name: Name,
    /// Dataset version, e.g., `1.0.0`
    #[serde(default)]
    pub version: Version,
    /// Dataset kind, must be `eth-beacon`.
    pub kind: EthBeaconDatasetKind,
    /// Network name, e.g., `mainnet-beacon`.
    pub network: String,
    /// Dataset start block.
    #[serde(default)]
    pub start_block: BlockNum,
}

#[serde_with::serde_as]
#[derive(Debug, serde::Deserialize)]
pub struct ProviderConfig {
    pub name: String,
    pub kind: EthBeaconDatasetKind,
    pub network: String,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub url: Url,
    pub concurrent_request_limit: Option<u16>,
    pub rate_limit_per_minute: Option<NonZeroU32>,
}

pub fn dataset(manifest: Manifest) -> Dataset {
    Dataset {
        name: manifest.name,
        version: Some(manifest.version),
        kind: manifest.kind.to_string(),
        network: Some(manifest.network.clone()),
        start_block: Some(manifest.start_block),
        tables: all_tables(manifest.network),
        functions: vec![],
    }
}

pub fn all_tables(network: String) -> Vec<common::Table> {
    vec![block::table(network)]
}

pub fn client(provider: ProviderConfig, final_blocks_only: bool) -> BeaconClient {
    BeaconClient::new(
        provider.url,
        provider.network,
        provider.name,
        u16::max(1, provider.concurrent_request_limit.unwrap_or(1024)),
        provider.rate_limit_per_minute,
        final_blocks_only,
    )
}

#[tokio::test]
async fn print_schema_to_readme() {
    fs_err::write(
        "../../../docs/schemas/eth-beacon.md",
        common::catalog::schema_to_markdown(all_tables("test_network".to_string())).await,
    )
    .unwrap();
}

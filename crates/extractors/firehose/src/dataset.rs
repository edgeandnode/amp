use common::BlockNum;
use datasets_common::{name::Name, version::Version};

use crate::dataset_kind::FirehoseDatasetKind;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Manifest {
    /// Dataset name
    pub name: Name,
    /// Dataset version, e.g., `1.0.0`
    #[serde(default)]
    pub version: Version,
    /// Dataset kind, must be `firehose`.
    pub kind: FirehoseDatasetKind,
    /// Network name, e.g., `mainnet`.
    pub network: String,
    /// Dataset start block.
    #[serde(default)]
    pub start_block: BlockNum,
}

#[derive(Debug, serde::Deserialize)]
pub struct ProviderConfig {
    pub name: String,
    pub kind: FirehoseDatasetKind,
    pub network: String,
    pub url: String,
    pub token: Option<String>,
}

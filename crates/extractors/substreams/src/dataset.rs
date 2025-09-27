use datasets_common::{name::Name, version::Version};

pub use crate::dataset_kind::DATASET_KIND;
use crate::dataset_kind::SubstreamsDatasetKind;

#[derive(Debug, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Manifest {
    /// Dataset name
    pub name: Name,
    /// Dataset version, e.g., `1.0.0`
    #[serde(default)]
    pub version: Version,
    /// Dataset kind, must be `substreams`.
    pub kind: SubstreamsDatasetKind,
    /// Network name, e.g., `mainnet`.
    pub network: String,
    /// Substreams package manifest URL.
    pub manifest: String,
    /// Substreams output module name.
    pub module: String,
}

#[derive(Debug, serde::Deserialize)]
pub struct ProviderConfig {
    pub name: String,
    pub kind: SubstreamsDatasetKind,
    pub network: String,
    pub url: String,
    pub token: Option<String>,
}

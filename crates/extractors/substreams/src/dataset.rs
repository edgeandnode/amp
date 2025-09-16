use schemars::JsonSchema;
use serde::Deserialize;
pub const DATASET_KIND: &str = "substreams";

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DatasetDef {
    /// Dataset kind, must be `substreams`.
    pub kind: String,
    /// Network name, e.g., `mainnet`.
    pub network: String,
    /// Dataset name.
    pub name: String,

    /// Substreams package manifest URL.
    pub manifest: String,

    /// Substreams output module name.
    pub module: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SubstreamsProvider {
    pub url: String,
    pub token: Option<String>,
}

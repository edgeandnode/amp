pub const DATASET_KIND: &str = "substreams";

#[derive(Debug, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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

#[derive(Debug, serde::Deserialize)]
pub(crate) struct SubstreamsProvider {
    pub url: String,
    pub token: Option<String>,
}

use serde::Deserialize;

pub const DATASET_KIND: &str = "substreams";

#[derive(Debug, Deserialize)]
pub(crate) struct DatasetDef {
    pub kind: String,
    pub network: String,
    pub name: String,

    /// Substreams package manifest URL
    pub manifest: String,

    /// Substreams output module name
    pub module: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SubstreamsProvider {
    pub url: String,
    pub token: Option<String>,
}

use common::DatasetValue;
use firehose_datasets::Error;
use serde::Deserialize;
pub const DATASET_KIND: &str = "substreams";

#[derive(Debug, Deserialize)]
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

impl DatasetDef {
    pub fn from_value(value: common::DatasetValue) -> Result<Self, Error> {
        match value {
            DatasetValue::Toml(value) => value.try_into().map_err(From::from),
            DatasetValue::Json(value) => serde_json::from_value(value).map_err(From::from),
        }
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct SubstreamsProvider {
    pub url: String,
    pub token: Option<String>,
}

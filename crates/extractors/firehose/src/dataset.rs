use common::BlockNum;
use datasets_common::value::ManifestValue;
use serde::Deserialize;

use crate::Error;

pub const DATASET_KIND: &str = "firehose";

#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct DatasetDef {
    /// Dataset kind, must be `firehose`.
    pub kind: String,
    /// Dataset name.
    pub name: String,
    /// Network name, e.g., `mainnet`.
    pub network: String,
    /// Dataset start block.
    #[serde(default)]
    pub start_block: BlockNum,
}

impl DatasetDef {
    pub fn from_value(value: ManifestValue) -> Result<Self, Error> {
        match value {
            ManifestValue::Toml(value) => value.try_into().map_err(From::from),
            ManifestValue::Json(value) => serde_json::from_value(value).map_err(From::from),
        }
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct FirehoseProvider {
    pub url: String,
    pub token: Option<String>,
}

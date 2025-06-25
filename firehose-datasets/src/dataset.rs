use common::DatasetValue;
use serde::Deserialize;

use crate::Error;

pub const DATASET_KIND: &str = "firehose";

#[derive(Debug, Deserialize)]
pub(crate) struct DatasetDef {
    pub kind: String,
    pub name: String,
    pub network: String,
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
pub(crate) struct FirehoseProvider {
    pub url: String,
    pub token: Option<String>,
}

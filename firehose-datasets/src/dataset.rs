use serde::Deserialize;

pub const DATASET_KIND: &str = "firehose";

#[derive(Debug, Deserialize)]
pub(crate) struct DatasetDef {
    pub kind: String,
    pub name: String,
    pub network: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct FirehoseProvider {
    pub url: String,
    pub token: Option<String>,
}

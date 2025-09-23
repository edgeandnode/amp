use common::BlockNum;

pub const DATASET_KIND: &str = "firehose";

#[derive(Debug, serde::Deserialize)]
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

#[derive(Debug, serde::Deserialize)]
pub(crate) struct FirehoseProvider {
    pub url: String,
    pub token: Option<String>,
}

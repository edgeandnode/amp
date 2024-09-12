mod client;

use alloy::transports::http::reqwest::Url;
pub use client::JsonRpcClient;
use common::{BoxError, Dataset};
use serde::Deserialize;
use serde_with::serde_as;

pub const DATASET_KIND: &str = "evm-rpc";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("RPC client error: {0}")]
    Client(BoxError),
    #[error("TOML parse error: {0}")]
    Toml(#[from] toml::de::Error),
}

#[serde_as]
#[derive(Debug, Deserialize)]
pub(crate) struct DatasetDef {
    pub kind: String,
    pub name: String,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub provider: Url,
    pub network: String,
}

pub fn dataset(dataset_cfg: toml::Value) -> Result<Dataset, Error> {
    let def: DatasetDef = dataset_cfg.try_into()?;
    Ok(Dataset {
        kind: def.kind,
        name: def.name,
        tables: vec![
            common::evm::tables::blocks::table(def.network.clone()),
            common::evm::tables::logs::table(def.network),
        ],
    })
}

pub fn client(dataset_cfg: toml::Value) -> Result<JsonRpcClient, Error> {
    let def: DatasetDef = dataset_cfg.try_into()?;
    let client = JsonRpcClient::new(def.provider, def.network).map_err(Error::Client)?;
    Ok(client)
}

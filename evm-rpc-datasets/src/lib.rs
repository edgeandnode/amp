mod client;

pub use client::JsonRpcClient;
use common::{BoxError, Dataset};
use serde::Deserialize;

pub const DATASET_KIND: &str = "evm-rpc";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("RPC client error: {0}")]
    Client(BoxError),
    #[error("TOML parse error: {0}")]
    Toml(#[from] toml::de::Error),
}

#[derive(Debug, Deserialize)]
pub(crate) struct DatasetDef {
    pub kind: String,
    pub name: String,
    pub provider: String,
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
    let client = JsonRpcClient::new(&def.provider, def.network).map_err(Error::Client)?;
    Ok(client)
}

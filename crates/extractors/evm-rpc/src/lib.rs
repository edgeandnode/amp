use std::{num::NonZeroU32, path::PathBuf};

use common::{BlockNum, BoxError, Dataset, store::StoreError};
use datasets_common::{
    name::Name,
    value::{ManifestValue, ManifestValueError},
    version::Version,
};
use serde_with::serde_as;
use url::Url;

mod client;
mod dataset_kind;
pub mod tables;

pub use self::{
    client::JsonRpcClient,
    dataset_kind::{DATASET_KIND, EvmRpcDatasetKind, EvmRpcDatasetKindError},
};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("RPC client error: {0}")]
    Client(BoxError),
    #[error("store error: {0}")]
    StoreError(#[from] StoreError),
    #[error("TOML parse error: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),
}

impl From<ManifestValueError> for Error {
    fn from(err: ManifestValueError) -> Self {
        match err {
            ManifestValueError::Toml(toml_err) => Error::Toml(toml_err),
            ManifestValueError::Json(json_err) => Error::Json(json_err),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Manifest {
    /// Dataset name
    pub name: Name,
    /// Dataset version, e.g., `1.0.0`
    #[serde(default)]
    pub version: Version,
    /// Dataset kind, must be `evm-rpc`
    pub kind: EvmRpcDatasetKind,
    /// Network name, e.g., `anvil`, `mainnet`
    pub network: String,

    /// Dataset start block
    #[serde(default)]
    pub start_block: BlockNum,
}

#[serde_as]
#[derive(Debug, serde::Deserialize)]
pub(crate) struct EvmRpcProvider {
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub url: Url,
    pub concurrent_request_limit: Option<u16>,
    /// Maximum number of json-rpc requests to batch together.
    #[serde(default)]
    pub rpc_batch_size: usize,
    pub rate_limit_per_minute: Option<NonZeroU32>,
    /// Whether to use `eth_getTransactionReceipt` to fetch receipts for each transaction
    /// or `eth_getBlockReceipts` to fetch all receipts for a block in one call.
    #[serde(default)]
    pub fetch_receipts_per_tx: bool,
}

pub fn dataset(value: ManifestValue) -> Result<Dataset, Error> {
    let manifest: Manifest = value.try_into_manifest()?;
    Ok(Dataset {
        name: manifest.name,
        version: Some(manifest.version),
        kind: manifest.kind.to_string(),
        start_block: Some(manifest.start_block),
        tables: tables::all(&manifest.network),
        network: manifest.network,
        functions: vec![],
    })
}

pub async fn client(
    provider: toml::Value,
    network: String,
    provider_name: String,
    final_blocks_only: bool,
    meter: Option<&monitoring::telemetry::metrics::Meter>,
) -> Result<JsonRpcClient, Error> {
    let provider: EvmRpcProvider = provider.try_into()?;
    let request_limit = u16::max(1, provider.concurrent_request_limit.unwrap_or(1024));
    let client = match provider.url.scheme() {
        "ipc" => {
            let path = provider.url.path();
            JsonRpcClient::new_ipc(
                PathBuf::from(path),
                network,
                provider_name,
                request_limit,
                provider.rpc_batch_size,
                provider.rate_limit_per_minute,
                provider.fetch_receipts_per_tx,
                final_blocks_only,
                meter,
            )
            .await
            .map_err(Error::Client)?
        }
        "ws" | "wss" => JsonRpcClient::new_ws(
            provider.url,
            network,
            provider_name,
            request_limit,
            provider.rpc_batch_size,
            provider.rate_limit_per_minute,
            provider.fetch_receipts_per_tx,
            final_blocks_only,
            meter,
        )
        .await
        .map_err(Error::Client)?,
        _ => JsonRpcClient::new(
            provider.url,
            network,
            provider_name,
            request_limit,
            provider.rpc_batch_size,
            provider.rate_limit_per_minute,
            provider.fetch_receipts_per_tx,
            final_blocks_only,
            meter,
        )
        .map_err(Error::Client)?,
    };

    Ok(client)
}

/// Automatically generate a README.md file with the schema whenever tests are executed.
#[tokio::test]
async fn print_schema_to_readme() {
    fs_err::write(
        "../../../docs/schemas/evm-rpc.md",
        common::catalog::schema_to_markdown(tables::all("test_network")).await,
    )
    .unwrap();
}

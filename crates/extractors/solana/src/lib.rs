//! Solana dataset extractor.
//!
//! ### Important note - Slot vs. Block Number
//!
//! In Solana, each produced block gets placed in a "slot", which is a specific time interval
//! during which a validator can propose a block. However, not every slot results in a produced
//! block; some slots may be skipped due to various reasons such as network issues or validator
//! performance. Therefore, the slot number does not always correspond directly to a block number.
//!
//! Since [`common::BlockStreamer`] and related infrastructure generally operate on the concept of block numbers,
//! this implementation treats Solana slots as block numbers for the most part. Skipped slots are handled
//! by yielding empty rows for those slots, ensuring that the sequence of block numbers remains continuous.

use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use common::{BlockNum, BoxError, Dataset, store::StoreError};
use datasets_common::manifest::TableSchema;
use serde_with::serde_as;
use solana_clock::Slot;
use solana_transaction_status_client_types::UiConfirmedBlock as SolanaConfirmedBlock;
use url::Url;

mod dataset_kind;
mod extractor;
mod metrics;
pub mod ring_buffer;
mod rpc_client;
mod subscription_task;
pub mod tables;

pub use self::{
    dataset_kind::{SolanaDatasetKind, SolanaDatasetKindError},
    extractor::SolanaExtractor,
};
use crate::ring_buffer::SolanaSlotRingBuffer;

/// A Solana slot and its corresponding confirmed block (if available).
pub(crate) type SolanaSlotAndBlock = (Slot, Option<SolanaConfirmedBlock>);

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("RPC client error: {0}")]
    Client(BoxError),
    #[error("store error: {0}")]
    StoreError(#[from] StoreError),
}

/// Table definition for raw datasets.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Table {
    /// Arrow schema for this table.
    pub schema: TableSchema,
    /// Network for this table.
    pub network: String,
}

impl Table {
    /// Create a new table with the given schema and network.
    pub fn new(schema: TableSchema, network: String) -> Self {
        Self { schema, network }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Manifest {
    /// Dataset kind, must be `solana`
    pub kind: SolanaDatasetKind,

    /// Network name, e.g., `mainnet`
    pub network: String,
    /// Dataset start block
    #[serde(default)]
    pub start_block: BlockNum,
    /// Only include finalized block data
    #[serde(default)]
    pub finalized_blocks_only: bool,

    /// Dataset tables. Maps table names to their definitions.
    pub tables: BTreeMap<String, Table>,
}

#[serde_as]
#[derive(Debug, serde::Deserialize)]
pub struct ProviderConfig {
    pub name: String,
    pub kind: SolanaDatasetKind,
    pub network: String,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub http_url: Url,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub ws_url: Url,
    pub of1_car_directory: String,
}

/// Convert a Solana manifest into a logical dataset representation.
///
/// Dataset identity (namespace, name, version) must be provided externally as they are not part
/// of the manifest.
pub fn dataset(manifest_hash: datasets_common::hash::Hash, manifest: Manifest) -> Dataset {
    Dataset {
        manifest_hash,
        dependencies: BTreeMap::new(),
        kind: manifest.kind.to_string(),
        start_block: Some(manifest.start_block),
        finalized_blocks_only: manifest.finalized_blocks_only,
        tables: tables::all(&manifest.network),
        network: Some(manifest.network),
        functions: vec![],
    }
}

/// Create a Solana extractor based on the provided configuration.
pub fn extractor(
    config: ProviderConfig,
    subscription_ring_buffer: Arc<Mutex<SolanaSlotRingBuffer>>,
    meter: Option<&monitoring::telemetry::metrics::Meter>,
) -> Result<SolanaExtractor, Error> {
    let of1_car_directory = PathBuf::from(&config.of1_car_directory);
    std::fs::create_dir_all(&of1_car_directory)?;

    let client = match config.http_url.scheme() {
        "http" | "https" => SolanaExtractor::new(
            config.http_url,
            config.network,
            config.name,
            of1_car_directory,
            subscription_ring_buffer,
            meter,
        )
        .map_err(Error::Client)?,
        scheme => {
            let err = format!("unsupported URL scheme: {}", scheme);
            return Err(Error::Client(err.into()));
        }
    };

    Ok(client)
}

/// Run the subscription task that listens for new slots via WebSocket and places them in the ring buffer.
pub fn run_subscription(
    ws_url: Url,
    subscription_ring_buffer: Arc<Mutex<SolanaSlotRingBuffer>>,
) -> tokio::task::JoinHandle<()> {
    subscription_task::spawn(ws_url, subscription_ring_buffer)
}

/// Automatically generate a README.md file with the schema whenever tests are executed.
#[tokio::test]
async fn print_schema_to_readme() {
    fs_err::write(
        "../../../docs/schemas/solana.md",
        common::catalog::schema_to_markdown(tables::all("test_network")).await,
    )
    .unwrap();
}

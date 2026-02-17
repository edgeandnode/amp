//! Canton Network extractor for AMP.
//!
//! This crate provides data extraction from the Canton Network via the Scan API.
//! The Scan API is an HTTP REST interface that provides access to ledger updates,
//! contract events, and other blockchain data from Canton Network validators.
//!
//! ## Data Model
//!
//! Canton uses `offset` for ordering instead of traditional block numbers.
//! The offset is mapped to AMP's `_block_num` column for compatibility.
//!
//! ## Tables
//!
//! Schemas are designed to match the CSV data samples from canton-network-validator:
//!
//! - `transactions` - Transaction metadata (01_transactions.csv)
//! - `contracts_created` - Contract creation events (03_contracts_created.csv)
//! - `choices_exercised` - Choice exercise events (04_choices_exercised.csv)
//! - `mining_rounds` - Mining round lifecycle (05_mining_rounds.csv)
//!
//! ## API Reference
//!
//! This extractor connects to the Splice Scan API documented at:
//! <https://docs.dev.sync.global/app_dev/scan_api/>

use std::collections::BTreeMap;

use datasets_common::{block_num::BlockNum, hash_reference::HashReference, network_id::NetworkId};
use serde_with::serde_as;
use url::Url;

mod client;
pub mod csv_loader;
mod dataset;
mod dataset_kind;
pub mod error;
pub mod tables;

// Reuse types from datasets-common for consistency
pub use datasets_common::manifest::{ArrowSchema, Field, TableSchema};

pub use self::{
    client::Client,
    dataset::Dataset,
    dataset_kind::{CantonScanDatasetKind, CantonScanDatasetKindError},
};
use crate::error::ProviderError;

/// Table definition for raw datasets
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Table {
    /// Arrow schema for this table
    pub schema: TableSchema,
    /// Network for this table
    pub network: NetworkId,
}

impl Table {
    /// Create a new table with the given schema and network
    pub fn new(schema: TableSchema, network: NetworkId) -> Self {
        Self { schema, network }
    }
}

/// Canton Scan API dataset manifest.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Manifest {
    /// Dataset kind, must be `canton-scan`
    pub kind: CantonScanDatasetKind,

    /// Network name, e.g., `canton-devnet`, `canton-testnet`
    pub network: NetworkId,

    /// Dataset start offset (mapped to block number)
    #[serde(default)]
    pub start_block: BlockNum,

    /// Only include finalized data (Canton has instant finality, so this is always true)
    #[serde(default = "default_finalized")]
    pub finalized_blocks_only: bool,

    /// Dataset tables. Maps table names to their definitions.
    pub tables: BTreeMap<String, Table>,
}

fn default_finalized() -> bool {
    true
}

/// Provider configuration for Canton Scan API endpoints.
#[serde_as]
#[derive(Debug, serde::Deserialize)]
pub struct ProviderConfig {
    /// Provider name (unique identifier)
    pub name: String,
    /// Dataset kind, must be `canton-scan`
    pub kind: CantonScanDatasetKind,
    /// Network identifier
    pub network: NetworkId,
    /// Scan API base URL (e.g., `https://scan.canton.network`)
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub scan_url: Url,
    /// Request timeout in seconds (default: 60)
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
    /// Page size for pagination (default: 100)
    #[serde(default = "default_page_size")]
    pub page_size: u32,
}

fn default_timeout() -> u64 {
    60
}

fn default_page_size() -> u32 {
    100
}

/// Convert a Canton Scan manifest into a logical dataset representation.
///
/// Dataset identity (namespace, name, version, hash reference) must be provided externally as they
/// are not part of the manifest.
pub fn dataset(reference: HashReference, manifest: Manifest) -> crate::dataset::Dataset {
    let network = manifest.network;
    crate::dataset::Dataset {
        reference,
        kind: manifest.kind,
        start_block: Some(manifest.start_block),
        finalized_blocks_only: manifest.finalized_blocks_only,
        tables: tables::all(&network),
    }
}

/// Create a Canton Scan API client from provider configuration.
pub async fn client(config: ProviderConfig) -> Result<Client, ProviderError> {
    Client::new(config).await.map_err(ProviderError::Client)
}

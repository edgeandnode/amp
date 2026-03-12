//! Raw dataset manifest types.

use std::collections::BTreeMap;

use datasets_common::{block_num::BlockNum, manifest::TableSchema, network_id::NetworkId};

use crate::dataset_kind::{EvmRpcDatasetKind, FirehoseDatasetKind, SolanaDatasetKind};

/// EVM-RPC dataset manifest.
pub type EvmRpcManifest = Manifest<EvmRpcDatasetKind>;

/// Firehose dataset manifest.
pub type FirehoseManifest = Manifest<FirehoseDatasetKind>;

/// Solana dataset manifest.
pub type SolanaManifest = Manifest<SolanaDatasetKind>;

/// Generic raw dataset manifest, parameterized by dataset kind `K`.
///
/// All raw datasets share the same structure: a typed kind, network identifier,
/// optional start block, finalization preference, and table definitions.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Manifest<K> {
    /// Dataset kind identifier.
    pub kind: K,

    /// Network name.
    #[cfg_attr(
        feature = "schemars",
        schemars(example = &"mainnet", example = &"base", example = &"solana-mainnet")
    )]
    pub network: NetworkId,

    /// Dataset start block.
    #[serde(default)]
    pub start_block: BlockNum,

    /// Only include finalized block data.
    #[serde(default)]
    pub finalized_blocks_only: bool,

    /// Dataset tables. Maps table names to their definitions.
    pub tables: BTreeMap<String, Table>,
}

/// Table definition for raw dataset manifests.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Table {
    /// Arrow schema for this table.
    pub schema: TableSchema,
    /// Network for this table.
    pub network: NetworkId,
}

/// Schema generation types, gated behind the `schemars` feature.
#[cfg(feature = "schemars")]
pub mod schema {
    use super::*;

    /// Manifest type using the unified `RawDatasetKind` enum for JSON schema generation.
    ///
    /// Produces a single schema where `kind` accepts any of `"evm-rpc"`, `"firehose"`, `"solana"`.
    pub type RawManifestSchema = Manifest<RawDatasetKind>;

    /// Unified enum of all raw dataset kinds, used for JSON schema generation.
    ///
    /// This enum produces `{"enum": ["evm-rpc", "firehose", "solana"]}` in the JSON schema,
    /// allowing a single schema file to cover all raw dataset kinds.
    #[derive(Debug, schemars::JsonSchema, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "kebab-case")]
    pub enum RawDatasetKind {
        EvmRpc,
        Firehose,
        Solana,
    }
}

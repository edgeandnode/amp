use std::{num::NonZeroU32, path::PathBuf};

use amp_providers_common::{network_id::NetworkId, redacted::Redacted};
use url::Url;

use crate::kind::SolanaProviderKind;

/// Solana provider configuration for parsing TOML config.
///
/// This structure defines the parameters required to connect to a Solana
/// RPC endpoint for blockchain data extraction. The `kind` field validates
/// that the config belongs to a `solana` provider at deserialization time.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct SolanaProviderConfig {
    /// The provider kind, must be `"solana"`.
    pub kind: SolanaProviderKind,

    /// The network this provider serves.
    pub network: NetworkId,

    /// The URL of the Solana RPC endpoint.
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub rpc_provider_url: Redacted<Url>,

    /// Optional rate limit for RPC calls per second.
    pub max_rpc_calls_per_second: Option<NonZeroU32>,

    /// Directory for storing Old Faithful ONE CAR files.
    pub of1_car_directory: PathBuf,

    /// Whether to keep downloaded CAR files after processing.
    #[serde(default)]
    pub keep_of1_car_files: bool,

    /// Controls when to use the Solana archive for historical data.
    #[serde(default)]
    pub use_archive: UseArchive,
}

/// Configures when to use the Solana archive for fetching historical data.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum UseArchive {
    /// Automatically determine whether to use the archive based on block age.
    Auto,
    /// Always use the archive, even for recent blocks.
    #[default]
    Always,
    /// Never use the archive, fetch all blocks from the RPC provider.
    Never,
}

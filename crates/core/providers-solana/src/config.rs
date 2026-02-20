use std::{num::NonZeroU32, path::PathBuf};

use amp_providers_common::{network_id::NetworkId, redacted::Redacted};
use headers::{HeaderName, HeaderValue};
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

    /// Custom header name for authentication.
    ///
    /// When set alongside `auth_token`, sends `<auth_header>: <auth_token>` as a raw header
    /// instead of the default `Authorization: Bearer <auth_token>`.
    /// Ignored if `auth_token` is not set.
    #[serde(default)]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<String>"))]
    pub auth_header: Option<AuthHeaderName>,

    /// Authentication token for RPC requests.
    ///
    /// Default: sent as `Authorization: Bearer <token>`.
    /// With `auth_header`: sent as `<auth_header>: <token>`.
    #[serde(default)]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<String>"))]
    pub auth_token: Option<AuthToken>,

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

/// Validated HTTP header name for custom authentication.
///
/// Stores a [`String`] that has been validated as a legal HTTP header name
/// at deserialization time (rejects empty strings, control characters,
/// separators, and non-ASCII bytes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthHeaderName(String);

impl AuthHeaderName {
    /// Consumes the wrapper and returns the inner string.
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl<'de> serde::Deserialize<'de> for AuthHeaderName {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        // Validate as a legal HTTP header name, but store the original string.
        HeaderName::try_from(&s).map_err(serde::de::Error::custom)?;
        Ok(AuthHeaderName(s))
    }
}

/// Validated authentication token for RPC requests.
///
/// Stores a [`Redacted<String>`] that has been validated as a legal HTTP
/// header value at deserialization time (rejects control characters and
/// non-ASCII bytes). The inner value is hidden in `Debug` output to
/// prevent credential leakage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthToken(Redacted<String>);

impl AuthToken {
    /// Consumes the wrapper and returns the inner string.
    pub fn into_inner(self) -> String {
        self.0.into_inner()
    }
}

impl<'de> serde::Deserialize<'de> for AuthToken {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        // Validate as a legal HTTP header value, but store the original string.
        HeaderValue::try_from(&s).map_err(serde::de::Error::custom)?;
        Ok(AuthToken(Redacted::from(s)))
    }
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

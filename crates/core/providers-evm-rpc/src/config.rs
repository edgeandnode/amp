use std::num::NonZeroU32;

use amp_providers_common::{network_id::NetworkId, redacted::Redacted};
use headers::{HeaderName, HeaderValue};
use url::Url;

use crate::kind::EvmRpcProviderKind;

/// EVM RPC provider configuration for parsing TOML config.
///
/// This struct contains only the fields needed for provider construction.
/// The `kind` field validates that the config belongs to an `evm-rpc` provider
/// at deserialization time.
#[derive(Debug, Clone, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct EvmRpcProviderConfig {
    /// The provider kind, must be `"evm-rpc"`.
    pub kind: EvmRpcProviderKind,

    /// The network this provider serves.
    pub network: NetworkId,

    /// The URL of the EVM RPC endpoint (HTTP, HTTPS, or IPC).
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub url: Redacted<Url>,

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

    /// Optional rate limit for requests per minute.
    pub rate_limit_per_minute: Option<NonZeroU32>,

    /// Optional limit on the number of concurrent requests.
    pub concurrent_request_limit: Option<u16>,

    /// Maximum number of JSON-RPC requests to batch together.
    #[serde(default)]
    pub rpc_batch_size: usize,

    /// Whether to use `eth_getTransactionReceipt` to fetch receipts for each transaction
    /// or `eth_getBlockReceipts` to fetch all receipts for a block in one call.
    #[serde(default)]
    pub fetch_receipts_per_tx: bool,

    /// Request timeout in seconds.
    ///
    /// Maximum time to wait for an RPC request to complete (including connection
    /// establishment and response). Requests exceeding this duration will be cancelled
    /// and treated as errors.
    ///
    /// Default: 30 seconds
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
}

/// Validated HTTP header name for custom authentication.
///
/// Stores a [`String`] that has been validated as a legal HTTP header name
/// at deserialization time (rejects empty strings, control characters,
/// separators, and non-ASCII bytes).
#[derive(Debug, Clone)]
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
#[derive(Debug, Clone)]
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

/// Default timeout for RPC requests (30 seconds)
fn default_timeout_secs() -> u64 {
    30
}

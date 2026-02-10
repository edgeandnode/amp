use std::num::NonZeroU32;

use amp_providers_common::network_id::NetworkId;

use crate::kind::EvmRpcProviderKind;

/// EVM RPC provider configuration for parsing TOML config.
///
/// This struct contains only the fields needed for provider construction.
/// The `kind` field validates that the config belongs to an `evm-rpc` provider
/// at deserialization time.
#[derive(Clone, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct EvmRpcProviderConfig {
    /// The provider kind, must be `"evm-rpc"`.
    pub kind: EvmRpcProviderKind,

    /// The network this provider serves.
    pub network: NetworkId,

    /// The URL of the EVM RPC endpoint (HTTP, HTTPS, or IPC).
    #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub url: url::Url,

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
}

impl std::fmt::Debug for EvmRpcProviderConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EvmRpcProviderConfig")
            .field("kind", &self.kind)
            .field("network", &self.network)
            .field("url", &"<redacted>")
            .field("rate_limit_per_minute", &self.rate_limit_per_minute)
            .field("concurrent_request_limit", &self.concurrent_request_limit)
            .field("rpc_batch_size", &self.rpc_batch_size)
            .field("fetch_receipts_per_tx", &self.fetch_receipts_per_tx)
            .finish()
    }
}

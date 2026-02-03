use std::{num::NonZeroU32, sync::Arc, time::Instant};

use datasets_common::network_id::NetworkId;
pub use solana_client::{
    rpc_config,
    rpc_response::UiReturnDataEncoding,
};
use solana_clock::Slot;
pub use solana_transaction_status_client_types::{
    EncodedTransaction, EncodedTransactionWithStatusMeta, Reward, TransactionStatusMeta,
    TransactionTokenBalance, UiConfirmedBlock, UiInstruction, UiMessage, UiRawMessage,
    UiTransaction, UiTransactionStatusMeta, UiTransactionTokenBalance,
};
use url::Url;

use crate::metrics;

/// A Solana JSON-RPC client.
///
/// If provided with a [meter](monitoring::telemetry::metrics::Meter), the client will record
/// JSON-RPC related metrics.
pub(crate) struct SolanaRpcClient {
    inner: solana_client::nonblocking::rpc_client::RpcClient,
    rate_limiter: Option<governor::DefaultDirectRateLimiter>,
    provider: String,
    network: NetworkId,
}

impl SolanaRpcClient {
    pub(crate) fn new(
        url: Url,
        max_calls_per_second: Option<NonZeroU32>,
        provider: String,
        network: NetworkId,
    ) -> Self {
        let inner = solana_client::nonblocking::rpc_client::RpcClient::new(url.to_string());
        let rate_limiter = max_calls_per_second.map(|max| {
            let quota = governor::Quota::per_second(max);
            governor::RateLimiter::direct(quota)
        });
        Self {
            inner,
            rate_limiter,
            provider,
            network,
        }
    }

    /// Get the current slot.
    ///
    /// ### RPC Reference
    ///
    /// [getSlot](https://solana.com/docs/rpc/http/getslot)
    pub(crate) async fn get_slot(
        &self,
        metrics: Option<Arc<metrics::MetricsRegistry>>,
    ) -> solana_rpc_client_api::client_error::Result<Slot> {
        let slot = self
            .rpc_call("getSlot", metrics, self.inner.get_slot())
            .await?;

        Ok(slot)
    }

    /// Get the current block height (latest slot).
    ///
    /// ### RPC Reference
    ///
    /// [getBlockHeight](https://solana.com/docs/rpc/http/getblockheight)
    pub(crate) async fn get_block_height(
        &self,
        metrics: Option<Arc<metrics::MetricsRegistry>>,
    ) -> solana_rpc_client_api::client_error::Result<u64> {
        let height = self
            .rpc_call("getBlockHeight", metrics, self.inner.get_block_height())
            .await?;

        Ok(height)
    }

    /// Get the confirmed block for a given slot.
    ///
    /// ### RPC Reference
    ///
    /// [getBlock](https://solana.com/docs/rpc/http/getblock)
    pub(crate) async fn get_block(
        &self,
        slot: Slot,
        config: rpc_config::RpcBlockConfig,
        metrics: Option<Arc<metrics::MetricsRegistry>>,
    ) -> solana_rpc_client_api::client_error::Result<UiConfirmedBlock> {
        let block = self
            .rpc_call(
                "getBlock",
                metrics,
                self.inner.get_block_with_config(slot, config),
            )
            .await?;

        Ok(block)
    }

    /// Use the [SolanaRpcClient] to execute a JSON-RPC call with the following additional behavior:
    ///   - Record metrics if enabled.
    ///   - Perform rate limiting if enabled.
    async fn rpc_call<T, E, Fut>(
        &self,
        method: &str,
        metrics: Option<Arc<metrics::MetricsRegistry>>,
        fut: Fut,
    ) -> Result<T, E>
    where
        Fut: Future<Output = Result<T, E>>,
    {
        if let Some(rate_limiter) = &self.rate_limiter {
            // Wait for a permit from the rate limiter.
            rate_limiter.until_ready().await;
        }

        let Some(metrics) = metrics else {
            // Execute the future without recording metrics.
            return fut.await;
        };

        let start = Instant::now();
        let resp = fut.await;

        let duration = start.elapsed().as_millis();
        let duration = u64::try_from(duration).unwrap_or(u64::MAX);

        metrics.record_rpc_request(duration, &self.provider, &self.network, method);
        if resp.is_err() {
            metrics.record_rpc_error(&self.provider, &self.network);
        }

        resp
    }
}

/// Returns `true` if the given error indicates that the block is missing for the requested slot.
///
/// ### Reference
///
/// <https://www.quicknode.com/docs/solana/error-references>
pub fn is_block_missing_err(err: &solana_rpc_client_api::client_error::Error) -> bool {
    matches!(
        err.kind(),
        solana_rpc_client_api::client_error::ErrorKind::RpcError(
            solana_rpc_client_api::request::RpcError::RpcResponseError {
                code: -32007 | -32009,
                ..
            },
        )
    )
}

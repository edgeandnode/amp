use std::{num::NonZeroU32, sync::Arc, time::Instant};

use solana_client::rpc_config;
use solana_clock::Slot;
use solana_transaction_status_client_types::UiConfirmedBlock as SolanaConfirmedBlock;
use url::Url;

use crate::metrics;

/// A Solana JSON-RPC client.
///
/// If provided with a [meter](monitoring::telemetry::metrics::Meter), the client will record
/// JSON-RPC related metrics.
#[derive(Clone)]
pub(crate) struct SolanaRpcClient {
    inner: Arc<solana_client::nonblocking::rpc_client::RpcClient>,
    metrics: Option<metrics::MetricsRegistry>,
    provider: String,
    network: String,
}

impl SolanaRpcClient {
    pub(crate) fn new(
        url: Url,
        provider: String,
        network: String,
        _rate_limit: Option<NonZeroU32>,
        meter: Option<&monitoring::telemetry::metrics::Meter>,
    ) -> Self {
        let inner = solana_client::nonblocking::rpc_client::RpcClient::new(url.to_string());
        let metrics = meter.map(metrics::MetricsRegistry::new);
        Self {
            inner: Arc::new(inner),
            metrics,
            provider,
            network,
        }
    }

    /// Get the current block height (latest slot).
    ///
    /// ### RPC Reference
    ///
    /// [getBlockHeight](https://solana.com/docs/rpc/http/getblockheight)
    pub(crate) async fn get_block_height(
        &self,
    ) -> solana_rpc_client_api::client_error::Result<u64> {
        let height = self
            .with_metrics("getBlockHeight", async |c| c.get_block_height().await)
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
    ) -> solana_rpc_client_api::client_error::Result<SolanaConfirmedBlock> {
        let block = self
            .with_metrics("getBlock", async |c| {
                c.get_block_with_config(slot, config).await
            })
            .await?;

        Ok(block)
    }

    /// Get the list of confirmed blocks between `start` and `end` slots.
    ///
    /// The range between `start` and `end` is inclusive and limited to [SOLANA_RPC_GET_BLOCKS_LIMIT]. Requesting
    /// more than this limit will result in an error.
    ///
    /// If `end` is `None`, returns blocks from `start` to the most recent confirmed block.
    ///
    /// ### RPC Reference
    ///
    /// [getBlocks](https://solana.com/docs/rpc/http/getblocks)
    pub(crate) async fn _get_blocks(
        &self,
        start: Slot,
        end: Option<Slot>,
    ) -> solana_rpc_client_api::client_error::Result<Vec<Slot>> {
        let blocks = self
            .with_metrics("getBlocks", async |c| c.get_blocks(start, end).await)
            .await?;

        Ok(blocks)
    }

    /// Use the [SolanaRpcClient] to execute a function, recording metrics if enabled.
    async fn with_metrics<T, E, F, Fut>(&self, method: &str, func: F) -> Fut::Output
    where
        F: FnOnce(Arc<solana_client::nonblocking::rpc_client::RpcClient>) -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        let Some(metrics) = self.metrics.as_ref() else {
            // Execute the function without recording metrics.
            return func(Arc::clone(&self.inner)).await;
        };

        let start = Instant::now();

        let resp = func(Arc::clone(&self.inner)).await;

        let duration = start.elapsed().as_millis() as f64;
        metrics.record_single_request(duration, &self.provider, &self.network, method);
        if resp.is_err() {
            metrics.record_error(&self.provider, &self.network);
        }

        resp
    }
}

/// Returns `true` if the given error indicates that the requested block is missing.
///
/// ### Reference
///
/// <https://www.quicknode.com/docs/solana/error-references>
pub fn is_block_missing_err(err: &solana_rpc_client_api::client_error::Error) -> bool {
    matches!(
        err.kind(),
        solana_rpc_client_api::client_error::ErrorKind::RpcError(
            solana_rpc_client_api::request::RpcError::RpcResponseError { code: -32009, .. },
        )
    )
}

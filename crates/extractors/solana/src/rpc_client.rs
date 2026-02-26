use std::{num::NonZeroU32, sync::Arc, time::Instant};

use amp_providers_common::{network_id::NetworkId, redacted::Redacted};
use headers::{Authorization, HeaderMap, HeaderMapExt as _, HeaderValue};
pub use solana_client::rpc_config;
use solana_clock::Slot;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
pub use solana_rpc_client_api::client_error;
use solana_transaction_status_client_types::EncodedConfirmedTransactionWithStatusMeta;
pub use solana_transaction_status_client_types::{
    EncodedTransaction, EncodedTransactionWithStatusMeta, Reward, TransactionStatusMeta,
    TransactionTokenBalance, UiConfirmedBlock, UiInstruction, UiMessage, UiRawMessage,
    UiReturnDataEncoding, UiTransaction, UiTransactionStatusMeta, UiTransactionTokenBalance,
};
use url::Url;

use crate::metrics;

/// Runtime connection details for a Solana RPC endpoint.
pub struct RpcProviderConnectionInfo {
    pub url: Redacted<Url>,
    pub auth: Option<Auth>,
}

impl RpcProviderConnectionInfo {
    /// Creates a new connection info from a URL and optional authentication credentials.
    ///
    /// When `auth_token` is provided without an `auth_header`, a `Bearer` token is used.
    /// When both are provided, a custom header is used instead.
    pub fn new(url: Url, auth_token: Option<String>, auth_header: Option<String>) -> Self {
        let auth = auth_token.map(|token| Auth::new(token, auth_header));
        Self {
            url: Redacted::from(url),
            auth,
        }
    }
}

/// A Solana JSON-RPC client.
///
/// If provided with a [meter](monitoring::telemetry::metrics::Meter), the client will record
/// JSON-RPC related metrics.
pub struct SolanaRpcClient {
    inner: solana_client::nonblocking::rpc_client::RpcClient,
    rate_limiter: Option<governor::DefaultDirectRateLimiter>,
    provider: String,
    network: NetworkId,
}

impl SolanaRpcClient {
    pub fn new(
        rpc_connection_info: RpcProviderConnectionInfo,
        max_calls_per_second: Option<NonZeroU32>,
        provider: String,
        network: NetworkId,
    ) -> Self {
        let RpcProviderConnectionInfo { url, auth } = rpc_connection_info;
        let inner = if let Some(auth) = auth {
            // Build a reqwest 0.12 client (the version used by solana-rpc-client) with
            // auth in default_headers so credentials are never embedded in URLs.
            let mut headers = HeaderMap::new();
            match auth {
                Auth::Bearer(token) => {
                    headers.typed_insert(
                        Authorization::bearer(&token).expect("validated at config parse time"),
                    );
                }
                Auth::CustomHeader { name, value: token } => {
                    let name = client_error::reqwest::header::HeaderName::try_from(name)
                        .expect("validated at config parse time");
                    let mut value =
                        HeaderValue::try_from(token).expect("validated at config parse time");
                    value.set_sensitive(true);
                    headers.insert(name, value);
                }
            }

            let http_client = client_error::reqwest::Client::builder()
                .default_headers(headers)
                .build()
                .expect("http client with auth header");
            let sender = solana_rpc_client::http_sender::HttpSender::new_with_client(
                url.to_string(),
                http_client,
            );
            RpcClient::new_sender(sender, Default::default())
        } else {
            RpcClient::new(url.to_string())
        };
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
    pub async fn get_slot(
        &self,
        metrics: Option<Arc<metrics::MetricsRegistry>>,
    ) -> client_error::Result<Slot> {
        let slot = self
            .rpc_call("getSlot", metrics, self.inner.get_slot())
            .await?;

        Ok(slot)
    }

    /// Get the confirmed block for a given slot.
    ///
    /// ### RPC Reference
    ///
    /// [getBlock](https://solana.com/docs/rpc/http/getblock)
    pub async fn get_block(
        &self,
        slot: Slot,
        config: rpc_config::RpcBlockConfig,
        metrics: Option<Arc<metrics::MetricsRegistry>>,
    ) -> client_error::Result<UiConfirmedBlock> {
        let block = self
            .rpc_call(
                "getBlock",
                metrics,
                self.inner.get_block_with_config(slot, config),
            )
            .await?;

        Ok(block)
    }

    /// Get the confirmed transaction for a given signature.
    ///
    /// ### RPC Reference
    ///
    /// [getTransaction](https://solana.com/docs/rpc/http/gettransaction)
    pub async fn get_transaction(
        &self,
        signature: solana_transaction::Signature,
        config: rpc_config::RpcTransactionConfig,
        metrics: Option<Arc<metrics::MetricsRegistry>>,
    ) -> client_error::Result<EncodedConfirmedTransactionWithStatusMeta> {
        let transaction = self
            .rpc_call(
                "getTransaction",
                metrics,
                self.inner.get_transaction_with_config(&signature, config),
            )
            .await?;

        Ok(transaction)
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

/// Authentication configuration for Solana RPC requests.
pub enum Auth {
    /// Standard `Authorization: Bearer <token>` header.
    Bearer(String),
    /// Custom header: `<name>: <value>` (e.g. `X-Api-Key: <token>`).
    CustomHeader {
        /// Header name (e.g. `X-Api-Key`).
        name: String,
        /// Header value (raw token, no `Bearer` prefix).
        value: String,
    },
}

impl Auth {
    /// Creates a new `Auth` value.
    ///
    /// When `header` is `Some`, a [`CustomHeader`](Auth::CustomHeader) is used.
    /// When `header` is `None`, a standard [`Bearer`](Auth::Bearer) token is used.
    pub fn new(token: String, header: Option<String>) -> Self {
        match header {
            Some(name) => Auth::CustomHeader { name, value: token },
            None => Auth::Bearer(token),
        }
    }
}

/// Returns `true` if the given error indicates that the block is missing for the requested slot.
///
/// ### Reference
///
/// <https://www.quicknode.com/docs/solana/error-references>
pub fn is_block_missing_err(err: &client_error::Error) -> bool {
    matches!(
        err.kind(),
        client_error::ErrorKind::RpcError(
            solana_rpc_client_api::request::RpcError::RpcResponseError {
                code: -32007 | -32009,
                ..
            },
        )
    )
}

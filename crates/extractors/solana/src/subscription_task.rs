//! Solana RPC PubSub subscription task that listens for new block notifications and
//! pushes them into a ring buffer. These notifications can then be consumed by other
//! components to process new blocks as soon as they arrive.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use futures::StreamExt;
use solana_client::{
    nonblocking::pubsub_client::PubsubClient as SolanaPubsubClient,
    rpc_config::{RpcBlockSubscribeConfig, RpcBlockSubscribeFilter},
};
use solana_transaction_status_client_types::{TransactionDetails, UiTransactionEncoding};
use url::Url;

use crate::ring_buffer::SolanaSlotRingBuffer;

const RETRY_DELAY: Duration = Duration::from_secs(5);
const NOTIFICATION_TIMEOUT: Duration = Duration::from_secs(10);

/// Spawns a task that subscribes to Solana block notifications via RPC PubSub
/// and pushes received slot and block data into the provided ring buffer.
///
/// The task will attempt to reconnect and resubscribe in the case of errors.
pub fn spawn(
    url: Url,
    subscription_ring_buffer: Arc<Mutex<SolanaSlotRingBuffer>>,
) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn(async move {
        'retry: loop {
            tracing::debug!("Running Solana RPC block subscription");

            let pubsub = match SolanaPubsubClient::new(url.as_str()).await {
                Ok(pubsub) => pubsub,
                Err(e) => {
                    tracing::error!(
                        "Failed to create Solana RPC PubSub client: {}. Retrying in 5 seconds...",
                        e
                    );
                    tokio::time::sleep(RETRY_DELAY).await;
                    continue 'retry;
                }
            };

            let mut sub_stream = match pubsub
                .block_subscribe(
                    RpcBlockSubscribeFilter::All,
                    Some(RpcBlockSubscribeConfig {
                        encoding: Some(UiTransactionEncoding::Json),
                        transaction_details: Some(TransactionDetails::Full),
                        max_supported_transaction_version: Some(0),
                        ..Default::default()
                    }),
                )
                .await
            {
                Ok((stream, _)) => stream,
                Err(e) => {
                    tracing::error!(
                        "Failed to subscribe to Solana RPC block notifications: {}. Retrying in 5 seconds...",
                        e
                    );
                    tokio::time::sleep(RETRY_DELAY).await;
                    continue 'retry;
                }
            };

            loop {
                let next_fut = tokio::time::timeout(NOTIFICATION_TIMEOUT, sub_stream.next());
                let Ok(msg_opt) = next_fut.await else {
                    tracing::warn!(
                        "Solana RPC block notification timeout. Reconnecting in 5 seconds..."
                    );
                    tokio::time::sleep(RETRY_DELAY).await;
                    continue 'retry;
                };
                let Some(msg) = msg_opt else {
                    tracing::warn!(
                        "Solana RPC block notification stream closed. Reconnecting in 5 seconds..."
                    );
                    tokio::time::sleep(RETRY_DELAY).await;
                    continue 'retry;
                };

                tracing::debug!(
                    slot = msg.value.slot,
                    has_block = msg.value.block.is_some(),
                    "Received new block notification via Solana RPC subscription"
                );

                let mut ringbuf = subscription_ring_buffer.lock().unwrap();
                ringbuf.push((msg.value.slot, msg.value.block));
            }
        }
    })
}

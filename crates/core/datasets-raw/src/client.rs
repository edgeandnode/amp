//! BlockStreamer trait and retry wrapper for extracting raw blockchain data.

use std::{future::Future, time::Duration};

use datasets_common::block_num::BlockNum;
use futures::{Stream, StreamExt as _};

use crate::rows::Rows;

/// Error type for [`BlockStreamer::block_stream`].
pub type BlockStreamError = Box<dyn std::error::Error + Sync + Send + 'static>;

/// Error type for [`BlockStreamer::latest_block`].
pub type LatestBlockError = Box<dyn std::error::Error + Sync + Send + 'static>;

/// Error type for [`BlockStreamer::wait_for_cleanup`].
pub type CleanupError = Box<dyn std::error::Error + Sync + Send + 'static>;

pub trait BlockStreamer: Clone + 'static {
    fn block_stream(
        self,
        start: BlockNum,
        end: BlockNum,
    ) -> impl Future<Output = impl Stream<Item = Result<Rows, BlockStreamError>> + Send> + Send;

    fn latest_block(
        &mut self,
        finalized: bool,
    ) -> impl Future<Output = Result<Option<BlockNum>, LatestBlockError>> + Send;

    /// Waits for any background work and resources associated with this [`BlockStreamer`]
    /// to be cleaned up.
    ///
    /// This should be called once the user no longer needs to create new block streams
    /// to allow implementations to terminate internal tasks, flush or release network
    /// connections, and free any other resources.
    ///
    /// After requesting cleanup, callers should not call [BlockStreamer::block_stream]
    /// again on the same instance. Behavior when creating new streams after cleanup is
    /// implementation-defined and must not be relied on.
    fn wait_for_cleanup(self) -> impl Future<Output = Result<(), CleanupError>> + Send;

    fn provider_name(&self) -> &str;
}

pub trait BlockStreamerExt: BlockStreamer {
    fn with_retry(self) -> BlockStreamerWithRetry<Self> {
        BlockStreamerWithRetry(self)
    }
}

impl<T> BlockStreamerExt for T where T: BlockStreamer {}

#[derive(Clone)]
pub struct BlockStreamerWithRetry<T: BlockStreamer>(T);

impl<T> BlockStreamer for BlockStreamerWithRetry<T>
where
    T: BlockStreamer + Send + Sync,
{
    async fn block_stream(
        self,
        start: BlockNum,
        end: BlockNum,
    ) -> impl Stream<Item = Result<Rows, BlockStreamError>> + Send {
        const DEBUG_RETRY_LIMIT: u16 = 8;
        const DEBUG_RETRY_DELAY: Duration = Duration::from_millis(50);
        const WARN_RETRY_LIMIT: u16 = 16;
        const WARN_RETRY_DELAY: Duration = Duration::from_millis(100);
        const ERROR_RETRY_DELAY: Duration = Duration::from_millis(300);

        let mut current_block = start;
        let mut num_retries = 0;

        async_stream::stream! {
            'retry: loop {
                let inner_stream = self.0.clone().block_stream(current_block, end).await;
                futures::pin_mut!(inner_stream);
                while let Some(block) = inner_stream.next().await {
                    match &block {
                        Ok(_) => {
                            num_retries = 0;
                            current_block += 1;
                            yield block;
                        }
                        Err(e) => {
                            let error_source = monitoring::logging::error_source(e.as_ref());
                            // Progressively more severe logging and longer retry interval.
                            match num_retries {
                                0 => {
                                    // First error, make sure it is visible in info (default) logs.
                                    num_retries += 1;
                                    tracing::info!(
                                        block = %current_block,
                                        error = %e,
                                        error_source,
                                        "Block streaming failed, retrying"
                                    );
                                    tokio::time::sleep(DEBUG_RETRY_DELAY).await;
                                }
                                1..DEBUG_RETRY_LIMIT => {
                                    num_retries += 1;
                                    tracing::debug!(
                                        block = %current_block,
                                        error = %e,
                                        error_source,
                                        "Block streaming failed, retrying");
                                    tokio::time::sleep(DEBUG_RETRY_DELAY).await;
                                }
                                DEBUG_RETRY_LIMIT..WARN_RETRY_LIMIT => {
                                    num_retries += 1;
                                    tracing::warn!(
                                        block = %current_block,
                                        error = %e,
                                        error_source,
                                        "Block streaming failed, retrying"
                                    );
                                    tokio::time::sleep(WARN_RETRY_DELAY).await;
                                }
                                _ => {
                                    tracing::error!(
                                        block = %current_block,
                                        error = %e,
                                        error_source,
                                        "Block streaming failed, retrying"
                                    );
                                    tokio::time::sleep(ERROR_RETRY_DELAY).await;
                                }
                            }
                            continue 'retry;
                        }
                    }
                }
                break 'retry;
            }
        }
    }

    async fn latest_block(
        &mut self,
        finalized: bool,
    ) -> Result<Option<BlockNum>, LatestBlockError> {
        use backon::{ExponentialBuilder, Retryable};

        (|| async {
            let mut inner = self.0.clone();
            inner.latest_block(finalized).await
        })
        .retry(
            ExponentialBuilder::default()
                .with_min_delay(Duration::from_secs(2))
                .with_max_delay(Duration::from_secs(20))
                .with_max_times(10),
        )
        .notify(|err, dur| {
            tracing::warn!(
                error = %err,
                "Failed to get latest block. Retrying in {:.1}s",
                dur.as_secs_f32()
            );
        })
        .await
    }

    fn wait_for_cleanup(self) -> impl Future<Output = Result<(), CleanupError>> + Send {
        self.0.wait_for_cleanup()
    }

    fn provider_name(&self) -> &str {
        self.0.provider_name()
    }
}

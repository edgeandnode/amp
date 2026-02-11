//! BlockStreamer trait and retry wrapper for extracting raw blockchain data.

use std::{future::Future, time::Duration};

use datasets_common::block_num::BlockNum;
use futures::{Stream, StreamExt as _};

use crate::rows::Rows;

/// Error type for [`BlockStreamer::block_stream`].
///
/// Block streams distinguish between recoverable and fatal errors to allow [`BlockStreamerWithRetry`]
/// to automatically retry transient failures while immediately aborting on unrecoverable ones.
#[derive(Debug, thiserror::Error)]
pub enum BlockStreamError {
    /// Transient error that may succeed on retry (e.g., network timeouts, rate limits).
    #[error("Recoverable error: {0}")]
    Recoverable(#[source] RecoverableError),
    /// Permanent error that should abort the stream (e.g., invalid data, out-of-range blocks).
    #[error("Fatal error: {0}")]
    Fatal(#[source] FatalError),
}

/// Extension trait for converting `Result<T, E>` into `Result<T, BlockStreamError>`.
///
/// Provides ergonomic methods to classify errors as recoverable or fatal when yielding
/// from a block stream.
///
/// # Example
///
/// ```ignore
/// // Mark RPC errors as recoverable (will be retried)
/// let block = rpc_client.get_block(slot).await.recoverable()?;
///
/// // Mark data conversion errors as fatal (no retry)
/// let rows = block.into_db_rows().fatal()?;
/// ```
pub trait BlockStreamResultExt<T> {
    /// Converts the error into [`BlockStreamError::Recoverable`].
    fn recoverable(self) -> Result<T, BlockStreamError>;

    /// Converts the error into [`BlockStreamError::Fatal`].
    fn fatal(self) -> Result<T, BlockStreamError>;
}

impl<T, E> BlockStreamResultExt<T> for Result<T, E>
where
    E: Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
{
    fn recoverable(self) -> Result<T, BlockStreamError> {
        self.map_err(|e| BlockStreamError::Recoverable(e.into()))
    }

    fn fatal(self) -> Result<T, BlockStreamError> {
        self.map_err(|e| BlockStreamError::Fatal(e.into()))
    }
}

/// Errors that are expected to be transient and may succeed if retried
/// (e.g., network timeouts, rate limiting, temporary service unavailability).
pub type RecoverableError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Errors that indicate a permanent failure and should not be retried
/// (e.g., malformed data, unsupported versions, blocks outside requested range).
pub type FatalError = Box<dyn std::error::Error + Send + Sync + 'static>;

impl std::convert::AsRef<dyn std::error::Error + 'static> for BlockStreamError {
    fn as_ref(&self) -> &(dyn std::error::Error + 'static) {
        match self {
            BlockStreamError::Recoverable(e) => e.as_ref(),
            BlockStreamError::Fatal(e) => e.as_ref(),
        }
    }
}

/// Error type for [`BlockStreamer::latest_block`].
pub type LatestBlockError = Box<dyn std::error::Error + Sync + Send + 'static>;

/// Error type for [`BlockStreamer::wait_for_cleanup`].
pub type CleanupError = Box<dyn std::error::Error + Sync + Send + 'static>;

/// Trait for extracting raw blockchain data as a stream of rows.
///
/// Implementations fetch blocks from a data source (RPC, archive, etc.) and convert them
/// into database rows. Use [`BlockStreamerExt::with_retry`] to wrap with automatic retry
/// handling for recoverable errors.
pub trait BlockStreamer: Clone + 'static {
    /// Streams blocks in the inclusive range `[start, end]`.
    ///
    /// Yields [`Rows`] for each successfully processed block. Skipped blocks (e.g., empty
    /// slots in Solana) should not yield any rows for that block.
    fn block_stream(
        self,
        start: BlockNum,
        end: BlockNum,
    ) -> impl Future<Output = impl Stream<Item = Result<Rows, BlockStreamError>> + Send> + Send;

    /// Returns the latest available block number, or `None` if no blocks exist.
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

/// Extension trait providing retry functionality for [`BlockStreamer`].
pub trait BlockStreamerExt: BlockStreamer {
    /// Wraps this streamer with automatic retry handling.
    fn with_retry(self) -> BlockStreamerWithRetry<Self> {
        BlockStreamerWithRetry(self)
    }
}

impl<T> BlockStreamerExt for T where T: BlockStreamer {}

/// A [`BlockStreamer`] wrapper that automatically retries on [`BlockStreamError::Recoverable`].
///
/// When a recoverable error occurs, the wrapper restarts the stream from the last successful
/// block with progressive backoff. Fatal errors immediately terminate the stream.
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

        let mut next_block = start;
        let mut num_retries = 0;

        async_stream::stream! {
            'retry: loop {
                let inner_stream = self.0.clone().block_stream(next_block, end).await;
                futures::pin_mut!(inner_stream);
                while let Some(row_result) = inner_stream.next().await {
                    match row_result.as_ref() {
                        Ok(rows) => {
                            num_retries = 0;
                            next_block = rows.block_num() + 1;
                            yield row_result;
                        }
                        Err(BlockStreamError::Fatal(e)) => {
                            let error_source = monitoring::logging::error_source(e.as_ref());
                            tracing::error!(
                                block = %next_block,
                                error = %e,
                                error_source,
                                "Fatal error in block streamer, aborting"
                            );
                            yield row_result;
                            return;
                        }
                        Err(BlockStreamError::Recoverable(e)) => {
                            let error_source = monitoring::logging::error_source(e.as_ref());
                            // Progressively more severe logging and longer retry interval.
                            match num_retries {
                                0 => {
                                    // First error, make sure it is visible in info (default) logs.
                                    num_retries += 1;
                                    tracing::info!(
                                        block = %next_block,
                                        error = %e,
                                        error_source,
                                        "Block streaming failed, retrying"
                                    );
                                    tokio::time::sleep(DEBUG_RETRY_DELAY).await;
                                }
                                1..DEBUG_RETRY_LIMIT => {
                                    num_retries += 1;
                                    tracing::debug!(
                                        block = %next_block,
                                        error = %e,
                                        error_source,
                                        "Block streaming failed, retrying");
                                    tokio::time::sleep(DEBUG_RETRY_DELAY).await;
                                }
                                DEBUG_RETRY_LIMIT..WARN_RETRY_LIMIT => {
                                    num_retries += 1;
                                    tracing::warn!(
                                        block = %next_block,
                                        error = %e,
                                        error_source,
                                        "Block streaming failed, retrying"
                                    );
                                    tokio::time::sleep(WARN_RETRY_DELAY).await;
                                }
                                _ => {
                                    tracing::error!(
                                        block = %next_block,
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

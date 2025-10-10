//! Stream task abstraction for per-table data streaming.
//!
//! This module encapsulates the logic for streaming data from a Nozzle table
//! to PostgreSQL, including batch processing, checkpoint management, and reorg handling.

use std::{sync::Arc, time::Duration};

use common::metadata::segments::ResumeWatermark;
use futures::StreamExt;
use nozzle_client::{ResponseBatchWithReorg, SqlClient};
use tracing::{debug, error, info, warn};

use crate::{
    batch_utils::{convert_nanosecond_timestamps, inject_system_metadata},
    sync_engine::AmpsyncDbEngine,
};

/// Maximum number of stream reconnection attempts before giving up
const MAX_STREAM_RETRIES: u32 = 5;

/// Maximum delay between reconnection attempts (in seconds)
const MAX_RETRY_DELAY_SECS: u64 = 60;

/// Represents a single table's streaming task configuration and state.
pub struct StreamTask {
    /// Name of the table being streamed
    pub table_name: String,
    /// SQL query for streaming (includes SETTINGS stream = true)
    pub streaming_query: String,
    /// Optional watermark for resumption
    pub resume_watermark: Option<ResumeWatermark>,
}

impl StreamTask {
    /// Creates a new StreamTask.
    pub fn new(
        table_name: String,
        streaming_query: String,
        resume_watermark: Option<ResumeWatermark>,
    ) -> Self {
        Self {
            table_name,
            streaming_query,
            resume_watermark,
        }
    }

    /// Runs the streaming task until shutdown is requested.
    ///
    /// This is the main streaming loop that:
    /// 1. Connects to Nozzle and starts streaming
    /// 2. Processes batches, reorgs, and watermarks
    /// 3. Reconnects automatically on errors with exponential backoff
    /// 4. Respects shutdown signals
    ///
    /// # Arguments
    /// * `sql_client` - Nozzle client for querying
    /// * `db_engine` - Database engine for inserts and checkpoints
    /// * `batch_semaphore` - Semaphore for backpressure control
    /// * `shutdown_token` - Token for graceful shutdown signaling
    pub async fn run(
        self,
        sql_client: &mut SqlClient,
        db_engine: &AmpsyncDbEngine,
        batch_semaphore: Arc<tokio::sync::Semaphore>,
        shutdown_token: tokio_util::sync::CancellationToken,
    ) {
        let mut retry_count = 0u32;

        loop {
            // Check if shutdown has been requested
            if shutdown_token.is_cancelled() {
                info!(
                    table = %self.table_name,
                    "shutdown_requested"
                );
                return;
            }

            // Query with watermark for hash-verified resumption
            let result_stream = match sql_client
                .query(&self.streaming_query, None, self.resume_watermark.as_ref())
                .await
            {
                Ok(stream) => stream,
                Err(e) => {
                    error!(
                        table = %self.table_name,
                        attempt = retry_count + 1,
                        max_retries = MAX_STREAM_RETRIES,
                        error = %e,
                        "stream_creation_failed"
                    );

                    if retry_count >= MAX_STREAM_RETRIES {
                        error!(
                            table = %self.table_name,
                            max_retries = MAX_STREAM_RETRIES,
                            "max_retries_reached"
                        );
                        return;
                    }

                    // Exponential backoff: 2^retry_count seconds, capped at MAX_RETRY_DELAY_SECS
                    let delay_secs = std::cmp::min(2u64.pow(retry_count), MAX_RETRY_DELAY_SECS);
                    warn!(
                        table = %self.table_name,
                        retry_delay_secs = delay_secs,
                        attempt = retry_count + 1,
                        "retrying_stream_creation"
                    );
                    tokio::time::sleep(Duration::from_secs(delay_secs)).await;
                    retry_count += 1;
                    continue;
                }
            };

            let mut reorg_stream = nozzle_client::with_reorg(result_stream);
            info!(
                table = %self.table_name,
                "stream_started"
            );

            // Reset retry count on successful connection
            retry_count = 0;

            // Process stream events
            if !self
                .process_stream(
                    &mut reorg_stream,
                    db_engine,
                    &batch_semaphore,
                    &shutdown_token,
                )
                .await
            {
                // Shutdown requested during stream processing
                return;
            }

            // Stream ended - will retry with exponential backoff in outer loop
            warn!(
                table = %self.table_name,
                "stream_ended"
            );
        }
    }

    /// Processes events from the stream until an error occurs or shutdown is requested.
    ///
    /// Returns true if the stream should reconnect, false if shutdown was requested.
    async fn process_stream<S>(
        &self,
        reorg_stream: &mut S,
        db_engine: &AmpsyncDbEngine,
        batch_semaphore: &Arc<tokio::sync::Semaphore>,
        shutdown_token: &tokio_util::sync::CancellationToken,
    ) -> bool
    where
        S: futures::Stream<Item = Result<ResponseBatchWithReorg, nozzle_client::Error>> + Unpin,
    {
        while let Some(result) = reorg_stream.next().await {
            match result {
                Ok(ResponseBatchWithReorg::Batch { data, metadata }) => {
                    if !self
                        .handle_batch(data, metadata, db_engine, batch_semaphore)
                        .await
                    {
                        return true; // Error - reconnect
                    }
                }
                Ok(ResponseBatchWithReorg::Reorg { invalidation }) => {
                    if !self
                        .handle_reorg(invalidation, db_engine, batch_semaphore)
                        .await
                    {
                        return true; // Error - reconnect
                    }
                }
                Ok(ResponseBatchWithReorg::Watermark(watermark)) => {
                    if !self
                        .handle_watermark(watermark, db_engine, batch_semaphore)
                        .await
                    {
                        return true; // Error - reconnect
                    }
                }
                Err(e) => {
                    error!(
                        table = %self.table_name,
                        error = %e,
                        "stream_error"
                    );
                    return true; // Error - reconnect
                }
            }

            // Check for shutdown after each event
            if shutdown_token.is_cancelled() {
                info!(
                    table = %self.table_name,
                    "shutdown_requested_during_stream"
                );
                return false; // Shutdown - don't reconnect
            }
        }

        true // Stream ended normally - reconnect
    }

    /// Handles a data batch from the stream.
    ///
    /// Returns true on success, false if an error occurred (triggers reconnect).
    async fn handle_batch(
        &self,
        data: common::arrow::array::RecordBatch,
        metadata: nozzle_client::Metadata,
        db_engine: &AmpsyncDbEngine,
        batch_semaphore: &Arc<tokio::sync::Semaphore>,
    ) -> bool {
        info!(
            table = %self.table_name,
            rows = data.num_rows(),
            block_ranges = ?metadata.ranges,
            "batch_received"
        );

        // Acquire semaphore permit before processing batch
        let _permit = match batch_semaphore.acquire().await {
            Ok(permit) => permit,
            Err(_) => {
                warn!(
                    table = %self.table_name,
                    "semaphore_closed"
                );
                return false; // Shutdown in progress
            }
        };

        // Convert nanosecond timestamps to microseconds for PostgreSQL compatibility
        let converted_batch = match convert_nanosecond_timestamps(data) {
            Ok(batch) => batch,
            Err(e) => {
                error!(
                    table = %self.table_name,
                    error = %e,
                    "timestamp_conversion_failed"
                );
                return false; // Reconnect
            }
        };

        // Inject system metadata columns (_id, _block_num_start, _block_num_end)
        let batch_with_metadata = match inject_system_metadata(converted_batch, &metadata.ranges) {
            Ok(batch) => batch,
            Err(e) => {
                error!(
                    table = %self.table_name,
                    error = %e,
                    "system_metadata_injection_failed"
                );
                return false; // Reconnect
            }
        };

        // High-performance bulk insert using arrow_to_pg
        if let Err(e) = db_engine
            .insert_record_batch(&self.table_name, &batch_with_metadata)
            .await
        {
            error!(
                table = %self.table_name,
                rows = batch_with_metadata.num_rows(),
                error = %e,
                "batch_insert_failed"
            );
            return false; // Reconnect
        }

        info!(
            table = %self.table_name,
            rows = batch_with_metadata.num_rows(),
            "batch_inserted"
        );

        // Update incremental checkpoint for progress tracking between watermarks
        if let Some((network, max_block)) =
            AmpsyncDbEngine::extract_max_block_from_ranges(&metadata.ranges)
        {
            if let Err(e) = db_engine
                .update_incremental_checkpoint(&self.table_name, &network, max_block)
                .await
            {
                warn!(
                    table = %self.table_name,
                    network = %network,
                    block_num = max_block,
                    error = %e,
                    "incremental_checkpoint_update_failed"
                );
                // Don't break - incremental checkpoint failure is not critical
            } else {
                debug!(
                    table = %self.table_name,
                    network = %network,
                    block_num = max_block,
                    "incremental_checkpoint_updated"
                );
            }
        }

        true // Success
    }

    /// Handles a blockchain reorganization event.
    ///
    /// Returns true on success, false if an error occurred (triggers reconnect).
    async fn handle_reorg(
        &self,
        invalidation: Vec<nozzle_client::InvalidationRange>,
        db_engine: &AmpsyncDbEngine,
        batch_semaphore: &Arc<tokio::sync::Semaphore>,
    ) -> bool {
        warn!(
            table = %self.table_name,
            invalidation_ranges = ?invalidation,
            "reorg_detected"
        );

        // Acquire semaphore permit for reorg handling
        let _permit = match batch_semaphore.acquire().await {
            Ok(permit) => permit,
            Err(_) => {
                warn!(
                    table = %self.table_name,
                    context = "reorg",
                    "semaphore_closed"
                );
                return false; // Shutdown in progress
            }
        };

        // Handle reorg by deleting affected rows
        if let Err(e) = db_engine
            .handle_reorg(&self.table_name, &invalidation)
            .await
        {
            error!(
                table = %self.table_name,
                error = %e,
                "reorg_handling_failed"
            );
            return false; // Reconnect
        }

        info!(
            table = %self.table_name,
            "reorg_handled"
        );

        true // Success
    }

    /// Handles a watermark checkpoint event.
    ///
    /// Returns true on success, false if an error occurred (triggers reconnect).
    async fn handle_watermark(
        &self,
        watermark: ResumeWatermark,
        db_engine: &AmpsyncDbEngine,
        batch_semaphore: &Arc<tokio::sync::Semaphore>,
    ) -> bool {
        info!(
            table = %self.table_name,
            watermark = ?watermark,
            "watermark_received"
        );

        // Acquire semaphore permit for watermark save
        let _permit = match batch_semaphore.acquire().await {
            Ok(permit) => permit,
            Err(_) => {
                warn!(
                    table = %self.table_name,
                    context = "watermark",
                    "semaphore_closed"
                );
                return false; // Shutdown in progress
            }
        };

        // Save watermark for hash-verified stream resumption
        if let Err(e) = db_engine.save_watermark(&self.table_name, &watermark).await {
            error!(
                table = %self.table_name,
                error = %e,
                "watermark_save_failed"
            );
            return false; // Reconnect
        }

        info!(
            table = %self.table_name,
            "watermark_saved"
        );

        true // Success
    }
}

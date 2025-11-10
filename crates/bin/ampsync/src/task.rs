//! Per-table streaming task with crash-safe event processing.

use amp_client::{AmpClient, PostgresStateStore, TransactionEvent};
use common::BlockNum;
use datasets_common::reference::Reference;
use futures::StreamExt;
use monitoring::logging;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};

use crate::engine::Engine;

/// Errors that occur during stream task execution
#[derive(Debug, thiserror::Error)]
pub enum StreamTaskError {
    /// Failed to create PostgresStateStore
    #[error("Failed to create PostgresStateStore for stream '{stream_id}'")]
    CreateStateStore {
        stream_id: String,
        #[source]
        source: amp_client::Error,
    },

    /// Failed to create transactional stream
    #[error("Failed to create transactional stream for query '{query}'")]
    CreateStream {
        query: String,
        #[source]
        source: amp_client::Error,
    },

    /// Stream returned an error
    #[error("Stream error occurred")]
    StreamError(#[source] amp_client::Error),

    /// Failed to add transaction metadata to batch
    #[error("Failed to add transaction metadata to batch for table '{table_name}'")]
    AddMetadata {
        table_name: String,
        #[source]
        source: crate::arrow::AddTransactionMetadataError,
    },

    /// Failed to insert batch
    #[error("Failed to insert batch into table '{table_name}'")]
    InsertBatch {
        table_name: String,
        #[source]
        source: crate::engine::InsertBatchError,
    },

    /// Failed to delete by transaction range
    #[error("Failed to delete by transaction range in table '{table_name}'")]
    DeleteByTxRange {
        table_name: String,
        #[source]
        source: crate::engine::DeleteByTxRangeError,
    },

    /// Failed to commit state
    #[error("Failed to commit state for table '{table_name}'")]
    CommitState {
        table_name: String,
        #[source]
        source: amp_client::Error,
    },

    /// Stream ended unexpectedly (streams should be continuous)
    #[error(
        "Stream ended unexpectedly for table '{table_name}' - continuous streams should never terminate naturally"
    )]
    UnexpectedStreamEnd { table_name: String },
}

/// Per-table streaming task.
///
/// Processes TransactionalStream events and applies them to PostgreSQL:
/// - Data events: Insert batches with transaction metadata
/// - Undo events: Delete uncommitted data by transaction ID range
/// - Watermark events: Commit state only (no database operations)
///
/// # State Management
///
/// Each StreamTask maintains its own state in PostgreSQL via `PostgresStateStore`,
/// which tracks transaction IDs and watermarks. The state store ensures:
/// - Resume from last committed position after restart
/// - Detection of gaps (uncommitted transactions) via Undo events
/// - Proper ordering of transactions
///
/// # Graceful Shutdown
///
/// The task respects the `CancellationToken` for clean shutdown. When cancelled,
/// it stops processing new events and allows the current event to complete.
pub struct StreamTask {
    table_name: String,
    dataset: Reference,
    query: String,
    engine: Engine,
    client: AmpClient,
    pool: PgPool,
    retention: BlockNum,
    shutdown: CancellationToken,
}

impl StreamTask {
    /// Create a new StreamTask.
    ///
    /// # Arguments
    /// - `table_name`: Target table name
    /// - `dataset`: Fully resolved dataset reference (for stream ID)
    /// - `query`: SQL query to stream
    /// - `engine`: Database engine for insert/delete operations
    /// - `client`: AmpClient instance
    /// - `pool`: Database connection pool (for creating state store)
    /// - `retention`: Retention window in blocks
    /// - `shutdown`: Cancellation token for graceful shutdown
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        table_name: String,
        dataset: Reference,
        query: String,
        engine: Engine,
        client: AmpClient,
        pool: PgPool,
        retention: BlockNum,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            table_name,
            dataset,
            query,
            engine,
            client,
            pool,
            retention,
            shutdown,
        }
    }

    /// Run the streaming task.
    ///
    /// This method implements the main event loop for processing stream events.
    /// It runs until either:
    /// - A shutdown signal is received via `CancellationToken`
    /// - An unrecoverable error occurs (including unexpected stream termination)
    ///
    /// # Event Loop Pattern
    ///
    /// 1. Creates a TransactionalStream with PostgresStateStore for this table
    /// 2. Enters a `tokio::select!` loop that waits for:
    ///    - Shutdown signal (breaks immediately)
    ///    - Stream events (processes and commits)
    /// 3. For each event:
    ///    - Applies database changes (insert/delete)
    ///    - Calls `commit.await` to persist state atomically
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Stream creation fails (invalid query, connection error)
    /// - Event processing fails (database operation error)
    /// - State commit fails (PostgreSQL connection error)
    #[instrument(skip(self), fields(table = %self.table_name, dataset = %self.dataset))]
    pub async fn run(self) -> Result<(), StreamTaskError> {
        info!("starting_task");

        // Create state store for this table using fully qualified dataset reference
        let stream_id = format!("{}:{}", self.dataset, self.table_name);
        info!(stream_id = %stream_id, "creating_state_store");
        let store = PostgresStateStore::new(self.pool.clone(), &stream_id)
            .await
            .map_err(|err| StreamTaskError::CreateStateStore {
                stream_id: stream_id.clone(),
                source: err,
            })?;

        // Create transactional stream
        info!(
            query = %self.query,
            retention_blocks = self.retention,
            "creating_transactional_stream"
        );
        let mut stream = self
            .client
            .stream(&self.query)
            .transactional(store, self.retention)
            .await
            .map_err(|err| StreamTaskError::CreateStream {
                query: self.query.clone(),
                source: err,
            })?;

        info!("task_ready");

        loop {
            tokio::select! {
                // Shutdown signal
                _ = self.shutdown.cancelled() => {
                    info!("shutdown_requested");
                    break;
                }

                // Process events
                event_result = stream.next() => {
                    match event_result {
                        Some(Ok((event, commit))) => {
                            if let Err(e) = self.handle_event(event, commit).await {
                                error!(error = %e, error_source = logging::error_source(&e), "event_handling_failed");
                                return Err(e);
                            }
                        }
                        Some(Err(e)) => {
                            error!(error = %e, error_source = logging::error_source(&e), "stream_error");
                            return Err(StreamTaskError::StreamError(e));
                        }
                        None => {
                            error!("stream_ended_unexpectedly");
                            return Err(StreamTaskError::UnexpectedStreamEnd {
                                table_name: self.table_name.clone(),
                            });
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Handle a single TransactionEvent.
    ///
    /// Processes the event and commits state changes atomically. Each event type
    /// requires different database operations:
    ///
    /// - **Data**: Insert batch with metadata, then commit state
    /// - **Undo**: Delete uncommitted transactions, then commit state
    /// - **Watermark**: Only commit state (no database changes)
    ///
    /// The `commit.await` call is critical for crash safety - it atomically updates
    /// the state store to mark this transaction as durable.
    ///
    /// # Errors
    ///
    /// Returns an error if database operations or state commits fail. Errors
    /// cause the entire task to fail, requiring a restart (with automatic Undo
    /// cleanup via gap detection).
    #[instrument(skip(self, event, commit), fields(table = %self.table_name))]
    async fn handle_event(
        &self,
        event: TransactionEvent,
        commit: amp_client::CommitHandle,
    ) -> Result<(), StreamTaskError> {
        match event {
            TransactionEvent::Data { id, batch, ranges } => {
                let num_rows = batch.num_rows();
                let blocks_start = ranges.first().map(|r| *r.numbers.start());
                let blocks_end = ranges.last().map(|r| *r.numbers.end());

                info!(
                    tx_id = id,
                    rows = num_rows,
                    blocks_start = ?blocks_start,
                    blocks_end = ?blocks_end,
                    num_ranges = ranges.len(),
                    "data_event_processing"
                );

                // Add _tx_id and _row_index columns
                let batch_with_metadata = crate::arrow::add_transaction_metadata(batch, id)
                    .map_err(|err| StreamTaskError::AddMetadata {
                        table_name: self.table_name.clone(),
                        source: err,
                    })?;

                // Insert to database
                self.engine
                    .insert_batch(&self.table_name, batch_with_metadata)
                    .await
                    .map_err(|err| StreamTaskError::InsertBatch {
                        table_name: self.table_name.clone(),
                        source: err,
                    })?;

                // Commit state (marks transaction as durable)
                commit.await.map_err(|err| StreamTaskError::CommitState {
                    table_name: self.table_name.clone(),
                    source: err,
                })?;

                info!(tx_id = id, rows = num_rows, "data_event_processed");
            }

            TransactionEvent::Undo {
                id,
                invalidate,
                cause,
            } => {
                let invalidate_start = *invalidate.start();
                let invalidate_end = *invalidate.end();
                let invalidate_count = invalidate_end.saturating_sub(invalidate_start) + 1;

                warn!(
                    tx_id = id,
                    invalidate_start = invalidate_start,
                    invalidate_end = invalidate_end,
                    invalidate_count = invalidate_count,
                    cause = ?cause,
                    "undo_event_processing"
                );

                // Delete rows by transaction ID range
                self.engine
                    .delete_by_tx_range(&self.table_name, invalidate_start, invalidate_end)
                    .await
                    .map_err(|err| StreamTaskError::DeleteByTxRange {
                        table_name: self.table_name.clone(),
                        source: err,
                    })?;

                // Commit state (acknowledges cleanup)
                commit.await.map_err(|err| StreamTaskError::CommitState {
                    table_name: self.table_name.clone(),
                    source: err,
                })?;

                warn!(
                    tx_id = id,
                    invalidate_start = invalidate_start,
                    invalidate_end = invalidate_end,
                    "undo_event_processed"
                );
            }

            TransactionEvent::Watermark { id, ranges, prune } => {
                let blocks_start = ranges.first().map(|r| *r.numbers.start());
                let blocks_end = ranges.last().map(|r| *r.numbers.end());

                debug!(
                    tx_id = id,
                    blocks_start = ?blocks_start,
                    blocks_end = ?blocks_end,
                    num_ranges = ranges.len(),
                    prune_tx_id = ?prune,
                    "watermark_event_processing"
                );

                // Just commit (state managed by amp-client)
                // No database operations needed for watermarks
                commit.await.map_err(|err| StreamTaskError::CommitState {
                    table_name: self.table_name.clone(),
                    source: err,
                })?;

                debug!(tx_id = id, "watermark_event_processed");
            }
        }

        Ok(())
    }
}

//! Per-table streaming task with crash-safe event processing.

use amp_client::{AmpClient, HasSchema, PostgresStateStore, TransactionEvent, TransactionalStream};
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

    /// Failed to create table
    #[error("Failed to create table '{table_name}'")]
    CreateTable {
        table_name: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
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
    stream: TransactionalStream,
    engine: Engine,
    shutdown: CancellationToken,
}

impl StreamTask {
    /// Create a new StreamTask.
    ///
    /// This performs the following steps:
    /// 1. Builds the streaming query from dataset and table name
    /// 2. Creates a state store for the stream
    /// 3. Creates a transactional stream
    /// 4. Retrieves the schema from the stream
    /// 5. Creates the PostgreSQL table with the schema
    /// 6. Returns a configured StreamTask ready to run
    ///
    /// # Arguments
    /// - `table_name`: Target table name
    /// - `dataset`: Fully resolved dataset reference (for stream ID)
    /// - `engine`: Database engine for table creation
    /// - `client`: AmpClient instance
    /// - `pool`: Database connection pool (for creating state store)
    /// - `retention`: Retention window in blocks
    /// - `shutdown`: Cancellation token for graceful shutdown
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - State store creation fails
    /// - Stream creation fails
    /// - Table creation fails
    pub async fn new(
        table_name: String,
        dataset: Reference,
        engine: Engine,
        client: AmpClient,
        pool: PgPool,
        retention: BlockNum,
        shutdown: CancellationToken,
    ) -> Result<Self, StreamTaskError> {
        // Build streaming query
        let query = crate::sql::streaming_query(&dataset, &table_name);

        // Create state store for this table using fully qualified dataset reference
        let stream_id = format!("{}:{}", dataset, table_name);
        info!(
            table = %table_name,
            stream_id = %stream_id,
            "creating_state_store"
        );
        let store = PostgresStateStore::new(pool.clone(), &stream_id)
            .await
            .map_err(|err| StreamTaskError::CreateStateStore {
                stream_id: stream_id.clone(),
                source: err,
            })?;

        // Create transactional stream
        info!(
            table = %table_name,
            query = %query,
            retention_blocks = retention,
            "creating_transactional_stream"
        );
        let stream = client
            .stream(&query)
            .transactional(store, retention)
            .await
            .map_err(|err| StreamTaskError::CreateStream {
                query: query.clone(),
                source: err,
            })?;

        // Get schema from stream
        let schema = stream.schema();
        info!(
            table = %table_name,
            num_fields = schema.fields().len(),
            "retrieved_schema_from_stream"
        );

        // Create table using stream schema
        info!(table = %table_name, "creating_table");
        engine
            .create_table(&table_name, schema)
            .await
            .map_err(|err| StreamTaskError::CreateTable {
                table_name: table_name.clone(),
                source: Box::new(err),
            })?;

        info!(table = %table_name, "table_created");

        Ok(Self {
            table_name,
            dataset,
            stream,
            engine,
            shutdown,
        })
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
    /// 1. Enters a `tokio::select!` loop that waits for:
    ///    - Shutdown signal (breaks immediately)
    ///    - Stream events (processes and commits)
    /// 2. For each event:
    ///    - Applies database changes (insert/delete)
    ///    - Calls `commit.await` to persist state atomically
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Event processing fails (database operation error)
    /// - State commit fails (PostgreSQL connection error)
    #[instrument(skip(self), fields(table = %self.table_name, dataset = %self.dataset))]
    pub async fn run(self) -> Result<(), StreamTaskError> {
        // Destructure self to avoid borrowing issues with non-Sync types
        let Self {
            table_name,
            dataset: _,
            mut stream,
            engine,
            shutdown,
        } = self;

        info!("starting_task");

        loop {
            tokio::select! {
                // Shutdown signal
                _ = shutdown.cancelled() => {
                    info!("shutdown_requested");
                    break;
                }

                // Process events
                event_result = stream.next() => {
                    match event_result {
                        Some(Ok((event, commit))) => {
                            if let Err(e) = Self::handle_event(&table_name, &engine, event, commit).await {
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
                                table_name: table_name.clone(),
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
    #[instrument(skip(engine, event, commit), fields(table = %table_name))]
    async fn handle_event(
        table_name: &str,
        engine: &Engine,
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
                        table_name: table_name.to_string(),
                        source: err,
                    })?;

                // Insert to database
                engine
                    .insert_batch(table_name, batch_with_metadata)
                    .await
                    .map_err(|err| StreamTaskError::InsertBatch {
                        table_name: table_name.to_string(),
                        source: err,
                    })?;

                // Commit state (marks transaction as durable)
                commit.await.map_err(|err| StreamTaskError::CommitState {
                    table_name: table_name.to_string(),
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
                engine
                    .delete_by_tx_range(table_name, invalidate_start, invalidate_end)
                    .await
                    .map_err(|err| StreamTaskError::DeleteByTxRange {
                        table_name: table_name.to_string(),
                        source: err,
                    })?;

                // Commit state (acknowledges cleanup)
                commit.await.map_err(|err| StreamTaskError::CommitState {
                    table_name: table_name.to_string(),
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
                    table_name: table_name.to_string(),
                    source: err,
                })?;

                debug!(tx_id = id, "watermark_event_processed");
            }
        }

        Ok(())
    }
}

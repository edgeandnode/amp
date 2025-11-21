//! Database operations for ampsync.
//!
//! This module handles all database operations including table creation,
//! batch insertion, and transaction ID-based deletions.

use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use arrow_to_postgres::ArrowToPostgresBinaryEncoder;
use backon::{ExponentialBuilder, Retryable};
use monitoring::logging;
use sqlx::PgPool;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::sql;

/// Optimizes batch sizes based on processing performance.
///
/// Uses atomics for lock-free reads in the hot path. Only locks when updating samples.
#[derive(Debug)]
pub struct AdaptiveBatchManager {
    /// Current target batch size (atomic for lock-free reads)
    current_batch_size: Arc<AtomicUsize>,
    /// Minimum allowed batch size
    min_batch_size: usize,
    /// Maximum allowed batch size
    max_batch_size: usize,
    /// Target processing time per batch (milliseconds)
    target_duration_ms: u64,
    /// Recent performance samples (duration_ms, rows)
    recent_samples: Arc<Mutex<VecDeque<(u64, usize)>>>,
    /// Maximum samples to keep for averaging
    max_samples: usize,
    /// Consecutive error count (atomic)
    error_count: Arc<AtomicUsize>,
    /// Target memory per batch (bytes)
    target_memory_bytes: usize,
}

impl Clone for AdaptiveBatchManager {
    fn clone(&self) -> Self {
        Self {
            current_batch_size: Arc::clone(&self.current_batch_size),
            min_batch_size: self.min_batch_size,
            max_batch_size: self.max_batch_size,
            target_duration_ms: self.target_duration_ms,
            recent_samples: Arc::clone(&self.recent_samples),
            max_samples: self.max_samples,
            error_count: Arc::clone(&self.error_count),
            target_memory_bytes: self.target_memory_bytes,
        }
    }
}

impl Default for AdaptiveBatchManager {
    fn default() -> Self {
        Self {
            current_batch_size: Arc::new(AtomicUsize::new(1000)), // Start with 1K rows
            min_batch_size: 100,                                  // Minimum 100 rows
            max_batch_size: 50_000,                               // Maximum 50K rows
            target_duration_ms: 1000,                             // Target 1 second per batch
            recent_samples: Arc::new(Mutex::new(VecDeque::with_capacity(10))),
            max_samples: 10,
            error_count: Arc::new(AtomicUsize::new(0)),
            target_memory_bytes: 50 * 1024 * 1024, // Target 50MB per batch
        }
    }
}

impl AdaptiveBatchManager {
    /// Records successful batch processing and adjusts batch size.
    pub async fn record_success(&self, duration: Duration, rows_processed: usize) {
        let duration_ms = duration.as_millis() as u64;

        // Reset error count on success (lock-free atomic operation)
        self.error_count.store(0, Ordering::Relaxed);

        // Update recent samples (only lock for the queue)
        let mut samples = self.recent_samples.lock().await;
        samples.push_back((duration_ms, rows_processed));
        if samples.len() > self.max_samples {
            samples.pop_front();
        }

        // Adjust batch size based on performance (still holding the samples lock)
        self.adjust_batch_size_with_samples(&samples);

        // Release lock before logging
        drop(samples);

        let current_size = self.current_batch_size.load(Ordering::Relaxed);
        tracing::debug!(
            duration_ms = duration_ms,
            rows = rows_processed,
            new_batch_size = current_size,
            "batch_performance_recorded"
        );
    }

    /// Records batch processing error and reduces batch size after 3 consecutive errors.
    pub fn record_error(&self) {
        let error_count = self.error_count.fetch_add(1, Ordering::Relaxed) + 1;

        // Reduce batch size on repeated errors
        if error_count >= 3 {
            let current = self.current_batch_size.load(Ordering::Relaxed);
            let new_size = ((current as f64) * 0.7) as usize;
            let new_size = new_size.max(self.min_batch_size);

            self.current_batch_size.store(new_size, Ordering::Relaxed);
            self.error_count.store(0, Ordering::Relaxed); // Reset after adjustment

            tracing::warn!(
                new_batch_size = new_size,
                "batch_size_reduced_due_to_errors"
            );
        }
    }

    /// Returns optimal batch size considering both performance and memory constraints.
    pub fn get_optimal_batch_size(&self, record_batch: &RecordBatch) -> usize {
        let estimated_row_bytes = self.estimate_row_size(record_batch);
        let memory_constrained_size = self.target_memory_bytes / estimated_row_bytes.max(1);

        // Use the smaller of current batch size or memory-constrained size
        let current = self.current_batch_size.load(Ordering::Relaxed);
        current
            .min(memory_constrained_size)
            .max(self.min_batch_size)
    }

    /// Estimates average row size in bytes.
    fn estimate_row_size(&self, record_batch: &RecordBatch) -> usize {
        if record_batch.num_rows() == 0 {
            return 100; // Default estimate
        }

        // Calculate total memory usage and divide by row count
        let total_bytes: usize = record_batch
            .columns()
            .iter()
            .map(|col| col.get_array_memory_size())
            .sum();

        total_bytes / record_batch.num_rows()
    }

    /// Adjusts batch size based on recent performance samples.
    fn adjust_batch_size_with_samples(&self, samples: &VecDeque<(u64, usize)>) {
        if samples.len() < 3 {
            return; // Need more samples
        }

        let avg_duration =
            samples.iter().map(|(duration, _)| *duration).sum::<u64>() / samples.len() as u64;

        let adjustment_factor = if avg_duration > self.target_duration_ms {
            // Too slow, reduce batch size
            0.9
        } else if avg_duration < self.target_duration_ms / 2 {
            // Too fast, increase batch size
            1.2
        } else {
            // Just right, small increase
            1.05
        };

        let current = self.current_batch_size.load(Ordering::Relaxed);
        let new_size = ((current as f64) * adjustment_factor) as usize;
        let new_size = new_size.max(self.min_batch_size).min(self.max_batch_size);

        self.current_batch_size.store(new_size, Ordering::Relaxed);
    }
}

/// Errors that occur when creating tables
#[derive(Debug, thiserror::Error)]
pub enum CreateTableError {
    /// Failed to build encoder for Arrow schema
    #[error("Failed to build PostgreSQL encoder for table '{table_name}'")]
    BuildEncoder {
        table_name: String,
        #[source]
        source: arrow_to_postgres::error::Error,
    },

    /// Failed to execute CREATE TABLE DDL statement
    #[error("Failed to execute CREATE TABLE for '{table_name}' with {num_columns} columns")]
    ExecuteDdl {
        table_name: String,
        num_columns: usize,
        #[source]
        source: sqlx::Error,
    },
}

/// Errors that occur when inserting batches
#[derive(Debug, thiserror::Error)]
pub enum InsertBatchError {
    /// Failed to convert RecordBatch to PostgreSQL COPY format
    #[error(
        "Failed to convert RecordBatch to PostgreSQL COPY format for table '{table_name}' ({num_rows} rows)"
    )]
    ConvertToCopyFormat {
        table_name: String,
        num_rows: usize,
        #[source]
        source: crate::arrow::ToPostgresCopyError,
    },

    /// Failed to begin transaction
    #[error("Failed to begin transaction for inserting into '{table_name}'")]
    TransactionBegin {
        table_name: String,
        #[source]
        source: sqlx::Error,
    },

    /// Failed to create temporary table
    #[error("Failed to create temp table '{temp_table}' for inserting into '{table_name}'")]
    CreateTempTable {
        table_name: String,
        temp_table: String,
        #[source]
        source: sqlx::Error,
    },

    /// Failed to initiate COPY operation
    #[error("Failed to initiate COPY for temp table '{temp_table}'")]
    CopyInitiate {
        temp_table: String,
        #[source]
        source: sqlx::Error,
    },

    /// Failed to send COPY data
    #[error("Failed to send COPY data to temp table '{temp_table}'")]
    CopySend {
        temp_table: String,
        #[source]
        source: sqlx::Error,
    },

    /// Failed to finish COPY operation
    #[error("Failed to finish COPY into temp table '{temp_table}'")]
    CopyFinish {
        temp_table: String,
        #[source]
        source: sqlx::Error,
    },

    /// Failed to INSERT with conflict handling
    #[error(
        "Failed to INSERT {num_rows} rows from temp table '{temp_table}' into '{table_name}' with conflict handling"
    )]
    InsertWithConflict {
        table_name: String,
        temp_table: String,
        num_rows: usize,
        #[source]
        source: sqlx::Error,
    },

    /// Failed to commit transaction
    #[error("Failed to commit transaction for inserting into '{table_name}'")]
    TransactionCommit {
        table_name: String,
        #[source]
        source: sqlx::Error,
    },
}

/// Errors that occur when deleting by transaction range
#[derive(Debug, thiserror::Error)]
pub enum DeleteByTxRangeError {
    /// Failed to execute DELETE statement
    #[error("Failed to DELETE rows from '{table_name}' for transaction range {start}..{end}")]
    ExecuteDelete {
        table_name: String,
        start: u64,
        end: u64,
        #[source]
        source: sqlx::Error,
    },
}

/// Database engine for ampsync operations.
///
/// Provides methods for:
/// - Creating tables with composite primary keys (_tx_id, _row_index)
/// - Inserting batches using PostgreSQL COPY protocol
/// - Deleting rows by transaction ID range (for Undo events)
/// - Adaptive batch size optimization based on performance
#[derive(Clone)]
pub struct Engine {
    /// PostgreSQL connection pool
    pool: PgPool,
    /// Adaptive batch size manager
    batch_manager: AdaptiveBatchManager,
}

impl Engine {
    /// Create a new Engine with the given connection pool.
    ///
    /// # Arguments
    /// - `pool`: PostgreSQL connection pool
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            batch_manager: AdaptiveBatchManager::default(),
        }
    }

    /// Maximum duration to retry database operations before giving up
    const DB_OPERATION_MAX_RETRY_DURATION: Duration = Duration::from_secs(30);

    /// Returns retry policy for database operations.
    fn db_retry_policy() -> ExponentialBuilder {
        ExponentialBuilder::default()
            .with_min_delay(Duration::from_millis(50))
            .with_max_delay(Duration::from_secs(5))
            .with_max_times(50) // High limit, circuit breaker will stop us first
    }

    /// Returns true if the database error is retryable.
    fn is_retryable_db_error(err: &sqlx::Error) -> bool {
        match err {
            sqlx::Error::Database(db_err) => {
                // PostgreSQL error codes for temporary issues
                db_err.code().is_some_and(|code| {
                    matches!(
                        code.as_ref(),
                        "53300" | // Too many connections
                        "53400" | // Configuration limit exceeded
                        "40001" | // Serialization failure
                        "40P01" | // Deadlock detected
                        "08006" | // Connection failure
                        "08001" | // Unable to connect to server
                        "57P03" // Database starting up
                    )
                })
            }
            sqlx::Error::Io(_) => true,        // I/O errors are retryable
            sqlx::Error::PoolTimedOut => true, // Pool timeout, retry
            _ => false,
        }
    }

    /// Creates a circuit breaker-aware retryable condition.
    ///
    /// Stops retrying after max_duration even if error is retryable.
    fn create_retryable_with_circuit_breaker(
        max_duration: Duration,
    ) -> impl Fn(&sqlx::Error) -> bool {
        let start_time = Instant::now();
        move |err: &sqlx::Error| -> bool {
            let elapsed = start_time.elapsed();
            if elapsed >= max_duration {
                tracing::error!(
                    error = %err, error_source = logging::error_source(&err),
                    elapsed_secs = elapsed.as_secs(),
                    max_duration_secs = max_duration.as_secs(),
                    "db_operation_circuit_breaker_triggered"
                );
                return false; // Circuit breaker: stop retrying
            }
            Self::is_retryable_db_error(err)
        }
    }

    /// Creates a table with composite primary key `(_tx_id, _row_index)`.
    ///
    /// # Composite Primary Key Design
    ///
    /// Multiple rows in a batch share the same transaction ID:
    /// ```text
    /// TransactionEvent::Data { id: 42, batch: 100 rows }
    ///   → All 100 rows have _tx_id = 42
    /// ```
    ///
    /// We add `_row_index` (0-based position in batch) for uniqueness:
    /// - Row 0: (_tx_id=42, _row_index=0)
    /// - Row 1: (_tx_id=42, _row_index=1)
    /// - ...
    ///
    /// # Idempotency via Retraction
    ///
    /// This design achieves idempotency through retraction, not deduplication:
    /// 1. On crash: Uncommitted transactions are DELETED (Undo event)
    /// 2. On retry: New transaction ID assigned (monotonic counter)
    /// 3. No conflict: Old data deleted, new data has different tx_id
    ///
    /// Row ordering within a batch doesn't matter across retries because
    /// each retry gets a fresh transaction ID.
    pub async fn create_table(
        &self,
        table_name: &str,
        stream_schema: SchemaRef,
    ) -> Result<(), CreateTableError> {
        // Build schema with system columns prepended
        // System columns: _tx_id, _row_index
        let mut fields = vec![
            Arc::new(Field::new("_tx_id", DataType::Int64, false)),
            Arc::new(Field::new("_row_index", DataType::Int32, false)),
        ];
        fields.extend(stream_schema.fields().iter().cloned());
        let full_schema = Schema::new(fields);

        // Get PostgreSQL type mapping from arrow-to-postgres
        let encoder = ArrowToPostgresBinaryEncoder::try_new(&full_schema).map_err(|err| {
            CreateTableError::BuildEncoder {
                table_name: table_name.to_string(),
                source: err,
            }
        })?;
        let pg_schema = encoder.schema();

        // Build column definitions with quoted identifiers
        let mut columns = Vec::new();
        for (name, column) in &pg_schema.columns {
            columns.push(sql::column_definition(
                name,
                &column.data_type.to_ddl_string(),
                column.nullable,
            ));
        }

        let num_columns = columns.len();

        // Build CREATE TABLE statement with safe identifier quoting
        let ddl = sql::create_table(table_name, &columns.join(", "), "_tx_id, _row_index");

        let table_name_owned = table_name.to_string();
        let ddl_clone = ddl.clone();

        // Execute DDL with retry logic
        let create_fn = || async { sqlx::query(&ddl_clone).execute(&self.pool).await };

        create_fn
            .retry(Self::db_retry_policy())
            .when(Self::create_retryable_with_circuit_breaker(
                Self::DB_OPERATION_MAX_RETRY_DURATION,
            ))
            .notify(|err: &sqlx::Error, duration: Duration| {
                tracing::warn!(
                    table = table_name_owned.as_str(),
                    error = %err, error_source = logging::error_source(&err),
                    retry_after = ?duration,
                    "db_create_table_retry"
                );
            })
            .await
            .map_err(|err| CreateTableError::ExecuteDdl {
                table_name: table_name_owned,
                num_columns,
                source: err,
            })?;
        Ok(())
    }

    /// Insert a RecordBatch into a table using PostgreSQL COPY protocol.
    ///
    /// This method automatically chunks large batches based on the optimal size
    /// calculated by the AdaptiveBatchManager. Each chunk is processed independently
    /// with performance tracking to continuously optimize batch sizes.
    ///
    /// # Arguments
    ///
    /// - `table_name`: Target table name
    /// - `batch`: RecordBatch with transaction metadata columns
    ///
    /// # Errors
    ///
    /// Returns an error if any chunk insertion fails.
    pub async fn insert_batch(
        &self,
        table_name: &str,
        batch: RecordBatch,
    ) -> Result<(), InsertBatchError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let total_rows = batch.num_rows();
        let optimal_batch_size = self.batch_manager.get_optimal_batch_size(&batch);

        tracing::debug!(
            table = table_name,
            total_rows = total_rows,
            batch_size = optimal_batch_size,
            "processing_batch_chunks"
        );

        // If the batch is smaller than optimal size, process it all at once
        if total_rows <= optimal_batch_size {
            let start = Instant::now();
            let result = self.insert_batch_chunk(table_name, batch).await;

            match result {
                Ok(()) => {
                    let duration = start.elapsed();
                    self.batch_manager
                        .record_success(duration, total_rows)
                        .await;
                    Ok(())
                }
                Err(e) => {
                    self.batch_manager.record_error();
                    Err(e)
                }
            }
        } else {
            // Split the batch into optimal-sized chunks
            let mut start_row = 0;
            while start_row < total_rows {
                let end_row = (start_row + optimal_batch_size).min(total_rows);
                let chunk_size = end_row - start_row;

                // Create a slice of the record batch
                let chunk = batch.slice(start_row, chunk_size);

                // Process the chunk with performance tracking
                let chunk_start_time = Instant::now();
                match self.insert_batch_chunk(table_name, chunk).await {
                    Ok(()) => {
                        let chunk_duration = chunk_start_time.elapsed();
                        self.batch_manager
                            .record_success(chunk_duration, chunk_size)
                            .await;

                        tracing::debug!(
                            table = table_name,
                            start_row = start_row,
                            end_row = end_row,
                            rows = chunk_size,
                            duration_ms = chunk_duration.as_millis(),
                            "chunk_processed"
                        );
                    }
                    Err(e) => {
                        self.batch_manager.record_error();
                        return Err(e);
                    }
                }

                start_row = end_row;
            }

            tracing::info!(
                table = table_name,
                total_rows = total_rows,
                "batch_processing_complete"
            );

            Ok(())
        }
    }

    /// Insert a single batch chunk using PostgreSQL COPY protocol.
    ///
    /// This method provides idempotent batch insertion using a temporary table
    /// approach with ON CONFLICT handling. The batch must already include
    /// transaction metadata columns (`_tx_id` and `_row_index`).
    ///
    /// # Algorithm
    ///
    /// 1. Create a temporary table with the same schema as the target
    /// 2. Use COPY BINARY protocol to load data into temp table (fast)
    /// 3. INSERT from temp table with ON CONFLICT DO NOTHING (idempotent)
    /// 4. Commit transaction
    ///
    /// # Performance
    ///
    /// The COPY protocol provides significant performance improvements over
    /// individual INSERT statements:
    /// - 10-100x faster for large batches
    /// - Reduced network overhead (binary format)
    /// - Single round-trip for entire batch
    ///
    /// # Arguments
    ///
    /// - `table_name`: Target table name
    /// - `batch`: RecordBatch with transaction metadata columns
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Transaction creation fails
    /// - Temp table creation fails
    /// - COPY operation fails
    /// - INSERT with conflict handling fails
    /// - Transaction commit fails
    async fn insert_batch_chunk(
        &self,
        table_name: &str,
        batch: RecordBatch,
    ) -> Result<(), InsertBatchError> {
        let num_rows = batch.num_rows();
        let table_name_owned = table_name.to_string();

        // Convert to Postgres COPY binary format (not a DB operation, no retry)
        let copy_data = crate::arrow::to_postgres_copy(batch.clone()).map_err(|err| {
            InsertBatchError::ConvertToCopyFormat {
                table_name: table_name.to_string(),
                num_rows,
                source: err,
            }
        })?;

        // Use COPY protocol via temp table for conflict handling
        let temp_table = format!("_temp_{}", Uuid::new_v4().simple());

        // Build SQL statements with safe identifier quoting
        let create_temp_sql = sql::create_temp_table_like(&temp_table, &table_name_owned);
        let copy_sql = sql::copy_from_stdin(&temp_table);
        let insert_sql =
            sql::insert_from_select(&table_name_owned, &temp_table, "_tx_id, _row_index");

        // Wrap the entire database transaction with retry logic
        let insert_fn = || async {
            let mut tx = self.pool.begin().await?;

            // Create temp table (same schema as target)
            sqlx::query(&create_temp_sql).execute(&mut *tx).await?;

            // COPY into temp table
            let mut copy = tx.copy_in_raw(&copy_sql).await?;
            copy.send(copy_data.clone()).await?;
            copy.finish().await?;

            // Insert from temp with conflict handling
            sqlx::query(&insert_sql).execute(&mut *tx).await?;

            tx.commit().await?;
            Ok::<(), sqlx::Error>(())
        };

        // Execute with retry and circuit breaker
        insert_fn
            .retry(Self::db_retry_policy())
            .when(Self::create_retryable_with_circuit_breaker(
                Self::DB_OPERATION_MAX_RETRY_DURATION,
            ))
            .notify(|err: &sqlx::Error, duration: Duration| {
                tracing::warn!(
                    table = table_name,
                    error = %err, error_source = logging::error_source(&err),
                    retry_after = ?duration,
                    "db_insert_retry"
                );
            })
            .await
            .map_err(|err| {
                // Convert sqlx::Error back to InsertBatchError
                match &err {
                    sqlx::Error::Database(_) => InsertBatchError::TransactionCommit {
                        table_name: table_name_owned.clone(),
                        source: err,
                    },
                    _ => InsertBatchError::TransactionBegin {
                        table_name: table_name_owned.clone(),
                        source: err,
                    },
                }
            })
    }

    /// Delete rows by transaction ID range (inclusive).
    ///
    /// Used to process Undo events when uncommitted transactions need to be
    /// removed due to crashes or reorgs. The range is inclusive on both ends.
    ///
    /// # Arguments
    ///
    /// - `table_name`: Target table name
    /// - `start`: Starting transaction ID (inclusive)
    /// - `end`: Ending transaction ID (inclusive)
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` even if no rows were deleted (idempotent).
    ///
    /// # Errors
    ///
    /// Returns an error if the DELETE query fails to execute.
    pub async fn delete_by_tx_range(
        &self,
        table_name: &str,
        start: u64,
        end: u64,
    ) -> Result<(), DeleteByTxRangeError> {
        let table_name_owned = table_name.to_string();

        // Build DELETE statement with safe identifier quoting
        let delete_sql = sql::delete_between(table_name, "_tx_id");

        // Execute DELETE with retry logic
        let delete_fn = || async {
            sqlx::query(&delete_sql)
                .bind(start as i64)
                .bind(end as i64)
                .execute(&self.pool)
                .await
        };

        let result = delete_fn
            .retry(Self::db_retry_policy())
            .when(Self::create_retryable_with_circuit_breaker(
                Self::DB_OPERATION_MAX_RETRY_DURATION,
            ))
            .notify(|err: &sqlx::Error, duration: Duration| {
                tracing::warn!(
                    table = table_name_owned.as_str(),
                    start = start,
                    end = end,
                    error = %err, error_source = logging::error_source(&err),
                    retry_after = ?duration,
                    "db_delete_retry"
                );
            })
            .await
            .map_err(|err| DeleteByTxRangeError::ExecuteDelete {
                table_name: table_name_owned.clone(),
                start,
                end,
                source: err,
            })?;

        tracing::debug!(
            table = table_name,
            start = start,
            end = end,
            rows_deleted = result.rows_affected(),
            "deleted_by_tx_range"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{Int32Array, Int64Array};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    #[tokio::test]
    async fn test_create_table() {
        let _pg = pgtemp::PgTempDB::new();
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .connect(&_pg.connection_uri())
            .await
            .unwrap();
        let engine = Engine::new(pool);

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        engine.create_table("test_table", schema).await.unwrap();

        // Verify table exists and has correct schema
        let result: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'test_table'",
        )
        .fetch_one(&engine.pool)
        .await
        .unwrap();

        assert_eq!(result.0, 4); // _tx_id, _row_index, id, name
    }

    #[tokio::test]
    async fn test_insert_and_delete() {
        let _pg = pgtemp::PgTempDB::new();
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .connect(&_pg.connection_uri())
            .await
            .unwrap();
        let engine = Engine::new(pool);

        // Create table
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        engine.create_table("test_table", schema).await.unwrap();

        // Create batch with system columns (_tx_id, _row_index) + user columns
        let num_rows = 3;
        let batch_with_meta = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("_tx_id", DataType::Int64, false),
                Field::new("_row_index", DataType::Int32, false),
                Field::new("value", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![42i64; num_rows])), // _tx_id
                Arc::new(Int32Array::from(vec![0i32, 1i32, 2i32])), // _row_index
                Arc::new(Int64Array::from(vec![100, 200, 300])),   // value
            ],
        )
        .unwrap();

        // Insert
        engine
            .insert_batch("test_table", batch_with_meta)
            .await
            .unwrap();

        // Verify insert
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM test_table")
            .fetch_one(&engine.pool)
            .await
            .unwrap();
        assert_eq!(count.0, 3);

        // Delete by range
        engine
            .delete_by_tx_range("test_table", 42, 42)
            .await
            .unwrap();

        // Verify delete
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM test_table")
            .fetch_one(&engine.pool)
            .await
            .unwrap();
        assert_eq!(count.0, 0);
    }

    #[tokio::test]
    async fn test_idempotency() {
        let _pg = pgtemp::PgTempDB::new();
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .connect(&_pg.connection_uri())
            .await
            .unwrap();
        let engine = Engine::new(pool);

        // Create table
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        engine.create_table("test_table", schema).await.unwrap();

        // Create batch with system columns (_tx_id, _row_index) + user columns
        let batch_with_meta = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("_tx_id", DataType::Int64, false),
                Field::new("_row_index", DataType::Int32, false),
                Field::new("value", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![42i64])), // _tx_id
                Arc::new(Int32Array::from(vec![0i32])),  // _row_index
                Arc::new(Int64Array::from(vec![100])),   // value
            ],
        )
        .unwrap();

        // First insert
        engine
            .insert_batch("test_table", batch_with_meta.clone())
            .await
            .unwrap();

        // Second insert (should be idempotent)
        engine
            .insert_batch("test_table", batch_with_meta)
            .await
            .unwrap();

        // Verify only one row
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM test_table")
            .fetch_one(&engine.pool)
            .await
            .unwrap();
        assert_eq!(count.0, 1);
    }

    #[test]
    fn test_adaptive_batch_manager_default_configuration() {
        let mgr = AdaptiveBatchManager::default();
        // Verify defaults: min=100, max=50000, initial=1000
        assert_eq!(mgr.current_batch_size.load(Ordering::Relaxed), 1000);
        assert_eq!(mgr.min_batch_size, 100);
        assert_eq!(mgr.max_batch_size, 50_000);
    }

    #[tokio::test]
    async fn test_adaptive_batch_manager_record_success_increases_size_when_fast() {
        let mgr = AdaptiveBatchManager::default();

        // Record 5 fast batches (100ms each)
        for _ in 0..5 {
            mgr.record_success(Duration::from_millis(100), 1000).await;
        }

        // Batch size should increase
        let final_size = mgr.current_batch_size.load(Ordering::Relaxed);
        assert!(final_size > 1000, "Expected increase, got {}", final_size);
    }

    #[tokio::test]
    async fn test_adaptive_batch_manager_record_success_decreases_size_when_slow() {
        let mgr = AdaptiveBatchManager::default();

        // Record 5 slow batches (2000ms each)
        for _ in 0..5 {
            mgr.record_success(Duration::from_millis(2000), 1000).await;
        }

        // Batch size should decrease
        let final_size = mgr.current_batch_size.load(Ordering::Relaxed);
        assert!(final_size < 1000, "Expected decrease, got {}", final_size);
    }

    #[test]
    fn test_adaptive_batch_manager_record_error_reduces_after_three_errors() {
        let mgr = AdaptiveBatchManager::default();

        let initial_size = mgr.current_batch_size.load(Ordering::Relaxed);

        // Record 3 errors
        mgr.record_error();
        mgr.record_error();
        mgr.record_error();

        // Should reduce by 30%
        let final_size = mgr.current_batch_size.load(Ordering::Relaxed);
        let expected = (initial_size as f64 * 0.7) as usize;
        assert_eq!(final_size, expected.max(100));
    }

    #[test]
    fn test_adaptive_batch_manager_get_optimal_respects_bounds() {
        let mgr = AdaptiveBatchManager::default();

        // Create a small test batch
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();

        let optimal = mgr.get_optimal_batch_size(&batch);

        // Should respect min/max bounds
        assert!(optimal >= mgr.min_batch_size);
        assert!(optimal <= mgr.max_batch_size);
    }

    #[test]
    fn test_is_retryable_recognizes_connection_errors() {
        // I/O errors are retryable
        let io_err = sqlx::Error::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "connection reset",
        ));
        assert!(Engine::is_retryable_db_error(&io_err));

        // Pool timeout is retryable
        let pool_err = sqlx::Error::PoolTimedOut;
        assert!(Engine::is_retryable_db_error(&pool_err));
    }

    #[test]
    fn test_is_retryable_rejects_permanent_errors() {
        // Row not found is not retryable
        let not_found_err = sqlx::Error::RowNotFound;
        assert!(!Engine::is_retryable_db_error(&not_found_err));

        // Column not found is not retryable
        let column_err = sqlx::Error::ColumnNotFound("id".to_string());
        assert!(!Engine::is_retryable_db_error(&column_err));
    }

    #[test]
    fn test_db_retry_policy_configuration() {
        let policy = Engine::db_retry_policy();
        // Just verify we can create the policy - actual retry behavior
        // is tested in integration tests
        let _builder = policy;
    }

    #[tokio::test]
    async fn test_batch_chunking_large_batch() {
        let _pg = pgtemp::PgTempDB::new();
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .connect(&_pg.connection_uri())
            .await
            .unwrap();
        let engine = Engine::new(pool);

        // Create table
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        engine.create_table("test_chunking", schema).await.unwrap();

        // Create a large batch (2000 rows) with system columns
        let num_rows = 2000;
        let values: Vec<i64> = (0..num_rows as i64).collect();
        let row_indices: Vec<i32> = (0..num_rows as i32).collect();
        let batch_with_meta = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("_tx_id", DataType::Int64, false),
                Field::new("_row_index", DataType::Int32, false),
                Field::new("value", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1i64; num_rows])), // _tx_id
                Arc::new(Int32Array::from(row_indices)),          // _row_index
                Arc::new(Int64Array::from(values)),               // value
            ],
        )
        .unwrap();

        // Insert large batch - should be chunked automatically
        engine
            .insert_batch("test_chunking", batch_with_meta)
            .await
            .unwrap();

        // Verify all rows were inserted
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM test_chunking")
            .fetch_one(&engine.pool)
            .await
            .unwrap();
        assert_eq!(count.0, 2000);
    }
}

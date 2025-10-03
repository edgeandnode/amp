use std::{
    collections::VecDeque,
    ops::Deref,
    time::{Duration, Instant},
};

use backon::{ExponentialBuilder, Retryable};
use bytes::BytesMut;
use common::{BoxError, arrow::array::RecordBatch};
use datafusion::arrow::datatypes::DataType as ArrowDataType;
use datasets_derived::manifest::{ArrowSchema, Field};
use lazy_static::lazy_static;
use nozzle_client::InvalidationRange;
use sqlx::{Pool, Postgres};

use crate::{conn::DbConnPool, pgpq::ArrowToPostgresBinaryEncoder};

lazy_static! {
    /// Create a static HashSet of reserved SQL keywords.
    /// If a column in the derived arrow schema exists in this list, it needs to be wrapped in quotes,
    /// or the CREATE TABLE call will fail.
    static ref RESERVED_KEYWORDS: std::collections::HashSet<String> = {
        let mut reserved_sql_keywords = std::collections::HashSet::new();
        reserved_sql_keywords.insert("as".to_string());
        reserved_sql_keywords.insert("from".to_string());
        reserved_sql_keywords.insert("to".to_string());
        reserved_sql_keywords.insert("select".to_string());
        reserved_sql_keywords.insert("array".to_string());
        reserved_sql_keywords.insert("sum".to_string());
        reserved_sql_keywords.insert("all".to_string());
        reserved_sql_keywords.insert("allocate".to_string());
        reserved_sql_keywords.insert("alter".to_string());
        reserved_sql_keywords.insert("table".to_string());
        reserved_sql_keywords.insert("blob".to_string());

        reserved_sql_keywords
    };
}

/// Convert Arrow schema to PostgreSQL CREATE TABLE statement
///
/// Deduplicates fields by name - if multiple fields have the same name,
/// only the first occurrence is used. This handles cases where manifests
/// contain duplicate field definitions.
pub fn arrow_schema_to_postgres_ddl(
    table_name: &str,
    schema: &ArrowSchema,
) -> Result<String, BoxError> {
    let mut columns = Vec::new();
    let mut seen_names = std::collections::HashSet::new();

    for field in &schema.fields {
        // Skip duplicate field names
        if !seen_names.insert(field.name.clone()) {
            continue;
        }

        let column_def = arrow_field_to_postgres_column(field)?;
        columns.push(column_def);
    }

    let ddl = format!(
        "CREATE TABLE IF NOT EXISTS {} (\n  {}\n)",
        table_name,
        columns.join(",\n  ")
    );

    Ok(ddl)
}

/// Convert a single Arrow field to PostgreSQL column definition
fn arrow_field_to_postgres_column(field: &Field) -> Result<String, BoxError> {
    let pg_type = arrow_type_to_postgres_type(field.type_.as_arrow())?;
    let nullable = if field.nullable { "" } else { " NOT NULL" };

    // wrap reserved keywords in quotes
    let column_name = if RESERVED_KEYWORDS.contains(&field.name) {
        format!("\"{}\"", field.name.clone())
    } else {
        field.name.clone()
    };

    Ok(format!("{} {}{}", column_name, pg_type, nullable))
}

/// Map Arrow DataType to PostgreSQL type
fn arrow_type_to_postgres_type(data_type: &ArrowDataType) -> Result<String, BoxError> {
    let pg_type = match data_type {
        // Integer types
        ArrowDataType::Int8 => "SMALLINT",
        ArrowDataType::Int16 => "SMALLINT",
        ArrowDataType::Int32 => "INTEGER",
        ArrowDataType::Int64 => "BIGINT",
        ArrowDataType::UInt8 => "SMALLINT",
        ArrowDataType::UInt16 => "INTEGER",
        ArrowDataType::UInt32 => "BIGINT",
        ArrowDataType::UInt64 => "NUMERIC(20)", // PostgreSQL doesn't have unsigned 64-bit

        // Floating point types
        ArrowDataType::Float16 => "REAL",
        ArrowDataType::Float32 => "REAL",
        ArrowDataType::Float64 => "DOUBLE PRECISION",

        // String types
        ArrowDataType::Utf8 => "TEXT",
        ArrowDataType::LargeUtf8 => "TEXT",

        // Binary types
        ArrowDataType::Binary => "BYTEA",
        ArrowDataType::LargeBinary => "BYTEA",
        ArrowDataType::FixedSizeBinary(_) => "BYTEA",

        // Boolean
        ArrowDataType::Boolean => "BOOLEAN",

        // Date and time types
        ArrowDataType::Date32 => "DATE",
        ArrowDataType::Date64 => "DATE",
        ArrowDataType::Time32(_) => "TIME",
        ArrowDataType::Time64(_) => "TIME",
        ArrowDataType::Timestamp(_, timezone) => match timezone {
            Some(_) => "TIMESTAMPTZ",
            None => "TIMESTAMP",
        },

        // Decimal types
        ArrowDataType::Decimal128(precision, scale) => {
            return Ok(format!("NUMERIC({}, {})", precision, scale));
        }

        // List and struct types - map to JSONB for flexibility
        ArrowDataType::List(_) => "JSONB",
        ArrowDataType::LargeList(_) => "JSONB",
        ArrowDataType::FixedSizeList(_, _) => "JSONB",
        ArrowDataType::Struct(_) => "JSONB",
        ArrowDataType::Map(_, _) => "JSONB",

        // Other types
        ArrowDataType::Null => "TEXT", // Fallback for null type
        ArrowDataType::Dictionary(_, value_type) => {
            // For dictionary encoding, use the value type
            return arrow_type_to_postgres_type(value_type);
        }

        // Unsupported types - fall back to TEXT
        _ => "TEXT",
    };

    Ok(pg_type.to_string())
}

/// Adaptive batch manager that optimizes batch sizes based on performance metrics
#[derive(Debug, Clone)]
pub struct AdaptiveBatchManager {
    /// Current target batch size (in rows)
    current_batch_size: usize,
    /// Minimum allowed batch size
    min_batch_size: usize,
    /// Maximum allowed batch size
    max_batch_size: usize,
    /// Target processing time per batch (in milliseconds)
    target_duration_ms: u64,
    /// Recent performance samples (duration in ms, rows processed)
    /// Using VecDeque for O(1) pop_front instead of Vec's O(n) remove(0)
    recent_samples: VecDeque<(u64, usize)>,
    /// Maximum number of samples to keep
    max_samples: usize,
    /// Number of consecutive errors (for reducing batch size)
    error_count: usize,
    /// Target memory usage per batch (in bytes)
    target_memory_bytes: usize,
}

impl Default for AdaptiveBatchManager {
    fn default() -> Self {
        Self {
            current_batch_size: 1000, // Start with 1K rows
            min_batch_size: 100,      // Minimum 100 rows
            max_batch_size: 50_000,   // Maximum 50K rows
            target_duration_ms: 1000, // Target 1 second per batch
            recent_samples: VecDeque::with_capacity(10),
            max_samples: 10,
            error_count: 0,
            target_memory_bytes: 50 * 1024 * 1024, // Target 50MB per batch
        }
    }
}

impl AdaptiveBatchManager {
    /// Record a successful batch processing performance
    pub fn record_success(&mut self, duration: Duration, rows_processed: usize) {
        let duration_ms = duration.as_millis() as u64;

        // Reset error count on success
        self.error_count = 0;

        // Add to recent samples
        self.recent_samples.push_back((duration_ms, rows_processed));
        if self.recent_samples.len() > self.max_samples {
            self.recent_samples.pop_front();
        }

        // Adjust batch size based on performance
        self.adjust_batch_size();

        tracing::debug!(
            "Batch performance: {}ms for {} rows, new batch size: {}",
            duration_ms,
            rows_processed,
            self.current_batch_size
        );
    }

    /// Record a batch processing error
    pub fn record_error(&mut self) {
        self.error_count += 1;

        // Reduce batch size on repeated errors
        if self.error_count >= 3 {
            let new_size = (self.current_batch_size as f64 * 0.7) as usize;
            self.current_batch_size = new_size.max(self.min_batch_size);
            self.error_count = 0; // Reset after adjustment

            tracing::warn!(
                "Reducing batch size to {} due to repeated errors",
                self.current_batch_size
            );
        }
    }

    /// Get the optimal batch size for a RecordBatch considering memory constraints
    pub fn get_optimal_batch_size(&self, record_batch: &RecordBatch) -> usize {
        let estimated_row_bytes = self.estimate_row_size(record_batch);
        let memory_constrained_size = self.target_memory_bytes / estimated_row_bytes.max(1);

        // Use the smaller of current batch size or memory-constrained size
        self.current_batch_size
            .min(memory_constrained_size)
            .max(self.min_batch_size)
    }

    /// Estimate the size of a single row in bytes
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

    /// Adjust batch size based on recent performance samples
    fn adjust_batch_size(&mut self) {
        if self.recent_samples.len() < 3 {
            return; // Need more samples
        }

        let avg_duration = self
            .recent_samples
            .iter()
            .map(|(duration, _)| *duration)
            .sum::<u64>()
            / self.recent_samples.len() as u64;

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

        let new_size = (self.current_batch_size as f64 * adjustment_factor) as usize;
        self.current_batch_size = new_size.max(self.min_batch_size).min(self.max_batch_size);
    }
}

#[derive(Clone)]
pub struct AmpsyncDbEngine {
    pool: Pool<Postgres>,
    batch_manager: std::sync::Arc<tokio::sync::Mutex<AdaptiveBatchManager>>,
}

impl AmpsyncDbEngine {
    pub fn new(pool: &DbConnPool) -> Self {
        Self {
            pool: pool.deref().clone(),
            batch_manager: std::sync::Arc::new(tokio::sync::Mutex::new(
                AdaptiveBatchManager::default(),
            )),
        }
    }

    /// Create a retry policy for database operations
    fn db_retry_policy() -> ExponentialBuilder {
        ExponentialBuilder::default()
            .with_min_delay(Duration::from_millis(50))
            .with_max_delay(Duration::from_secs(5))
            .with_max_times(5)
    }

    /// Check if a database error should be retried
    fn is_retryable_db_error(err: &sqlx::Error) -> bool {
        match err {
            sqlx::Error::Database(db_err) => {
                // PostgreSQL error codes for temporary issues
                db_err.code().map_or(false, |code| {
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

    /// Notify function for database operation retries
    fn notify_db_retry(err: &sqlx::Error, dur: Duration) {
        tracing::warn!(
            error = %err,
            "Database operation failed. Retrying in {:.1}s",
            dur.as_secs_f32()
        );
    }

    /// Create a database table based on Arrow schema
    pub async fn create_table_from_schema(
        &self,
        table_name: &str,
        schema: &ArrowSchema,
    ) -> Result<(), BoxError> {
        let ddl = arrow_schema_to_postgres_ddl(table_name, schema)?;

        tracing::info!("Ensuring table '{}' exists with DDL: {}", table_name, ddl);

        let pool = self.pool.clone();
        let ddl_query = ddl.clone();

        (|| {
            let pool = pool.clone();
            let ddl_query = ddl_query.clone();
            async move { sqlx::query(&ddl_query).execute(&pool).await }
        })
        .retry(Self::db_retry_policy())
        .when(Self::is_retryable_db_error)
        .notify(Self::notify_db_retry)
        .await
        .map_err(|e| format!("Failed to create table '{}': {}", table_name, e))?;

        Ok(())
    }

    /// Insert a RecordBatch into a PostgreSQL table using adaptive batch sizing
    pub async fn insert_record_batch(
        &self,
        table_name: &str,
        batch: &RecordBatch,
    ) -> Result<(), BoxError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let total_rows = batch.num_rows();
        let optimal_batch_size = {
            let batch_manager = self.batch_manager.lock().await;
            batch_manager.get_optimal_batch_size(batch)
        };

        tracing::debug!(
            "Processing {} rows in chunks of {} for table '{}'",
            total_rows,
            optimal_batch_size,
            table_name
        );

        // If the batch is smaller than optimal size, process it all at once
        if total_rows <= optimal_batch_size {
            return self.insert_batch_chunk(table_name, batch).await;
        }

        // Split the batch into optimal-sized chunks
        let mut start_row = 0;
        while start_row < total_rows {
            let end_row = (start_row + optimal_batch_size).min(total_rows);
            let chunk_size = end_row - start_row;

            // Create a slice of the record batch
            let chunk = batch.slice(start_row, chunk_size);

            // Process the chunk with performance tracking
            let chunk_start_time = Instant::now();
            match self.insert_batch_chunk(table_name, &chunk).await {
                Ok(()) => {
                    let chunk_duration = chunk_start_time.elapsed();
                    let mut batch_manager = self.batch_manager.lock().await;
                    batch_manager.record_success(chunk_duration, chunk_size);

                    tracing::debug!(
                        "Successfully processed chunk {}-{} ({} rows) in {}ms for table '{}'",
                        start_row,
                        end_row,
                        chunk_size,
                        chunk_duration.as_millis(),
                        table_name
                    );
                }
                Err(e) => {
                    let mut batch_manager = self.batch_manager.lock().await;
                    batch_manager.record_error();

                    return Err(format!(
                        "Failed to insert chunk {}-{} for table '{}': {}",
                        start_row, end_row, table_name, e
                    )
                    .into());
                }
            }

            start_row = end_row;
        }

        tracing::info!(
            "Successfully processed all {} rows for table '{}' using adaptive batching",
            total_rows,
            table_name
        );

        Ok(())
    }

    /// Insert a single batch chunk using high-performance bulk copy
    async fn insert_batch_chunk(
        &self,
        table_name: &str,
        batch: &RecordBatch,
    ) -> Result<(), BoxError> {
        // Create the pgpq encoder for this batch's schema
        let mut encoder = ArrowToPostgresBinaryEncoder::try_new(batch.schema().as_ref())
            .map_err(|e| format!("Failed to create pgpq encoder: {:?}", e))?;

        // Calculate exact buffer size needed to avoid reallocations
        let buffer_size = encoder
            .calculate_buffer_size(batch)
            .map_err(|e| format!("Failed to calculate buffer size: {:?}", e))?;

        // Pre-allocate exact capacity - much better than arbitrary estimate
        let mut buffer = BytesMut::with_capacity(buffer_size);

        // Write header
        encoder.write_header(&mut buffer);

        // Write the batch data
        encoder
            .write_batch(batch, &mut buffer)
            .map_err(|e| format!("Failed to encode batch with pgpq: {:?}", e))?;

        // Write footer
        encoder
            .write_footer(&mut buffer)
            .map_err(|e| format!("Failed to write pgpq footer: {:?}", e))?;

        // Get column names for the COPY command
        let schema_fields = batch.schema();
        let column_names: Vec<&str> = schema_fields
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        let columns_clause = column_names.join(", ");

        // Execute COPY FROM STDIN with binary format
        let copy_query = format!(
            "COPY {} ({}) FROM STDIN WITH (FORMAT BINARY)",
            table_name, columns_clause
        );

        let pool = self.pool.clone();
        let query = copy_query.clone();
        let buffer_data = buffer.freeze();

        // Use retry logic for the entire COPY operation
        (|| {
            let pool = pool.clone();
            let query = query.clone();
            let buffer_data = buffer_data.clone();
            async move {
                // Get a connection from the pool and execute the COPY command
                let mut conn = pool.acquire().await?;

                // Use PostgreSQL's COPY protocol for maximum performance
                let mut copy_in = conn.copy_in_raw(&query).await?;

                // Send the binary data
                copy_in.send(buffer_data).await?;

                // Finish the copy operation
                let rows_affected = copy_in.finish().await?;

                tracing::trace!(
                    "COPY operation completed: {} rows affected for {} input rows in table '{}'",
                    rows_affected,
                    batch.num_rows(),
                    table_name
                );

                Ok::<(), sqlx::Error>(())
            }
        })
        .retry(Self::db_retry_policy())
        .when(Self::is_retryable_db_error)
        .notify(Self::notify_db_retry)
        .await
        .map_err(|e| format!("Failed to bulk insert into table '{}': {}", table_name, e))?;

        Ok(())
    }

    /// Handle blockchain reorganization by deleting affected rows
    pub async fn handle_reorg(
        &self,
        table_name: &str,
        invalidation_ranges: &[InvalidationRange],
    ) -> Result<(), BoxError> {
        if invalidation_ranges.is_empty() {
            return Ok(());
        }

        // Build a WHERE clause that covers all invalidation ranges
        let mut where_conditions = Vec::new();
        for range in invalidation_ranges {
            let condition = format!(
                "(block_num >= {} AND block_num <= {})",
                range.numbers.start(),
                range.numbers.end()
            );
            where_conditions.push(condition);
        }

        let where_clause = where_conditions.join(" OR ");
        let delete_query = format!("DELETE FROM {} WHERE {}", table_name, where_clause);

        tracing::info!(
            "Handling reorg for table '{}' with {} invalidation ranges: {}",
            table_name,
            invalidation_ranges.len(),
            delete_query
        );

        let pool = self.pool.clone();
        let query = delete_query.clone();

        let result = (|| {
            let pool = pool.clone();
            let query = query.clone();
            async move { sqlx::query(&query).execute(&pool).await }
        })
        .retry(Self::db_retry_policy())
        .when(Self::is_retryable_db_error)
        .notify(Self::notify_db_retry)
        .await
        .map_err(|e| {
            format!(
                "Failed to delete invalidated rows from table '{}': {}",
                table_name, e
            )
        })?;

        let rows_deleted = result.rows_affected();
        if rows_deleted > 0 {
            tracing::warn!(
                "Deleted {} rows from table '{}' due to blockchain reorganization",
                rows_deleted,
                table_name
            );
        } else {
            tracing::debug!(
                "No rows needed deletion in table '{}' for the invalidation ranges",
                table_name
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::DataType as ArrowDataType;
    use datasets_common::manifest::DataType;

    use super::*;

    #[test]
    fn test_arrow_type_mappings() {
        assert_eq!(
            arrow_type_to_postgres_type(&ArrowDataType::Int32).unwrap(),
            "INTEGER"
        );
        assert_eq!(
            arrow_type_to_postgres_type(&ArrowDataType::Utf8).unwrap(),
            "TEXT"
        );
        assert_eq!(
            arrow_type_to_postgres_type(&ArrowDataType::Boolean).unwrap(),
            "BOOLEAN"
        );
        assert_eq!(
            arrow_type_to_postgres_type(&ArrowDataType::Float64).unwrap(),
            "DOUBLE PRECISION"
        );
        assert_eq!(
            arrow_type_to_postgres_type(&ArrowDataType::Binary).unwrap(),
            "BYTEA"
        );
    }

    #[test]
    fn test_arrow_type_mappings_comprehensive() {
        use datafusion::arrow::datatypes::TimeUnit;

        // Integer types
        assert_eq!(
            arrow_type_to_postgres_type(&ArrowDataType::Int8).unwrap(),
            "SMALLINT"
        );
        assert_eq!(
            arrow_type_to_postgres_type(&ArrowDataType::Int16).unwrap(),
            "SMALLINT"
        );
        assert_eq!(
            arrow_type_to_postgres_type(&ArrowDataType::Int64).unwrap(),
            "BIGINT"
        );

        // Unsigned integer types
        assert_eq!(
            arrow_type_to_postgres_type(&ArrowDataType::UInt8).unwrap(),
            "SMALLINT"
        );
        assert_eq!(
            arrow_type_to_postgres_type(&ArrowDataType::UInt32).unwrap(),
            "BIGINT"
        );
        assert_eq!(
            arrow_type_to_postgres_type(&ArrowDataType::UInt64).unwrap(),
            "NUMERIC(20)"
        );

        // Float types
        assert_eq!(
            arrow_type_to_postgres_type(&ArrowDataType::Float32).unwrap(),
            "REAL"
        );

        // String types
        assert_eq!(
            arrow_type_to_postgres_type(&ArrowDataType::LargeUtf8).unwrap(),
            "TEXT"
        );

        // Binary types
        assert_eq!(
            arrow_type_to_postgres_type(&ArrowDataType::LargeBinary).unwrap(),
            "BYTEA"
        );
        assert_eq!(
            arrow_type_to_postgres_type(&ArrowDataType::FixedSizeBinary(32)).unwrap(),
            "BYTEA"
        );

        // Date/Time types
        assert_eq!(
            arrow_type_to_postgres_type(&ArrowDataType::Date32).unwrap(),
            "DATE"
        );
        assert_eq!(
            arrow_type_to_postgres_type(&ArrowDataType::Timestamp(TimeUnit::Microsecond, None))
                .unwrap(),
            "TIMESTAMP"
        );
        assert_eq!(
            arrow_type_to_postgres_type(&ArrowDataType::Timestamp(
                TimeUnit::Microsecond,
                Some("+00:00".into())
            ))
            .unwrap(),
            "TIMESTAMPTZ"
        );

        // Decimal type
        assert_eq!(
            arrow_type_to_postgres_type(&ArrowDataType::Decimal128(38, 10)).unwrap(),
            "NUMERIC(38, 10)"
        );

        // Complex types
        assert_eq!(
            arrow_type_to_postgres_type(&ArrowDataType::List(Arc::new(
                datafusion::arrow::datatypes::Field::new("item", ArrowDataType::Int32, true)
            )))
            .unwrap(),
            "JSONB"
        );
    }

    #[test]
    fn test_arrow_field_to_postgres_column() {
        use datasets_common::manifest::DataType;

        // Non-nullable field
        let field = Field {
            name: "id".to_string(),
            type_: DataType(ArrowDataType::Int64),
            nullable: false,
        };
        assert_eq!(
            arrow_field_to_postgres_column(&field).unwrap(),
            "id BIGINT NOT NULL"
        );

        // Nullable field
        let field = Field {
            name: "name".to_string(),
            type_: DataType(ArrowDataType::Utf8),
            nullable: true,
        };
        assert_eq!(arrow_field_to_postgres_column(&field).unwrap(), "name TEXT");

        // Binary field
        let field = Field {
            name: "hash".to_string(),
            type_: DataType(ArrowDataType::FixedSizeBinary(32)),
            nullable: false,
        };
        assert_eq!(
            arrow_field_to_postgres_column(&field).unwrap(),
            "hash BYTEA NOT NULL"
        );
    }

    #[test]
    fn test_wraps_reserved_words_in_quotes() {
        let field = Field {
            name: "to".to_string(),
            type_: DataType(ArrowDataType::FixedSizeBinary(32)),
            nullable: false,
        };
        assert_eq!(
            arrow_field_to_postgres_column(&field).unwrap(),
            "\"to\" BYTEA NOT NULL"
        );

        let field = Field {
            name: "from".to_string(),
            type_: DataType(ArrowDataType::FixedSizeBinary(32)),
            nullable: false,
        };
        assert_eq!(
            arrow_field_to_postgres_column(&field).unwrap(),
            "\"from\" BYTEA NOT NULL"
        );
    }

    #[test]
    fn test_arrow_schema_to_postgres_ddl() {
        use datasets_common::manifest::DataType;

        let schema = ArrowSchema {
            fields: vec![
                Field {
                    name: "id".to_string(),
                    type_: DataType(ArrowDataType::UInt64),
                    nullable: false,
                },
                Field {
                    name: "name".to_string(),
                    type_: DataType(ArrowDataType::Utf8),
                    nullable: true,
                },
                Field {
                    name: "balance".to_string(),
                    type_: DataType(ArrowDataType::Decimal128(38, 18)),
                    nullable: false,
                },
                Field {
                    name: "created_at".to_string(),
                    type_: DataType(ArrowDataType::Timestamp(
                        datafusion::arrow::datatypes::TimeUnit::Microsecond,
                        Some("+00:00".into()),
                    )),
                    nullable: false,
                },
            ],
        };

        let ddl = arrow_schema_to_postgres_ddl("test_table", &schema).unwrap();

        // Verify the DDL structure
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS test_table"));
        assert!(ddl.contains("id NUMERIC(20) NOT NULL"));
        assert!(ddl.contains("name TEXT"));
        assert!(ddl.contains("balance NUMERIC(38, 18) NOT NULL"));
        assert!(ddl.contains("created_at TIMESTAMPTZ NOT NULL"));

        // Verify fields are separated by commas and newlines
        assert!(ddl.contains(",\n"));
    }

    #[test]
    fn test_arrow_schema_to_postgres_ddl_empty_schema() {
        let schema = ArrowSchema { fields: vec![] };

        let ddl = arrow_schema_to_postgres_ddl("empty_table", &schema).unwrap();

        // Should still create a valid (though empty) table
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS empty_table"));
    }
}

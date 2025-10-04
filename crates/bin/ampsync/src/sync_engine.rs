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
use nozzle_client::InvalidationRange;
use phf::phf_set;
use sqlx::{Acquire, Pool, Postgres};

use crate::{conn::DbConnPool, pgpq::ArrowToPostgresBinaryEncoder};

/// Represents the existing schema of a table in the database
#[derive(Debug)]
struct ExistingSchema {
    columns: std::collections::HashMap<String, ExistingColumn>,
}

/// Information about an existing column in the database
#[derive(Debug)]
struct ExistingColumn {
    data_type: String,
    #[allow(dead_code)] // Reserved for future nullable constraint validation
    is_nullable: bool,
}

/// Result of comparing two schemas
#[derive(Debug)]
struct SchemaDiff<'a> {
    /// Columns that exist in expected schema but not in database
    new_columns: Vec<&'a Field>,
    /// Columns that exist in database but not in expected schema
    dropped_columns: Vec<String>,
    /// Columns where the type has changed
    type_mismatches: Vec<String>,
}

/// Compile-time perfect hash set of reserved SQL keywords.
///
/// If a column in the derived arrow schema exists in this set, it needs to be wrapped in quotes,
/// or the CREATE TABLE call will fail. Using phf (perfect hash function) provides O(1) lookups
/// with zero runtime initialization cost and no heap allocations.
static RESERVED_KEYWORDS: phf::Set<&'static str> = phf_set! {
    "as",
    "from",
    "to",
    "select",
    "array",
    "sum",
    "all",
    "allocate",
    "alter",
    "table",
    "blob",
};

/// Compare an existing database schema with an expected Arrow schema
///
/// Returns a SchemaDiff indicating what has changed
fn compare_schemas<'a>(
    existing: &ExistingSchema,
    expected: &'a ArrowSchema,
) -> Result<SchemaDiff<'a>, BoxError> {
    let mut new_columns = Vec::new();
    let mut type_mismatches = Vec::new();

    // Check each expected column
    for field in &expected.fields {
        if let Some(existing_col) = existing.columns.get(&field.name) {
            // Column exists - check if type matches
            let expected_pg_type = arrow_type_to_postgres_type(field.type_.as_arrow())?;

            // Normalize type names for comparison
            let normalized_existing = normalize_pg_type(&existing_col.data_type);
            let normalized_expected = normalize_pg_type(&expected_pg_type);

            if normalized_existing != normalized_expected {
                type_mismatches.push(format!(
                    "Column '{}': expected type '{}' but found '{}' in database",
                    field.name, expected_pg_type, existing_col.data_type
                ));
            }

            // Note: We don't fail on nullable mismatches, just log a warning
            // This is handled in migrate_table_schema
        } else {
            // Column doesn't exist - needs to be added
            new_columns.push(field);
        }
    }

    // Check for dropped columns
    let expected_column_names: std::collections::HashSet<_> =
        expected.fields.iter().map(|f| &f.name).collect();
    let dropped_columns: Vec<String> = existing
        .columns
        .keys()
        .filter(|col_name| !expected_column_names.contains(col_name))
        .cloned()
        .collect();

    Ok(SchemaDiff {
        new_columns,
        dropped_columns,
        type_mismatches,
    })
}

/// Normalize PostgreSQL type names for comparison
///
/// PostgreSQL's information_schema may return types in different formats than we generate.
/// For example: "numeric(20,0)" vs "numeric(20, 0)" or "character varying" vs "text"
/// Also handles cases where precision/scale are omitted (e.g., "numeric" vs "numeric(20)")
fn normalize_pg_type(pg_type: &str) -> String {
    let mut normalized = pg_type
        .to_lowercase()
        .replace(" ", "")
        .replace("charactervarying", "text")
        .replace("doubleprecision", "float8")
        .replace("timestampwithouttimezone", "timestamp")
        .replace("timestampwithtimezone", "timestamptz");

    // Handle numeric types: strip precision/scale for comparison
    // "numeric(20)" or "numeric(38,18)" becomes "numeric"
    if normalized.starts_with("numeric(") {
        normalized = "numeric".to_string();
    }

    // Handle bigint vs numeric(20) - they're functionally equivalent for our purposes
    if normalized == "bigint" || normalized == "numeric" {
        normalized = "numeric".to_string();
    }

    normalized
}

/// Convert Arrow schema to PostgreSQL CREATE TABLE statement
///
/// Deduplicates fields by name - if multiple fields have the same name,
/// only the first occurrence is used. This handles cases where manifests
/// contain duplicate field definitions.
///
/// If a `block_num` column exists in the schema, it will be set as the PRIMARY KEY.
pub fn arrow_schema_to_postgres_ddl(
    table_name: &str,
    schema: &ArrowSchema,
) -> Result<String, BoxError> {
    let mut columns = Vec::new();
    let mut seen_names = std::collections::HashSet::new();
    let mut has_block_num = false;

    for field in &schema.fields {
        // Skip duplicate field names
        if !seen_names.insert(field.name.clone()) {
            continue;
        }

        // Check if this is the block_num column
        if field.name == "block_num" {
            has_block_num = true;
        }

        let column_def = arrow_field_to_postgres_column(field)?;
        columns.push(column_def);
    }

    // Add PRIMARY KEY constraint if block_num exists
    let constraints = if has_block_num {
        ",\n  PRIMARY KEY (block_num)"
    } else {
        ""
    };

    let ddl = format!(
        "CREATE TABLE IF NOT EXISTS {} (\n  {}{}\n)",
        table_name,
        columns.join(",\n  "),
        constraints
    );

    Ok(ddl)
}

/// Convert a single Arrow field to PostgreSQL column definition
fn arrow_field_to_postgres_column(field: &Field) -> Result<String, BoxError> {
    let pg_type = arrow_type_to_postgres_type(field.type_.as_arrow())?;
    let nullable = if field.nullable { "" } else { " NOT NULL" };

    // wrap reserved keywords in quotes
    let column_name = if RESERVED_KEYWORDS.contains(field.name.as_str()) {
        format!("\"{}\"", field.name)
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

    /// Create a database table based on Arrow schema, with schema evolution support
    ///
    /// This is the main entry point for table creation/migration. It handles:
    /// - Creating new tables from scratch
    /// - Migrating existing tables when schema changes
    pub async fn create_table_from_schema(
        &self,
        table_name: &str,
        schema: &ArrowSchema,
    ) -> Result<(), BoxError> {
        // Check if table exists
        let table_exists = self.table_exists(table_name).await?;

        if !table_exists {
            // Table doesn't exist, create it
            self.create_table(table_name, schema).await?;
        } else {
            // Table exists, check for schema evolution
            tracing::debug!(
                "Table '{}' already exists, checking for schema changes",
                table_name
            );
            self.migrate_table_schema(table_name, schema).await?;
        }

        Ok(())
    }

    /// Initialize the checkpoint tracking table
    ///
    /// Creates a table to track the last successfully processed block_num for each table.
    /// This enables resuming from the last checkpoint on restart.
    pub async fn init_checkpoint_table(&self) -> Result<(), BoxError> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS _ampsync_checkpoints (
                table_name TEXT PRIMARY KEY,
                max_block_num BIGINT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )",
        )
        .execute(&self.pool)
        .await
        .map_err(|e| format!("Failed to create checkpoint table: {}", e))?;

        Ok(())
    }

    /// Get the checkpoint (last processed block_num) for a table
    ///
    /// Returns None if no checkpoint exists (first run)
    pub async fn get_checkpoint(&self, table_name: &str) -> Result<Option<i64>, BoxError> {
        let result: Option<(i64,)> =
            sqlx::query_as("SELECT max_block_num FROM _ampsync_checkpoints WHERE table_name = $1")
                .bind(table_name)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| {
                    format!("Failed to get checkpoint for table '{}': {}", table_name, e)
                })?;

        Ok(result.map(|(block_num,)| block_num))
    }

    /// Update the checkpoint for a table
    ///
    /// This should be called after successfully inserting a batch.
    /// Uses UPSERT (INSERT ... ON CONFLICT) for atomic updates.
    pub async fn update_checkpoint(
        &self,
        table_name: &str,
        max_block_num: i64,
    ) -> Result<(), BoxError> {
        sqlx::query(
            "INSERT INTO _ampsync_checkpoints (table_name, max_block_num, updated_at)
             VALUES ($1, $2, NOW())
             ON CONFLICT (table_name)
             DO UPDATE SET max_block_num = EXCLUDED.max_block_num, updated_at = NOW()",
        )
        .bind(table_name)
        .bind(max_block_num)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            format!(
                "Failed to update checkpoint for table '{}' to {}: {}",
                table_name, max_block_num, e
            )
        })?;

        Ok(())
    }

    /// Extract the maximum block_num from a RecordBatch
    ///
    /// Returns None if:
    /// - The batch has no block_num column
    /// - The batch is empty
    /// - All block_num values are NULL
    pub fn extract_max_block_num(batch: &RecordBatch) -> Option<i64> {
        use arrow_array::Array;

        // Find the block_num column
        let schema = batch.schema();
        let block_num_idx = schema
            .fields()
            .iter()
            .position(|f| f.name() == "block_num")?;

        // Get the column data
        let column = batch.column(block_num_idx);

        // Handle UInt64 type (most common for block_num)
        if let Some(uint64_array) = column.as_any().downcast_ref::<arrow_array::UInt64Array>() {
            let mut max_val: Option<u64> = None;
            for i in 0..uint64_array.len() {
                if !uint64_array.is_null(i) {
                    let val = uint64_array.value(i);
                    max_val = Some(max_val.map_or(val, |m| m.max(val)));
                }
            }
            return max_val.map(|v| v as i64);
        }

        // Handle Int64 type
        if let Some(int64_array) = column.as_any().downcast_ref::<arrow_array::Int64Array>() {
            let mut max_val: Option<i64> = None;
            for i in 0..int64_array.len() {
                if !int64_array.is_null(i) {
                    let val = int64_array.value(i);
                    max_val = Some(max_val.map_or(val, |m| m.max(val)));
                }
            }
            return max_val;
        }

        None
    }

    /// Check if a table exists in the database
    async fn table_exists(&self, table_name: &str) -> Result<bool, BoxError> {
        let result: Option<(bool,)> = sqlx::query_as(
            "SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = $1
            )",
        )
        .bind(table_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| format!("Failed to check if table '{}' exists: {}", table_name, e))?;

        Ok(result.map(|(exists,)| exists).unwrap_or(false))
    }

    /// Create a new table with the given schema
    async fn create_table(&self, table_name: &str, schema: &ArrowSchema) -> Result<(), BoxError> {
        let ddl = arrow_schema_to_postgres_ddl(table_name, schema)?;

        tracing::info!("Creating new table '{}' with DDL: {}", table_name, ddl);

        let pool = &self.pool;
        let ddl_query = &ddl;

        (|| async move { sqlx::query(ddl_query).execute(pool).await })
            .retry(Self::db_retry_policy())
            .when(Self::is_retryable_db_error)
            .notify(Self::notify_db_retry)
            .await
            .map_err(|e| format!("Failed to create table '{}': {}", table_name, e))?;

        tracing::info!("Successfully created table '{}'", table_name);
        Ok(())
    }

    /// Migrate an existing table's schema to match the expected schema
    ///
    /// Supports:
    /// - Adding new columns (safe, applied automatically)
    ///
    /// Will error on:
    /// - Type changes (requires manual intervention)
    /// - Dropped columns (requires manual intervention)
    async fn migrate_table_schema(
        &self,
        table_name: &str,
        expected_schema: &ArrowSchema,
    ) -> Result<(), BoxError> {
        // Get existing schema from database
        let existing_schema = self.get_table_schema(table_name).await?;

        // Compare schemas and determine what needs to change
        let schema_diff = compare_schemas(&existing_schema, expected_schema)?;

        // Fail on incompatible changes
        if !schema_diff.type_mismatches.is_empty() {
            return Err(format!(
                "Schema migration error for table '{}': Type changes detected (unsupported):\n{}",
                table_name,
                schema_diff.type_mismatches.join("\n")
            )
            .into());
        }

        if !schema_diff.dropped_columns.is_empty() {
            return Err(format!(
                "Schema migration error for table '{}': Columns dropped from schema (unsupported): {}",
                table_name,
                schema_diff.dropped_columns.join(", ")
            )
            .into());
        }

        // Apply new columns
        if !schema_diff.new_columns.is_empty() {
            tracing::info!(
                "Detected {} new column(s) in table '{}': {}",
                schema_diff.new_columns.len(),
                table_name,
                schema_diff
                    .new_columns
                    .iter()
                    .map(|f| f.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            );

            for field in &schema_diff.new_columns {
                self.add_column(table_name, field).await?;
            }
        } else {
            tracing::debug!("No schema changes detected for table '{}'", table_name);
        }

        Ok(())
    }

    /// Get the existing schema of a table from the database
    async fn get_table_schema(&self, table_name: &str) -> Result<ExistingSchema, BoxError> {
        #[derive(sqlx::FromRow)]
        struct ColumnInfo {
            column_name: String,
            data_type: String,
            is_nullable: String,
        }

        let columns: Vec<ColumnInfo> = sqlx::query_as(
            "SELECT column_name, data_type, is_nullable
             FROM information_schema.columns
             WHERE table_name = $1
             ORDER BY ordinal_position",
        )
        .bind(table_name)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| format!("Failed to query schema for table '{}': {}", table_name, e))?;

        let mut schema = ExistingSchema {
            columns: std::collections::HashMap::new(),
        };

        for col in columns {
            let is_nullable = col.is_nullable == "YES";
            schema.columns.insert(
                col.column_name,
                ExistingColumn {
                    data_type: col.data_type,
                    is_nullable,
                },
            );
        }

        Ok(schema)
    }

    /// Add a new column to an existing table
    async fn add_column(&self, table_name: &str, field: &Field) -> Result<(), BoxError> {
        // When adding columns to existing tables, always make them nullable
        // even if the schema says NOT NULL. This is because:
        // 1. Existing rows will have NULL for the new column
        // 2. PostgreSQL doesn't allow adding NOT NULL columns to non-empty tables
        // 3. New data from the stream will populate these columns properly
        let pg_type = arrow_type_to_postgres_type(field.type_.as_arrow())?;
        let column_name = if RESERVED_KEYWORDS.contains(field.name.as_str()) {
            format!("\"{}\"", field.name)
        } else {
            field.name.clone()
        };

        // Always add as nullable - existing rows will be NULL, new rows will have values
        let column_def = format!("{} {}", column_name, pg_type);
        let alter_stmt = format!("ALTER TABLE {} ADD COLUMN {}", table_name, column_def);

        tracing::info!("Executing schema migration: {}", alter_stmt);

        let pool = &self.pool;
        let stmt = &alter_stmt;

        (|| async move { sqlx::query(stmt).execute(pool).await })
            .retry(Self::db_retry_policy())
            .when(Self::is_retryable_db_error)
            .notify(Self::notify_db_retry)
            .await
            .map_err(|e| {
                format!(
                    "Failed to add column '{}' to table '{}': {}",
                    field.name, table_name, e
                )
            })?;

        tracing::info!(
            "Successfully added column '{}' to table '{}'",
            field.name,
            table_name
        );
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

    /// Check if a table has a primary key or unique constraint on block_num column
    async fn has_block_num_constraint(&self, table_name: &str) -> Result<bool, BoxError> {
        let result: Option<(bool,)> = sqlx::query_as(
            "SELECT EXISTS (
                SELECT 1 FROM pg_constraint c
                JOIN pg_class t ON c.conrelid = t.oid
                JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(c.conkey)
                WHERE t.relname = $1
                AND a.attname = 'block_num'
                AND c.contype IN ('p', 'u')
            )",
        )
        .bind(table_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            format!(
                "Failed to check block_num constraint for table '{}': {}",
                table_name, e
            )
        })?;

        Ok(result.map(|(exists,)| exists).unwrap_or(false))
    }

    /// Insert a single batch chunk using high-performance bulk copy with conflict handling
    ///
    /// Uses a temporary table approach to handle conflicts on block_num primary key:
    /// 1. COPY data into a temporary table (fast bulk load)
    /// 2. INSERT from temp table into main table with ON CONFLICT DO NOTHING
    /// 3. Drop temporary table
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

        // Check if schema has block_num column (primary key)
        let has_block_num = schema_fields
            .fields()
            .iter()
            .any(|f| f.name() == "block_num");

        // Check constraint ONCE before entering retry closure - critical for performance!
        // This query result is cached and reused across all retry attempts.
        let has_constraint = if has_block_num {
            self.has_block_num_constraint(table_name).await?
        } else {
            false
        };

        let pool = &self.pool;
        let buffer_data = &buffer.freeze();
        let num_rows = batch.num_rows();
        let columns_clause_ref = &columns_clause;

        // Use retry logic for the entire operation
        (|| async move {
            // Get a connection from the pool
            let mut conn = pool.acquire().await?;

            if has_constraint {
                // Use temporary table approach for conflict handling
                // Generate unique temp table name using process ID and timestamp
                let temp_table = format!("{}_tmp_{}", table_name, std::process::id());

                // Begin transaction to ensure temp table is properly scoped
                let mut tx = conn.begin().await?;

                // Create temporary table with same structure (without constraints)
                // Temp table is session-scoped and will be auto-dropped at end of session
                let create_temp = format!(
                    "CREATE TEMPORARY TABLE {} (LIKE {} INCLUDING DEFAULTS)",
                    temp_table, table_name
                );
                sqlx::query(&create_temp).execute(&mut *tx).await?;

                // COPY into temporary table
                let copy_query = format!(
                    "COPY {} ({}) FROM STDIN WITH (FORMAT BINARY)",
                    temp_table, columns_clause_ref
                );
                let mut copy_in = tx.copy_in_raw(&copy_query).await?;
                copy_in.send(buffer_data.clone()).await?;
                copy_in.finish().await?;

                // INSERT from temp table into main table with conflict handling
                let insert_query = format!(
                    "INSERT INTO {} ({}) SELECT {} FROM {} ON CONFLICT (block_num) DO NOTHING",
                    table_name, columns_clause_ref, columns_clause_ref, temp_table
                );
                let result = sqlx::query(&insert_query).execute(&mut *tx).await?;

                // Drop temporary table explicitly before committing
                let drop_temp = format!("DROP TABLE {}", temp_table);
                sqlx::query(&drop_temp).execute(&mut *tx).await?;

                // Commit the transaction
                tx.commit().await?;

                let rows_inserted = result.rows_affected();
                if rows_inserted < num_rows as u64 {
                    tracing::debug!(
                        "Inserted {} of {} rows into '{}' (skipped {} duplicates)",
                        rows_inserted,
                        num_rows,
                        table_name,
                        num_rows as u64 - rows_inserted
                    );
                }

                tracing::trace!(
                    "Bulk insert with conflict handling completed: {} rows inserted for {} input rows in table '{}'",
                    rows_inserted,
                    num_rows,
                    table_name
                );
            } else {
                // No constraint on block_num (or no block_num column) - use direct COPY
                let copy_query = format!(
                    "COPY {} ({}) FROM STDIN WITH (FORMAT BINARY)",
                    table_name, columns_clause_ref
                );
                let mut copy_in = conn.copy_in_raw(&copy_query).await?;
                copy_in.send(buffer_data.clone()).await?;
                let rows_affected = copy_in.finish().await?;

                tracing::trace!(
                    "COPY operation completed: {} rows affected for {} input rows in table '{}'",
                    rows_affected,
                    num_rows,
                    table_name
                );
            }

            Ok::<(), sqlx::Error>(())
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
        // Pre-allocate capacity to avoid reallocations: estimate ~50 chars per condition
        let estimated_capacity = invalidation_ranges.len() * 50;
        let mut where_clause = String::with_capacity(estimated_capacity);

        for (i, range) in invalidation_ranges.iter().enumerate() {
            if i > 0 {
                where_clause.push_str(" OR ");
            }
            use std::fmt::Write;
            write!(
                &mut where_clause,
                "(block_num >= {} AND block_num <= {})",
                range.numbers.start(),
                range.numbers.end()
            )
            .unwrap(); // Writing to String never fails
        }

        let delete_query = format!("DELETE FROM {} WHERE {}", table_name, where_clause);

        tracing::info!(
            "Handling reorg for table '{}' with {} invalidation ranges: {}",
            table_name,
            invalidation_ranges.len(),
            delete_query
        );

        let pool = &self.pool;
        let query = &delete_query;

        let result = (|| async move { sqlx::query(query).execute(pool).await })
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

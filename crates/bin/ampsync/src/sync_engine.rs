use std::{
    collections::VecDeque,
    ops::Deref,
    time::{Duration, Instant},
};

use arrow_to_postgres::ArrowToPostgresBinaryEncoder;
use backon::{ExponentialBuilder, Retryable};
use common::{BoxError, arrow::array::RecordBatch};
use datasets_derived::manifest::{ArrowSchema, Field};
use nozzle_client::InvalidationRange;
use phf::phf_set;
use sqlx::{Acquire, Pool, Postgres};

use crate::conn::DbConnPool;

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

/// Stream resumption strategy
///
/// Indicates how the stream should be resumed based on available checkpoint data.
#[derive(Debug, Clone)]
pub enum ResumePoint {
    /// Hash-verified watermark resumption (preferred)
    /// Pass this to SqlClient::query() for server-side resumption
    Watermark(common::metadata::segments::ResumeWatermark),

    /// Best-effort incremental checkpoint resumption
    /// Use WHERE block_num > max_block_num in the SQL query
    Incremental { network: String, max_block_num: i64 },

    /// No checkpoint exists - start from the beginning
    None,
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
            let arrow_field = arrow_schema::Field::new(
                field.name.clone(),
                field.type_.as_arrow().clone(),
                field.nullable,
            );
            let expected_pg_type = arrow_type_to_postgres_type(&arrow_field)?;

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
    // Note: Ignore system columns (prefixed with _) as they are auto-injected
    let expected_column_names: std::collections::HashSet<_> =
        expected.fields.iter().map(|f| &f.name).collect();
    let dropped_columns: Vec<String> = existing
        .columns
        .keys()
        .filter(|col_name| {
            // Ignore system columns like _block_num (auto-injected)
            !col_name.starts_with('_') && !expected_column_names.contains(col_name)
        })
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
/// Three system columns are ALWAYS injected:
/// - `_id` (BYTEA): Deterministic hash for PRIMARY KEY and deduplication
/// - `_block_num_start` (BIGINT): First block in batch range
/// - `_block_num_end` (BIGINT): Last block in batch range
///
/// PRIMARY KEY is always `_id` for consistency across all tables.
/// If user's schema includes `block_num`, it's preserved and indexed for query performance.
pub fn arrow_schema_to_postgres_ddl(
    table_name: &str,
    schema: &ArrowSchema,
) -> Result<String, BoxError> {
    let mut columns = Vec::new();
    let mut seen_names = std::collections::HashSet::new();

    // Always inject three system columns first
    // _id: deterministic xxh3_128 hash for PRIMARY KEY and deduplication
    //      - PRIMARY KEY constraint ensures hash collisions fail loudly (not silently)
    //      - Hash is computed from: row_content + block_range + row_index
    //      - Enables deduplication on reconnect and prevents duplicate inserts
    // _block_num_start/_block_num_end: block range for reorg handling
    //      - Conservative reorg: deletes entire batch if any block is invalidated
    columns.push("_id BYTEA NOT NULL".to_string());
    columns.push("_block_num_start BIGINT NOT NULL".to_string());
    columns.push("_block_num_end BIGINT NOT NULL".to_string());
    seen_names.insert("_id".to_string());
    seen_names.insert("_block_num_start".to_string());
    seen_names.insert("_block_num_end".to_string());

    // Add all fields from the schema
    for field in &schema.fields {
        // Skip duplicate field names
        if !seen_names.insert(field.name.clone()) {
            continue;
        }

        let column_def = arrow_field_to_postgres_column(field)?;
        columns.push(column_def);
    }

    // Build DDL with PRIMARY KEY on _id
    let ddl = format!(
        "CREATE TABLE IF NOT EXISTS {} (\n  {},\n  PRIMARY KEY (_id)\n)",
        table_name,
        columns.join(",\n  "),
    );

    // Note: Index on block_num (if present) is created separately in create_table_from_schema

    Ok(ddl)
}

/// Quote a column name if it's a SQL reserved keyword
///
/// This ensures column names like "to", "from", "select" work correctly in SQL statements.
pub fn quote_column_name(name: &str) -> String {
    if RESERVED_KEYWORDS.contains(name) {
        format!("\"{}\"", name)
    } else {
        name.to_string()
    }
}

/// Convert a single Arrow field to PostgreSQL column definition
fn arrow_field_to_postgres_column(field: &Field) -> Result<String, BoxError> {
    // Convert the datasets Field to an arrow_schema::Field
    let arrow_field = arrow_schema::Field::new(
        field.name.clone(),
        field.type_.as_arrow().clone(),
        field.nullable,
    );

    let pg_type = arrow_type_to_postgres_type(&arrow_field)?;
    let nullable = if field.nullable { "" } else { " NOT NULL" };

    // wrap reserved keywords in quotes
    let column_name = quote_column_name(&field.name);

    Ok(format!("{} {}{}", column_name, pg_type, nullable))
}

/// Map Arrow DataType to PostgreSQL type using arrow-to-postgres library
fn arrow_type_to_postgres_type(field: &arrow_schema::Field) -> Result<String, BoxError> {
    use arrow_to_postgres::encoders::{BuildEncoder, EncoderBuilder};

    // Create an encoder builder for this field to get the PostgreSQL type
    let encoder_builder =
        EncoderBuilder::try_new(std::sync::Arc::new(field.clone())).map_err(|e| {
            format!(
                "Failed to create encoder for field '{}': {:?}",
                field.name(),
                e
            )
        })?;

    // Get the PostgreSQL DDL string from the encoder's schema
    // The arrow-to-postgres library now handles all type-specific formatting:
    // - UInt64 → NUMERIC(20, 0)
    // - Decimal128(p, s) → NUMERIC(p, s)
    // - Timestamp(_, Some(tz)) → TIMESTAMPTZ
    Ok(encoder_builder.schema().data_type.to_ddl_string())
}

/// Optimizes batch sizes based on processing performance.
///
/// Uses atomics for lock-free reads in the hot path. Only locks when updating samples.
#[derive(Debug)]
pub struct AdaptiveBatchManager {
    /// Current target batch size (atomic for lock-free reads)
    current_batch_size: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    /// Minimum allowed batch size
    min_batch_size: usize,
    /// Maximum allowed batch size
    max_batch_size: usize,
    /// Target processing time per batch (milliseconds)
    target_duration_ms: u64,
    /// Recent performance samples (duration_ms, rows)
    recent_samples: std::sync::Arc<tokio::sync::Mutex<VecDeque<(u64, usize)>>>,
    /// Maximum samples to keep for averaging
    max_samples: usize,
    /// Consecutive error count (atomic)
    error_count: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    /// Target memory per batch (bytes)
    target_memory_bytes: usize,
}

impl Clone for AdaptiveBatchManager {
    fn clone(&self) -> Self {
        use std::sync::Arc;

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
        use std::sync::{Arc, atomic::AtomicUsize};

        Self {
            current_batch_size: Arc::new(AtomicUsize::new(1000)), // Start with 1K rows
            min_batch_size: 100,                                  // Minimum 100 rows
            max_batch_size: 50_000,                               // Maximum 50K rows
            target_duration_ms: 1000,                             // Target 1 second per batch
            recent_samples: Arc::new(tokio::sync::Mutex::new(VecDeque::with_capacity(10))),
            max_samples: 10,
            error_count: Arc::new(AtomicUsize::new(0)),
            target_memory_bytes: 50 * 1024 * 1024, // Target 50MB per batch
        }
    }
}

impl AdaptiveBatchManager {
    /// Records successful batch processing and adjusts batch size.
    pub async fn record_success(&self, duration: Duration, rows_processed: usize) {
        use std::sync::atomic::Ordering;

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
        use std::sync::atomic::Ordering;

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
        use std::sync::atomic::Ordering;

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
        use std::sync::atomic::Ordering;

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

#[derive(Clone)]
pub struct AmpsyncDbEngine {
    pool: Pool<Postgres>,
    batch_manager: AdaptiveBatchManager,
}

impl AmpsyncDbEngine {
    /// Column name used for reorg handling.
    /// All tables use the same system metadata column for tracking block ranges.
    const REORG_BLOCK_COLUMN: &'static str = "_block_num_end";

    pub fn new(pool: &DbConnPool) -> Self {
        Self {
            pool: pool.deref().clone(),
            batch_manager: AdaptiveBatchManager::default(),
        }
    }

    /// Returns retry policy for database operations.
    fn db_retry_policy() -> ExponentialBuilder {
        ExponentialBuilder::default()
            .with_min_delay(Duration::from_millis(50))
            .with_max_delay(Duration::from_secs(5))
            .with_max_times(50) // High limit, circuit breaker will stop us first
    }

    /// Maximum duration for database operation retries (60 seconds by default).
    ///
    /// Can be configured via `DB_OPERATION_MAX_RETRY_DURATION_SECS` environment variable.
    fn db_max_retry_duration() -> Duration {
        let secs = std::env::var("DB_OPERATION_MAX_RETRY_DURATION_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(60);
        Duration::from_secs(secs)
    }

    /// Returns true if the database error is retryable.
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

    /// Creates a circuit breaker-aware retryable condition.
    ///
    /// Wraps the normal retryable check with a time-based circuit breaker.
    fn create_retryable_with_circuit_breaker(
        max_duration: Duration,
    ) -> impl Fn(&sqlx::Error) -> bool {
        let start_time = Instant::now();
        move |err: &sqlx::Error| -> bool {
            let elapsed = start_time.elapsed();
            if elapsed >= max_duration {
                tracing::error!(
                    error = %err,
                    elapsed_secs = elapsed.as_secs(),
                    max_duration_secs = max_duration.as_secs(),
                    "db_operation_circuit_breaker_triggered"
                );
                return false; // Circuit breaker: stop retrying
            }
            Self::is_retryable_db_error(err)
        }
    }

    /// Logs database retry attempts.
    fn notify_db_retry(err: &sqlx::Error, dur: Duration) {
        tracing::warn!(
            error = %err,
            retry_delay_secs = dur.as_secs_f32(),
            "db_operation_retry"
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
                table = %table_name,
                "checking_schema_evolution"
            );
            self.migrate_table_schema(table_name, schema).await?;
        }

        Ok(())
    }

    /// Initialize the checkpoint tracking table
    ///
    /// Creates a table with hybrid checkpoint strategy:
    /// - Incremental checkpoints: Updated after each batch insertion (best-effort progress tracking)
    /// - Watermark checkpoints: Updated on Watermark events (hash-verified canonical resumption points)
    ///
    /// Schema supports multi-network scenarios with one row per (table, network) combination.
    pub async fn init_checkpoint_table(&self) -> Result<(), BoxError> {
        // First, check if old schema exists and migrate if needed
        let old_schema_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = '_ampsync_checkpoints'
                AND column_name = 'max_block_num'
                AND NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = '_ampsync_checkpoints'
                    AND column_name = 'network'
                )
            )",
        )
        .fetch_one(&self.pool)
        .await
        .unwrap_or(false);

        if old_schema_exists {
            // Migrate from old schema to new hybrid schema
            tracing::info!(
                "Migrating checkpoint table from old schema to hybrid watermark-based schema"
            );

            // Create new table with hybrid checkpoint support
            sqlx::query(
                "CREATE TABLE IF NOT EXISTS _ampsync_checkpoints_new (
                    table_name TEXT NOT NULL,
                    network TEXT NOT NULL,
                    -- Incremental checkpoint (best-effort, updated per batch)
                    incremental_block_num BIGINT,
                    incremental_updated_at TIMESTAMPTZ,
                    -- Watermark checkpoint (canonical, hash-verified, updated on Watermark event)
                    watermark_block_num BIGINT,
                    watermark_block_hash BYTEA,
                    watermark_updated_at TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (table_name, network)
                )",
            )
            .execute(&self.pool)
            .await
            .map_err(|e| format!("Failed to create new checkpoint table: {}", e))?;

            // Migrate old incremental checkpoints (preserve as best-effort resumption points)
            // We can't create watermarks without block hashes, but we can preserve progress
            tracing::info!("Migrating old checkpoint data to incremental checkpoints");
            sqlx::query(
                "INSERT INTO _ampsync_checkpoints_new
                    (table_name, network, incremental_block_num, incremental_updated_at, updated_at)
                 SELECT
                    table_name,
                    'unknown' as network,  -- Old schema didn't track network
                    max_block_num,
                    updated_at,
                    updated_at
                 FROM _ampsync_checkpoints",
            )
            .execute(&self.pool)
            .await
            .map_err(|e| format!("Failed to migrate old checkpoints: {}", e))?;

            // Backup old table instead of dropping
            sqlx::query("ALTER TABLE _ampsync_checkpoints RENAME TO _ampsync_checkpoints_backup")
                .execute(&self.pool)
                .await
                .map_err(|e| format!("Failed to backup old checkpoint table: {}", e))?;

            sqlx::query("ALTER TABLE _ampsync_checkpoints_new RENAME TO _ampsync_checkpoints")
                .execute(&self.pool)
                .await
                .map_err(|e| format!("Failed to rename new checkpoint table: {}", e))?;

            tracing::info!(
                "Checkpoint table migration completed. \
                 Old table backed up as _ampsync_checkpoints_backup. \
                 Incremental checkpoints preserved, watermarks will be established on first Watermark event."
            );
        } else {
            // Create new schema directly
            sqlx::query(
                "CREATE TABLE IF NOT EXISTS _ampsync_checkpoints (
                    table_name TEXT NOT NULL,
                    network TEXT NOT NULL,
                    -- Incremental checkpoint (best-effort, updated per batch)
                    incremental_block_num BIGINT,
                    incremental_updated_at TIMESTAMPTZ,
                    -- Watermark checkpoint (canonical, hash-verified, updated on Watermark event)
                    watermark_block_num BIGINT,
                    watermark_block_hash BYTEA,
                    watermark_updated_at TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (table_name, network)
                )",
            )
            .execute(&self.pool)
            .await
            .map_err(|e| format!("Failed to create checkpoint table: {}", e))?;
        }

        Ok(())
    }

    /// Load the watermark for a table to enable stream resumption
    ///
    /// Returns None if no watermark exists (first run or after migration).
    /// The returned ResumeWatermark can be passed to SqlClient::query() for hash-verified resumption.
    pub async fn load_watermark(
        &self,
        table_name: &str,
    ) -> Result<Option<common::metadata::segments::ResumeWatermark>, BoxError> {
        use std::collections::BTreeMap;

        use common::metadata::segments::ResumeWatermark;

        let rows: Vec<(String, i64, Vec<u8>)> = sqlx::query_as(
            "SELECT network, watermark_block_num, watermark_block_hash
             FROM _ampsync_checkpoints
             WHERE table_name = $1
             AND watermark_block_num IS NOT NULL
             AND watermark_block_hash IS NOT NULL
             ORDER BY network",
        )
        .bind(table_name)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| format!("Failed to load watermark for table '{}': {}", table_name, e))?;

        if rows.is_empty() {
            return Ok(None);
        }

        // Reconstruct ResumeWatermark from database rows
        let watermark_map: BTreeMap<String, (common::BlockNum, [u8; 32])> = rows
            .into_iter()
            .filter_map(|(network, block_num, hash_bytes)| {
                // Validate hash size to prevent panic
                if hash_bytes.len() != 32 {
                    tracing::warn!(
                        table = table_name,
                        network = %network,
                        hash_size = hash_bytes.len(),
                        "invalid_watermark_hash_size"
                    );
                    return None;
                }

                // Convert Vec<u8> to [u8; 32]
                let mut hash_array = [0u8; 32];
                hash_array.copy_from_slice(&hash_bytes);
                Some((network, (block_num as common::BlockNum, hash_array)))
            })
            .collect();

        if watermark_map.is_empty() {
            return Ok(None);
        }

        // Use From trait to convert to ResumeWatermark
        let watermark = ResumeWatermark::from(watermark_map);

        Ok(Some(watermark))
    }

    /// Update incremental checkpoint for a table
    ///
    /// This should be called after successfully inserting a batch (Batch variant).
    /// Provides best-effort progress tracking between watermark events.
    ///
    /// Uses UPSERT (INSERT ... ON CONFLICT) for atomic updates per network.
    pub async fn update_incremental_checkpoint(
        &self,
        table_name: &str,
        network: &str,
        max_block_num: i64,
    ) -> Result<(), BoxError> {
        sqlx::query(
            "INSERT INTO _ampsync_checkpoints
                (table_name, network, incremental_block_num, incremental_updated_at, updated_at)
             VALUES ($1, $2, $3, NOW(), NOW())
             ON CONFLICT (table_name, network)
             DO UPDATE SET
                incremental_block_num = GREATEST(COALESCE(_ampsync_checkpoints.incremental_block_num, 0), EXCLUDED.incremental_block_num),
                incremental_updated_at = NOW(),
                updated_at = NOW()",
        )
        .bind(table_name)
        .bind(network)
        .bind(max_block_num)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            format!(
                "Failed to update incremental checkpoint for table '{}', network '{}' to {}: {}",
                table_name, network, max_block_num, e
            )
        })?;

        Ok(())
    }

    /// Load the resume point for a table (watermark-first, fallback to incremental)
    ///
    /// Returns a ResumePoint indicating how the stream should be resumed:
    /// - Watermark: Hash-verified resumption point (preferred)
    /// - Incremental: Best-effort resumption from max block number
    /// - None: Start from the beginning
    pub async fn load_resume_point(&self, table_name: &str) -> Result<ResumePoint, BoxError> {
        // Try watermark first (hash-verified, canonical)
        if let Some(watermark) = self.load_watermark(table_name).await? {
            tracing::debug!(table = table_name, "loaded_watermark_resumption_point");
            return Ok(ResumePoint::Watermark(watermark));
        }

        // Fall back to incremental checkpoint (best-effort)
        let max_incremental: Option<(String, i64)> = sqlx::query_as(
            "SELECT network, MAX(incremental_block_num) as max_block
             FROM _ampsync_checkpoints
             WHERE table_name = $1
             AND incremental_block_num IS NOT NULL
             GROUP BY network
             ORDER BY max_block DESC
             LIMIT 1",
        )
        .bind(table_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            format!(
                "Failed to load incremental checkpoint for table '{}': {}",
                table_name, e
            )
        })?;

        if let Some((network, max_block)) = max_incremental {
            tracing::debug!(
                table = table_name,
                network = %network,
                max_block = max_block,
                "loaded_incremental_resumption_point"
            );
            return Ok(ResumePoint::Incremental {
                network,
                max_block_num: max_block,
            });
        }

        tracing::debug!(table = table_name, "no_resumption_point");
        Ok(ResumePoint::None)
    }

    /// Extract maximum block number from BlockRange metadata
    ///
    /// Used for incremental checkpoint updates in Batch variant.
    pub fn extract_max_block_from_ranges(
        ranges: &[common::metadata::segments::BlockRange],
    ) -> Option<(String, i64)> {
        ranges
            .iter()
            .max_by_key(|r| r.end())
            .map(|r| (r.network.clone(), r.end() as i64))
    }
    /// Save a watermark checkpoint for a table
    ///
    /// This should be called when receiving a ResponseBatchWithReorg::Watermark.
    /// Stores block number and hash for each network to enable hash-verified resumption.
    ///
    /// Uses UPSERT (INSERT ... ON CONFLICT) for atomic updates per network.
    pub async fn save_watermark(
        &self,
        table_name: &str,
        watermark: &common::metadata::segments::ResumeWatermark,
    ) -> Result<(), BoxError> {
        use std::collections::BTreeMap;

        // Convert ResumeWatermark to BTreeMap<String, (BlockNum, [u8; 32])>
        let watermark_map: BTreeMap<String, (common::BlockNum, [u8; 32])> =
            watermark.clone().into();

        // Start a transaction for atomic multi-row upsert
        let mut tx = self.pool.begin().await.map_err(|e| {
            format!(
                "Failed to start transaction for watermark save on table '{}': {}",
                table_name, e
            )
        })?;

        for (network, (block_num, block_hash)) in watermark_map {
            sqlx::query(
                "INSERT INTO _ampsync_checkpoints
                    (table_name, network, watermark_block_num, watermark_block_hash, watermark_updated_at, updated_at)
                 VALUES ($1, $2, $3, $4, NOW(), NOW())
                 ON CONFLICT (table_name, network)
                 DO UPDATE SET
                    watermark_block_num = EXCLUDED.watermark_block_num,
                    watermark_block_hash = EXCLUDED.watermark_block_hash,
                    watermark_updated_at = NOW(),
                    updated_at = NOW()",
            )
            .bind(table_name)
            .bind(&network)
            .bind(block_num as i64)
            .bind(&block_hash[..]) // Convert [u8; 32] to &[u8]
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                format!(
                    "Failed to save watermark for table '{}', network '{}': {}",
                    table_name, network, e
                )
            })?;
        }

        tx.commit().await.map_err(|e| {
            format!(
                "Failed to commit watermark transaction for table '{}': {}",
                table_name, e
            )
        })?;

        Ok(())
    }

    /// Returns true if the table exists.
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

    /// Creates a new table with the given schema.
    async fn create_table(&self, table_name: &str, schema: &ArrowSchema) -> Result<(), BoxError> {
        let ddl = arrow_schema_to_postgres_ddl(table_name, schema)?;

        tracing::info!(
            table = %table_name,
            field_count = schema.fields.len(),
            ddl = %ddl,
            "creating_table"
        );

        let pool = &self.pool;
        let ddl_query = &ddl;

        (|| async move { sqlx::query(ddl_query).execute(pool).await })
            .retry(Self::db_retry_policy())
            .when(Self::create_retryable_with_circuit_breaker(
                Self::db_max_retry_duration(),
            ))
            .notify(Self::notify_db_retry)
            .await
            .map_err(|e| format!("Failed to create table '{}': {}", table_name, e))?;

        // Create index on block_num if it exists in user's schema (for query performance)
        let has_block_num = schema.fields.iter().any(|f| f.name == "block_num");
        if has_block_num {
            let index_name = format!("{}_block_num_idx", table_name);
            let create_index_sql = format!(
                "CREATE INDEX IF NOT EXISTS {} ON {} (block_num)",
                index_name, table_name
            );

            tracing::info!(
                table = %table_name,
                index = %index_name,
                "creating_block_num_index"
            );

            let create_index_query = &create_index_sql;
            (|| async move { sqlx::query(create_index_query).execute(pool).await })
                .retry(Self::db_retry_policy())
                .when(Self::create_retryable_with_circuit_breaker(
                    Self::db_max_retry_duration(),
                ))
                .notify(Self::notify_db_retry)
                .await
                .map_err(|e| format!("Failed to create index on '{}': {}", table_name, e))?;

            tracing::info!(table = %table_name, index = %index_name, "index_created");
        }

        tracing::info!(table = %table_name, "table_created");
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
                table = %table_name,
                new_column_count = schema_diff.new_columns.len(),
                columns = %schema_diff
                    .new_columns
                    .iter()
                    .map(|f| f.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
                "schema_evolution_detected"
            );

            for field in &schema_diff.new_columns {
                self.add_column(table_name, field).await?;
            }
        } else {
            tracing::debug!(
                table = %table_name,
                "no_schema_changes"
            );
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
        let arrow_field = arrow_schema::Field::new(
            field.name.clone(),
            field.type_.as_arrow().clone(),
            field.nullable,
        );
        let pg_type = arrow_type_to_postgres_type(&arrow_field)?;
        let column_name = if RESERVED_KEYWORDS.contains(field.name.as_str()) {
            format!("\"{}\"", field.name)
        } else {
            field.name.clone()
        };

        // Always add as nullable - existing rows will be NULL, new rows will have values
        let column_def = format!("{} {}", column_name, pg_type);
        let alter_stmt = format!("ALTER TABLE {} ADD COLUMN {}", table_name, column_def);

        tracing::info!(
            table = %table_name,
            column = %field.name,
            pg_type = %pg_type,
            "adding_column"
        );

        let pool = &self.pool;
        let stmt = &alter_stmt;

        (|| async move { sqlx::query(stmt).execute(pool).await })
            .retry(Self::db_retry_policy())
            .when(Self::create_retryable_with_circuit_breaker(
                Self::db_max_retry_duration(),
            ))
            .notify(Self::notify_db_retry)
            .await
            .map_err(|e| {
                format!(
                    "Failed to add column '{}' to table '{}': {}",
                    field.name, table_name, e
                )
            })?;

        tracing::info!(
            table = %table_name,
            column = %field.name,
            "column_added"
        );
        Ok(())
    }

    /// Inserts RecordBatch into PostgreSQL using adaptive batch sizing.
    pub async fn insert_record_batch(
        &self,
        table_name: &str,
        batch: &RecordBatch,
    ) -> Result<(), BoxError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let total_rows = batch.num_rows();
        let optimal_batch_size = self.batch_manager.get_optimal_batch_size(batch);

        tracing::debug!(
            table = %table_name,
            total_rows = total_rows,
            batch_size = optimal_batch_size,
            "processing_batch_chunks"
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
                    self.batch_manager
                        .record_success(chunk_duration, chunk_size)
                        .await;

                    tracing::debug!(
                        table = %table_name,
                        start_row = start_row,
                        end_row = end_row,
                        rows = chunk_size,
                        duration_ms = chunk_duration.as_millis(),
                        "chunk_processed"
                    );
                }
                Err(e) => {
                    self.batch_manager.record_error();

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
            table = %table_name,
            total_rows = total_rows,
            "batch_processing_complete"
        );

        Ok(())
    }

    /// Helper to insert data using the temporary table approach for deduplication.
    ///
    /// All tables have `_id` as PRIMARY KEY for deduplication, so this approach is always used.
    /// Steps:
    /// 1. Create temporary table with same structure (no constraints)
    /// 2. COPY data into temp table (fast bulk load)
    /// 3. INSERT from temp table into main table with ON CONFLICT (_id) DO NOTHING
    /// 4. Drop temp table
    ///
    /// Returns the number of rows inserted (excludes duplicates).
    async fn insert_via_temp_table(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        table_name: &str,
        columns_clause: &str,
        conflict_column: &str,
        buffer_data: &bytes::Bytes,
        num_rows: usize,
    ) -> Result<u64, sqlx::Error> {
        // Generate unique temp table name using process ID
        let temp_table = format!("{}_tmp_{}", table_name, std::process::id());

        // Create temporary table with same structure (without constraints)
        // Temp table is session-scoped and will be auto-dropped at end of session
        let create_temp = format!(
            "CREATE TEMPORARY TABLE {} (LIKE {} INCLUDING DEFAULTS)",
            temp_table, table_name
        );
        sqlx::query(&create_temp).execute(&mut **tx).await?;

        // COPY into temporary table
        let copy_query = format!(
            "COPY {} ({}) FROM STDIN WITH (FORMAT BINARY)",
            temp_table, columns_clause
        );
        let mut copy_in = tx.copy_in_raw(&copy_query).await?;
        copy_in.send(buffer_data.clone()).await?;
        copy_in.finish().await?;

        // INSERT from temp table into main table with conflict handling
        let insert_query = format!(
            "INSERT INTO {} ({}) SELECT {} FROM {} ON CONFLICT ({}) DO NOTHING",
            table_name, columns_clause, columns_clause, temp_table, conflict_column
        );
        let result = sqlx::query(&insert_query).execute(&mut **tx).await?;

        // Drop temporary table explicitly before committing
        let drop_temp = format!("DROP TABLE {}", temp_table);
        sqlx::query(&drop_temp).execute(&mut **tx).await?;

        let rows_inserted = result.rows_affected();

        // Log if some rows were skipped due to duplicates
        if rows_inserted < num_rows as u64 {
            tracing::debug!(
                table = %table_name,
                rows_inserted = rows_inserted,
                input_rows = num_rows,
                duplicates_skipped = num_rows as u64 - rows_inserted,
                "batch_inserted_with_duplicates"
            );
        }

        tracing::trace!(
            table = %table_name,
            rows_inserted = rows_inserted,
            input_rows = num_rows,
            "bulk_insert_complete"
        );

        Ok(rows_inserted)
    }

    /// Insert a single batch chunk using high-performance bulk copy with deduplication
    ///
    /// Uses a temporary table approach to handle conflicts on the `_id` PRIMARY KEY:
    /// 1. COPY data into a temporary table (fast bulk load)
    /// 2. INSERT from temp table into main table with ON CONFLICT (_id) DO NOTHING
    /// 3. Drop temporary table
    ///
    /// This ensures deduplication on reconnect - if the same batch is re-inserted, duplicates are silently ignored.
    async fn insert_batch_chunk(
        &self,
        table_name: &str,
        batch: &RecordBatch,
    ) -> Result<(), BoxError> {
        // Encode to PostgreSQL COPY binary format
        let encoder = ArrowToPostgresBinaryEncoder::try_new(batch.schema().as_ref())
            .map_err(|e| format!("Failed to create arrow_to_pg encoder: {:?}", e))?;

        let (buffer, _finished) = encoder
            .encode_batch(batch)
            .map_err(|e| format!("Failed to encode batch to Postgres binary format: {:?}", e))?;

        // Get column names for the COPY command
        // IMPORTANT: Wrap reserved keywords in quotes (e.g., "to", "from")
        let schema_fields = batch.schema();
        let column_names: Vec<String> = schema_fields
            .fields()
            .iter()
            .map(|f| quote_column_name(f.name()))
            .collect();
        let columns_clause = column_names.join(", ");

        let pool = &self.pool;
        let buffer_data = &buffer.freeze();
        let num_rows = batch.num_rows();
        let columns_clause_ref = &columns_clause;

        // Use retry logic for the entire operation
        (|| async move {
            // Get a connection from the pool and start a transaction
            let mut conn = pool.acquire().await?;
            let mut tx = conn.begin().await?;

            // Always use temp table approach for deduplication on _id PRIMARY KEY
            Self::insert_via_temp_table(
                &mut tx,
                table_name,
                columns_clause_ref,
                "_id", // PRIMARY KEY is always _id
                buffer_data,
                num_rows,
            )
            .await?;

            // Commit the transaction
            tx.commit().await?;

            Ok::<(), sqlx::Error>(())
        })
        .retry(Self::db_retry_policy())
        .when(Self::create_retryable_with_circuit_breaker(
            Self::db_max_retry_duration(),
        ))
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
                "({} >= {} AND {} <= {})",
                Self::REORG_BLOCK_COLUMN,
                range.numbers.start(),
                Self::REORG_BLOCK_COLUMN,
                range.numbers.end()
            )
            .unwrap(); // Writing to String never fails
        }

        let delete_query = format!("DELETE FROM {} WHERE {}", table_name, where_clause);

        tracing::info!(
            table = %table_name,
            invalidation_count = invalidation_ranges.len(),
            query = %delete_query,
            "handling_reorg"
        );

        let pool = &self.pool;
        let query = &delete_query;

        let result = (|| async move { sqlx::query(query).execute(pool).await })
            .retry(Self::db_retry_policy())
            .when(Self::create_retryable_with_circuit_breaker(
                Self::db_max_retry_duration(),
            ))
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
                table = %table_name,
                rows_deleted = rows_deleted,
                "reorg_rows_deleted"
            );
        } else {
            tracing::debug!(
                table = %table_name,
                "reorg_no_rows_deleted"
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
    fn test_arrow_type_mappings_comprehensive() {
        use arrow_schema::Field;
        use datafusion::arrow::datatypes::TimeUnit;

        assert_eq!(
            arrow_type_to_postgres_type(&Field::new("test", ArrowDataType::Utf8, false)).unwrap(),
            "TEXT"
        );
        assert_eq!(
            arrow_type_to_postgres_type(&Field::new("test", ArrowDataType::Boolean, false))
                .unwrap(),
            "BOOLEAN"
        );
        assert_eq!(
            arrow_type_to_postgres_type(&Field::new("test", ArrowDataType::Float64, false))
                .unwrap(),
            "DOUBLE PRECISION"
        );
        assert_eq!(
            arrow_type_to_postgres_type(&Field::new("test", ArrowDataType::Binary, false)).unwrap(),
            "BYTEA"
        );

        // Integer types
        assert_eq!(
            arrow_type_to_postgres_type(&Field::new("test", ArrowDataType::Int32, false)).unwrap(),
            "INTEGER"
        );
        assert_eq!(
            arrow_type_to_postgres_type(&Field::new("test", ArrowDataType::Int8, false)).unwrap(),
            "SMALLINT"
        );
        assert_eq!(
            arrow_type_to_postgres_type(&Field::new("test", ArrowDataType::Int16, false)).unwrap(),
            "SMALLINT"
        );
        assert_eq!(
            arrow_type_to_postgres_type(&Field::new("test", ArrowDataType::Int64, false)).unwrap(),
            "BIGINT"
        );

        // Unsigned integer types
        assert_eq!(
            arrow_type_to_postgres_type(&Field::new("test", ArrowDataType::UInt8, false)).unwrap(),
            "SMALLINT"
        );
        assert_eq!(
            arrow_type_to_postgres_type(&Field::new("test", ArrowDataType::UInt32, false)).unwrap(),
            "BIGINT"
        );
        assert_eq!(
            arrow_type_to_postgres_type(&Field::new("test", ArrowDataType::UInt64, false)).unwrap(),
            "NUMERIC(20, 0)"
        );

        // Float types
        assert_eq!(
            arrow_type_to_postgres_type(&Field::new("test", ArrowDataType::Float32, false))
                .unwrap(),
            "REAL"
        );

        // String types
        assert_eq!(
            arrow_type_to_postgres_type(&Field::new("test", ArrowDataType::LargeUtf8, false))
                .unwrap(),
            "TEXT"
        );

        // Binary types
        assert_eq!(
            arrow_type_to_postgres_type(&Field::new("test", ArrowDataType::LargeBinary, false))
                .unwrap(),
            "BYTEA"
        );
        assert_eq!(
            arrow_type_to_postgres_type(&Field::new(
                "test",
                ArrowDataType::FixedSizeBinary(32),
                false
            ))
            .unwrap(),
            "BYTEA"
        );

        // Date/Time types
        assert_eq!(
            arrow_type_to_postgres_type(&Field::new("test", ArrowDataType::Date32, false)).unwrap(),
            "DATE"
        );
        assert_eq!(
            arrow_type_to_postgres_type(&Field::new(
                "test",
                ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
                false
            ))
            .unwrap(),
            "TIMESTAMP"
        );
        assert_eq!(
            arrow_type_to_postgres_type(&Field::new(
                "test",
                ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
                false
            ))
            .unwrap(),
            "TIMESTAMPTZ"
        );

        // Decimal type
        assert_eq!(
            arrow_type_to_postgres_type(&Field::new(
                "test",
                ArrowDataType::Decimal128(38, 10),
                false
            ))
            .unwrap(),
            "NUMERIC(38, 10)"
        );

        // Complex types - arrow-to-postgres library maps List to PostgreSQL native arrays
        assert_eq!(
            arrow_type_to_postgres_type(&Field::new(
                "test",
                ArrowDataType::List(Arc::new(datafusion::arrow::datatypes::Field::new(
                    "item",
                    ArrowDataType::Int32,
                    true
                ))),
                false
            ))
            .unwrap(),
            "INTEGER[]"
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
        assert!(ddl.contains("id NUMERIC(20, 0) NOT NULL"));
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

    #[test]
    fn test_ddl_always_includes_id_primary_key() {
        // Verify that ALL tables get _id as PRIMARY KEY for deduplication
        let schema = ArrowSchema {
            fields: vec![Field {
                name: "value".to_string(),
                type_: DataType(ArrowDataType::Int64),
                nullable: false,
            }],
        };

        let ddl = arrow_schema_to_postgres_ddl("test_dedup", &schema).unwrap();

        // Verify _id column is injected
        assert!(
            ddl.contains("_id BYTEA NOT NULL"),
            "DDL must include _id column"
        );

        // Verify _id is the PRIMARY KEY
        assert!(
            ddl.contains("PRIMARY KEY (_id)"),
            "DDL must have PRIMARY KEY on _id for deduplication"
        );

        // Verify system columns for reorg handling
        assert!(ddl.contains("_block_num_start BIGINT NOT NULL"));
        assert!(ddl.contains("_block_num_end BIGINT NOT NULL"));
    }
}

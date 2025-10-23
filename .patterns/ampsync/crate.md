🔧 `ampsync` guidelines
=========================

This document serves as both human and AI agent context for understanding and contributing to the `ampsync` crate.
The goal is a focused synchronization service that streams Apache Arrow `RecordBatch` data from Nozzle into PostgreSQL
database tables.

## Context and Purpose

**🎯 PRIMARY PURPOSE: Real-Time Database Synchronization Service**

The `ampsync` crate has ONE responsibility: **stream dataset changes from a Nozzle server and synchronize them to a
PostgreSQL database in real-time**. This crate is strictly a streaming synchronization service that maintains continuous
connections, handles blockchain reorganizations, and ensures data consistency - nothing more.

**✅ What `ampsync` IS:**

- A real-time streaming synchronization service for Nozzle datasets
- A high-performance Arrow-to-PostgreSQL data pipeline using COPY protocol

**❌ What `ampsync` is NOT:**

- A batch ETL tool (it's a continuous streaming service)
- A standalone indexing service (requires Nozzle server for data)
- A database management system (requires external PostgreSQL instance)
- A data transformation engine (applies schema mappings only)
- A query interface (writes to database, doesn't serve queries)

**🎯 Single Responsibility Principle:**

This crate exists solely to **synchronize Nozzle dataset streams into PostgreSQL tables**. All data extraction is
delegated to Nozzle (via Arrow Flight), all storage is delegated to PostgreSQL, and all schema management follows the
Arrow schema from Nozzle Admin API.

**🏗️ Architectural Context:**

In Nozzle's distributed architecture, ampsync serves as the **PostgreSQL synchronization layer**:

- Connects to Nozzle Arrow Flight server for data streaming
- Fetches schemas from Nozzle Admin API for table setup
- Transforms Arrow RecordBatches to PostgreSQL binary format
- Maintains checkpoints for resumable streaming
- Handles blockchain reorgs automatically
- Supports automatic version updates via polling

This clear separation ensures that streaming concerns remain isolated while data extraction and storage are handled by
specialized systems.

## Technology Stack

**🚀 Core Technologies**

The `ampsync` crate is built on these foundational technologies:

- **Apache Arrow**: Columnar data format for high-performance data transfer
    - `RecordBatch` streaming from Nozzle Arrow Flight server
    - Zero-copy operations where possible
    - Type-safe schema handling

- **PostgreSQL**: Target database for synchronized data
    - Binary COPY protocol for maximum throughput (30-50 GB/s hash performance)
    - Transaction support for atomic operations
    - Connection pooling via sqlx

- **Tokio Async Runtime**: Asynchronous, concurrent processing
    - Per-table async tasks for parallel data processing
    - Semaphore-based backpressure control
    - Graceful shutdown with cancellation tokens

- **Arrow Flight gRPC**: Network protocol for Arrow data streaming
    - Streaming data transfer (not batch downloads)
    - Built-in compression support
    - Efficient binary protocol

**Key Dependencies:**

- `arrow` - Apache Arrow Rust implementation
- `sqlx` - Async PostgreSQL driver with connection pooling
- `tokio` - Async runtime and concurrency primitives
- `arrow-flight` - gRPC-based Arrow streaming protocol
- `reqwest` - HTTP client for Nozzle Admin API
- `xxhash-rust` - High-performance hashing for deduplication

All modules in this crate MUST leverage these technologies appropriately and follow their best practices.

## Implementation Guidelines

This guide documents the **MANDATORY** patterns and conventions for implementing features in the `ampsync` crate.
🚨 **EVERY MODULE MUST FOLLOW THESE PATTERNS** - they ensure data consistency, reliability, and maintainability
across all streaming operations.

## 📁 Crate Structure

The `ampsync` crate follows a modular architecture pattern:

```
ampsync/src/
├── main.rs                  # Entry point, orchestration, signal handling
├── lib.rs                   # Public library interface
├── config.rs                # Configuration management, env var parsing
├── version_polling.rs       # Version change detection, auto-update coordination
├── stream_manager.rs        # Stream task spawning, query construction, graceful shutdown
├── stream_task.rs           # Per-table streaming logic, retry loops
├── sync_engine.rs           # Database operations, schema management, checkpoints
├── conn.rs                  # PostgreSQL connection pooling, retry logic
├── manifest.rs              # Schema fetching, Admin API client
├── batch_utils.rs           # RecordBatch utilities, system metadata injection
└── lib.rs                   # Public API exports
```

**Module Ownership:**

- **Orchestration**: `main.rs` coordinates subsystems but contains minimal logic
- **Query Construction**: `stream_manager.rs` builds streaming queries from manifest
- **Data Flow**: `stream_task.rs` → `sync_engine.rs` → PostgreSQL
- **Schema Management**: `manifest.rs` → `sync_engine.rs` (DDL generation)
- **Checkpointing**: `sync_engine.rs` owns all checkpoint logic (hybrid strategy)

## 🛠️ Core Implementation Patterns

### 1. 📄 Module Organization Pattern

🚨 **MANDATORY**: Each module MUST have a clear, singular responsibility:

- **`main.rs`**: Entry point ONLY - orchestrates subsystems, handles signals
- **`stream_task.rs`**: Per-table streaming loop - handles batches, reorgs, watermarks
- **`sync_engine.rs`**: Database operations ONLY - inserts, checkpoints, schema DDL
- **`manifest.rs`**: Schema fetching from Admin API ONLY
- **`stream_manager.rs`**: Query construction and stream task spawning ONLY
- **`config.rs`**: Configuration parsing and validation ONLY

**Anti-Pattern Example (❌ DON'T DO THIS):**

```rust
// main.rs - TOO MUCH LOGIC
pub async fn main() {
    // ❌ Database operations embedded in main
    let pool = create_pool().await?;
    pool.execute("CREATE TABLE ...").await?;

    // ❌ Schema fetching logic in main
    let schema = fetch_schema_from_api().await?;

    // ❌ Batch processing logic in main
    for batch in stream {
        process_batch(batch).await?;
    }
}
```

**Correct Pattern (✅ DO THIS):**

```rust
// main.rs - ORCHESTRATION ONLY
pub async fn main() {
    let config = AmpsyncConfig::from_env()?;
    let db_engine = AmpsyncDbEngine::new(&config).await?;
    let manifest = fetch_manifest(&config).await?;

    spawn_stream_tasks(manifest, db_engine, shutdown_token).await?;
    wait_for_shutdown(shutdown_token).await;
}

// stream_task.rs - STREAMING LOGIC
impl StreamTask {
    pub async fn run(self) -> Result<()> {
        loop {
            match self.stream.next().await {
                Some(batch) => self.handle_batch(batch).await?,
                None => self.reconnect().await?,
            }
        }
    }
}

// sync_engine.rs - DATABASE LOGIC
impl AmpsyncDbEngine {
    pub async fn insert_record_batch(&self, batch: RecordBatch) -> Result<()> {
        // All database operations here
    }
}
```

### 2. 🔄 Data Consistency Patterns

#### 🚨 CRITICAL DATA FLOW REQUIREMENTS

🔥 **ABSOLUTELY MANDATORY**: All data operations MUST follow these non-negotiable rules:

**Never Lose Data:**

- If batch insert fails, stream MUST halt (not silently drop)
- All errors MUST be logged with full context
- Retry logic MUST use exponential backoff
- Failed operations MUST eventually fail loudly (circuit breakers)

**Checkpoint Strategy - Hybrid Approach:**

The crate uses a **dual checkpoint system** to balance reliability and efficiency:

**1. Incremental Checkpoints** (Best-Effort Progress Tracking)

```rust
// Updated after EVERY successful batch insertion
pub async fn update_incremental_checkpoint(
    &self,
    table_name: &str,
    network: &str,
    block_num: i64,
) -> Result<()> {
    // ✅ Uses GREATEST() to prevent regression
    // ✅ Non-critical - warns but doesn't halt stream
    // ✅ Purpose: Minimize reprocessing between watermarks
}
```

**2. Watermark Checkpoints** (Canonical, Hash-Verified)

```rust
// Updated on Watermark events (when ranges complete)
pub async fn save_watermark(
    &self,
    table_name: &str,
    watermarks: &ResumeWatermark,
) -> Result<()> {
    // ✅ Includes block hash for fork detection
    // ✅ Multi-network support (one checkpoint per network)
    // ✅ CRITICAL - halts stream on failure
    // ✅ Purpose: Canonical resumption point
}
```

**Resumption Strategy:**

```rust
pub enum ResumePoint {
    Watermark(ResumeWatermark),           // Preferred: hash-verified, server-side
    Incremental { network, max_block_num }, // Fallback: best-effort, client-side WHERE clause
    None,                                  // Start from beginning
}

// Decision logic (in load_resume_point):
// 1. Try watermark first (pass to query(), server handles resumption)
// 2. Fall back to incremental (add WHERE block_num > X to SQL)
// 3. Start from beginning if no checkpoints exist
```

**Why Hybrid Strategy?**

- **Without incremental**: Watermarks emitted infrequently → could reprocess 1000s of batches
- **Without watermark**: No fork detection, less efficient server-side resumption
- **With both**: Minimal reprocessing (< 1 batch) + cryptographic verification

**Idempotency:**

```rust
// ALL tables use hash-based PRIMARY KEY for deduplication
// Hash includes: row_content + block_range + row_index
pub async fn insert_record_batch(&self, batch: RecordBatch) -> Result<()> {
    // ✅ System metadata injected: _id, _block_num_start, _block_num_end
    // ✅ ON CONFLICT DO NOTHING via temp table approach
    // ✅ Safe to restart at any point
}
```

**Reorg Handling:**

```rust
pub async fn handle_reorg(&self, table_name: &str, reorg_block: i64) -> Result<()> {
    // ✅ ALWAYS uses _block_num_end for deletion
    // ✅ DELETE WHERE _block_num_end >= reorg_block
    // ✅ Conservative: may delete more than necessary, but safe
    // ✅ Atomic operation
}
```

### 3. 🎯 System Metadata Architecture

🚨 **CRITICAL**: ALL tables MUST use consistent 3-column system metadata, injected via
`batch_utils::inject_system_metadata()`:

**Injected System Columns (ALWAYS, NO EXCEPTIONS):**

```rust
/// System metadata columns injected into ALL batches
///
/// 1. `_id` (BYTEA NOT NULL) - PRIMARY KEY
///    - Deterministic hash: xxh3_128(row_content + block_range + row_index)
///    - Prevents duplicate inserts on reconnect
///    - Hash collisions fail loudly (PRIMARY KEY constraint)
///    - High-performance: 30-50 GB/s
///
/// 2. `_block_num_start` (BIGINT NOT NULL)
///    - First block number in batch range
///    - Used for batch boundary tracking
///
/// 3. `_block_num_end` (BIGINT NOT NULL)
///    - Last block number in batch range
///    - Used for reorg handling: DELETE WHERE _block_num_end >= reorg_block
///    - Conservative deletion ensures no missed invalidations
pub fn inject_system_metadata(
    batch: &RecordBatch,
    ranges: &[BlockRange],
) -> Result<RecordBatch> {
    // Implementation in batch_utils.rs
}
```

**User Schema Columns:**

- **If user's query includes `block_num`**: Preserved as separate column with INDEX
- **If user's query does NOT include `block_num`**: Only 3 system columns + user columns

**Example Table Structures:**

```sql
-- Table WITH block_num in user's query
CREATE TABLE blocks
(
    _id BYTEA               NOT NULL, -- System: PRIMARY KEY
    _block_num_start BIGINT NOT NULL, -- System: Batch start
    _block_num_end BIGINT   NOT NULL, -- System: Batch end
    block_num NUMERIC(20)   NOT NULL, -- User: From query
    timestamp TIMESTAMPTZ   NOT NULL, -- User: From query
    hash      BYTEA         NOT NULL, -- User: From query
    PRIMARY KEY (_id)
);
CREATE INDEX blocks_block_num_idx ON blocks (block_num);

-- Table WITHOUT block_num in user's query
CREATE TABLE transfers
(
    _id BYTEA                NOT NULL, -- System: PRIMARY KEY
    _block_num_start BIGINT  NOT NULL, -- System: Batch start
    _block_num_end BIGINT    NOT NULL, -- System: Batch end
    from_addr TEXT           NOT NULL, -- User: From query
    to_addr   TEXT           NOT NULL, -- User: From query
    value     NUMERIC(38, 0) NOT NULL, -- User: From query
    PRIMARY KEY (_id)
);
```

**Why This Design?**

- **Simplicity**: Single code path for ALL tables (no conditional branching)
- **Robustness**: Hash-based PRIMARY KEY prevents silent data loss
- **Performance**: xxh3_128 is extremely fast, zero allocation with reusable buffers

### 4. 🔄 Version Polling Pattern

🚨 **MANDATORY**: When `DATASET_VERSION` is NOT specified, implement automatic version detection:

```rust
/// Background task that polls for new dataset versions
///
/// - Polls admin-api VERSIONS endpoint every 5 seconds (configurable)
/// - Only fetches version number (efficient - does NOT fetch schema)
/// - Sends version changes to main loop via channel
/// - Skips missed ticks if polling falls behind (prevents backlog)
pub async fn version_poll_task(
    config: AmpsyncConfig,
    version_tx: mpsc::Sender<Version>,
    shutdown: CancellationToken,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(config.version_poll_interval_secs));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = interval.tick() => {
                // ✅ Only fetch version, not full schema
                match fetch_latest_version(&config).await {
                    Ok(new_version) if new_version != current_version => {
                        version_tx.send(new_version).await.ok();
                        current_version = new_version;
                    }
                    Err(err) => {
                        tracing::warn!(error=?err, "version_poll_failed");
                    }
                    _ => {} // No change
                }
            }
            _ = shutdown.cancelled() => break,
        }
    }
}
```

**Critical State Management:**

1. New version detected → Send to main loop via channel
2. Main loop cancels all stream tasks via `CancellationToken`
3. Await all tasks with timeout (graceful shutdown)
4. Fetch new manifest with explicit version
5. Apply schema migrations (add new columns if needed)
6. Spawn new stream tasks with updated manifest
7. **Database connection pool is REUSED across reloads**

**What Can Change:**

- Dataset version (automatic detection and reload)
- Table schemas (adding new columns only)
- SQL queries (regenerated from new schema)

**What Cannot Change:**

- Column type changes (rejected with error to prevent data corruption)
- Dropping columns (rejected with error to prevent data loss)
- Dataset name (requires restart)
- Database connection (requires restart)

### 5. 🎯 Concurrency and Backpressure

🚨 **MANDATORY**: Use semaphore-based backpressure to prevent OOM:

```rust
/// Concurrency control pattern
///
/// - MAX_CONCURRENT_BATCHES (default: 10) limits concurrent processing
/// - CROSS-TABLE backpressure (not per-table)
/// - Stream won't pull more data until permit available
pub async fn spawn_stream_tasks(
    manifest: Manifest,
    db_engine: Arc<AmpsyncDbEngine>,
    concurrency_limit: Arc<Semaphore>,
    shutdown: CancellationToken,
) -> Vec<JoinHandle<()>> {
    let mut tasks = vec![];

    for (table_name, query) in manifest.queries {
        let task = StreamTask {
            table_name,
            query,
            db_engine: db_engine.clone(),
            concurrency_limit: concurrency_limit.clone(),
            shutdown: shutdown.clone(),
        };

        tasks.push(tokio::spawn(async move {
            task.run().await
        }));
    }

    tasks
}
```

**Per-Table Async Tasks:**

```rust
impl StreamTask {
    /// Each table runs in its own tokio::spawn task
    /// - Independent streams, independent error handling
    /// - Own retry loop with exponential backoff
    /// - Acquires semaphore permit before processing batch
    pub async fn run(self) -> Result<()> {
        loop {
            match self.reconnect_and_stream().await {
                Ok(_) => {
                    tracing::info!(table=%self.table_name, "stream_ended");
                }
                Err(err) => {
                    tracing::error!(table=%self.table_name, error=?err, "stream_error");
                    self.exponential_backoff().await;
                }
            }
        }
    }

    async fn handle_batch(&self, batch: RecordBatch) -> Result<()> {
        // ✅ Acquire permit (blocks if at limit)
        let _permit = self.concurrency_limit.acquire().await?;

        // ✅ Process batch
        self.db_engine.insert_record_batch(&self.table_name, batch).await?;

        // ✅ Update incremental checkpoint
        let max_block = extract_max_block_from_ranges(&ranges);
        self.db_engine.update_incremental_checkpoint(
            &self.table_name,
            &network,
            max_block,
        ).await?;

        // Permit automatically released when dropped
        Ok(())
    }
}
```

### 6. 🛡️ Error Handling and Retry Logic

🚨 **MANDATORY**: All database operations MUST use exponential backoff retry:

```rust
/// Retry pattern with circuit breaker
///
/// - Exponential backoff: 100ms → 200ms → 400ms → ... → 30s max
/// - Circuit breaker: Stop after configured duration (prevents infinite loops)
/// - Detailed logging at each retry
pub async fn db_operation_with_retry<F, T>(
    operation: F,
    context: &str,
    max_duration: Duration,
) -> Result<T>
where
    F: Fn() -> BoxFuture<'static, Result<T>>,
{
    let start = Instant::now();
    let mut delay = Duration::from_millis(100);
    let max_delay = Duration::from_secs(30);

    loop {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(err) if is_retryable(&err) => {
                if start.elapsed() >= max_duration {
                    tracing::error!(
                        context=%context,
                        elapsed=?start.elapsed(),
                        "db_operation_circuit_breaker_triggered"
                    );
                    return Err(err);
                }

                tracing::warn!(
                    context=%context,
                    error=?err,
                    delay=?delay,
                    "retrying_db_operation"
                );

                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(max_delay);
            }
            Err(err) => {
                tracing::error!(context=%context, error=?err, "non_retryable_error");
                return Err(err);
            }
        }
    }
}
```

**Circuit Breakers:**

```rust
/// Connection Circuit Breaker (default: 300s)
/// Prevents indefinite connection retry loops
pub async fn connect_with_max_duration(
    database_url: &str,
    max_duration: Duration,
) -> Result<Pool<Postgres>> {
    db_operation_with_retry(
        || Box::pin(connect_once(database_url)),
        "database_connection",
        max_duration,
    ).await
}

/// Operation Circuit Breaker (default: 60s)
/// Prevents indefinite database operation retries
pub async fn insert_with_retry(
    &self,
    table_name: &str,
    batch: RecordBatch,
) -> Result<()> {
    db_operation_with_retry(
        || Box::pin(self.insert_once(table_name, batch.clone())),
        &format!("insert_batch_{}", table_name),
        Duration::from_secs(self.config.db_operation_max_retry_duration_secs),
    ).await
}
```

### 7. 📋 SQL Query Construction

🚨 **MANDATORY**: Stream from materialized dataset tables using simple SELECT * pattern:

```rust
/// Simple query construction for materialized datasets
///
/// - Dataset has already been materialized by ampd
/// - Stream from the materialized table (not raw data sources)
/// - Always use SELECT * (no custom SQL from manifest)
/// - Automatically quote identifiers to prevent SQL syntax errors
pub fn construct_streaming_query(
    dataset_name: &str,
    table_name: &str,
) -> String {
    // Simple pattern: SELECT * FROM "{dataset}"."{table}" SETTINGS stream = true
    format!(
        "SELECT * FROM \"{}\".\"{}\" SETTINGS stream = true",
        dataset_name, table_name
    )
}
```

**Key Design Decisions:**

1. **No SQL Validation Needed**: We use simple SELECT * queries (no complex SQL to validate)
2. **No Column Extraction**: SELECT * always includes all columns from manifest schema
3. **Identifier Quoting**: Always quote dataset/table names for safety
4. **Materialized Data**: Stream from already-processed data (ampd materializes, ampsync streams)

**Query Construction Examples:**

```rust
// Basic query
let query = construct_streaming_query("battleship", "game_created");
// Result: SELECT * FROM "battleship"."game_created" SETTINGS stream = true

// With incremental resumption (WHERE clause injection)
let query_with_where = format!(
    "SELECT * FROM \"{}\".\"{}\" WHERE _block_num_end > {} SETTINGS stream = true",
    dataset_name, table_name, max_block_num
);
// Note: Uses _block_num_end (system metadata column injected by ampsync)
```

**Why This Approach:**

- ✅ **Efficient**: Stream from materialized tables (no redundant computation)
- ✅ **Simple**: Single code path for all tables
- ✅ **Reliable**: No complex SQL parsing that can fail
- ✅ **Fast**: No validation overhead on startup
- ✅ **Maintainable**: Easy to understand and debug

### 8. 🔒 Schema Evolution Pattern

🚨 **MANDATORY**: Support adding columns, reject dropping columns:

```rust
/// Schema evolution handler
///
/// - Supports adding new columns (hot-reload)
/// - Rejects dropping columns (data loss prevention)
/// - Rejects type changes (data corruption prevention)
pub async fn migrate_schema(
    &self,
    table_name: &str,
    old_schema: &Schema,
    new_schema: &Schema,
) -> Result<()> {
    let old_fields: HashSet<_> = old_schema.fields().iter().map(|f| f.name()).collect();
    let new_fields: HashSet<_> = new_schema.fields().iter().map(|f| f.name()).collect();

    // ✅ Check for dropped columns
    let dropped: Vec<_> = old_fields.difference(&new_fields).collect();
    if !dropped.is_empty() {
        return Err(anyhow!(
            "Columns dropped from schema (unsupported): {:?}. \
             To remove columns: \
             1. Manually DROP COLUMN from PostgreSQL \
             2. Publish new dataset version \
             3. Restart ampsync",
            dropped
        ));
    }

    // ✅ Add new columns
    let added: Vec<_> = new_fields.difference(&old_fields).collect();
    for field_name in added {
        let field = new_schema.field_with_name(field_name)?;
        let pg_type = arrow_type_to_postgres_type(field.data_type())?;

        let sql = format!(
            "ALTER TABLE {} ADD COLUMN {} {}",
            table_name,
            quote_identifier(field_name),
            pg_type
        );

        self.pool.execute(&sql).await?;

        tracing::info!(
            table=%table_name,
            column=%field_name,
            pg_type=%pg_type,
            "column_added"
        );
    }

    // ✅ Verify type compatibility
    for field_name in old_fields.intersection(&new_fields) {
        let old_field = old_schema.field_with_name(field_name)?;
        let new_field = new_schema.field_with_name(field_name)?;

        if old_field.data_type() != new_field.data_type() {
            return Err(anyhow!(
                "Column type changed: {} ({:?} → {:?}). \
                 Type changes not supported to prevent data corruption.",
                field_name,
                old_field.data_type(),
                new_field.data_type()
            ));
        }
    }

    Ok(())
}
```

## 📚 Documentation Standards

### 1. 📝 Module Documentation

🚨 **MANDATORY**: Each module MUST include comprehensive documentation following this EXACT pattern:

```rust
//! Per-table streaming task implementation
//!
//! This module contains the core streaming logic for individual dataset tables.
//! Each table runs in its own async task with independent error handling and retry logic.
//!
//! ## Responsibilities
//! - Establish and maintain Arrow Flight streaming connection to Nozzle server
//! - Process incoming RecordBatches and write to PostgreSQL
//! - Handle blockchain reorganizations by deleting invalidated data
//! - Maintain incremental checkpoints for progress tracking
//! - Save watermark checkpoints for hash-verified resumption
//! - Implement exponential backoff retry on failures
//!
//! ## Error Handling
//! - Stream errors trigger reconnection with exponential backoff
//! - Batch insert failures halt the stream (data consistency)
//! - Checkpoint update failures are logged but don't halt stream (best-effort)
//! - Non-retryable errors are logged and propagated
//!
//! ## Concurrency
//! - Each table runs in independent `tokio::spawn` task
//! - Semaphore-based backpressure prevents OOM (MAX_CONCURRENT_BATCHES)
//! - Graceful shutdown via `CancellationToken`

use /* ... */;

pub struct StreamTask {
    // ...
}
```

### 2. 📊 Function Documentation

🚨 **MANDATORY**: All public functions MUST include comprehensive documentation:

```rust
/// Processes a single RecordBatch from the stream
///
/// This function performs the complete batch processing workflow:
/// 1. Acquires semaphore permit (backpressure control)
/// 2. Injects system metadata columns (_id, _block_num_start, _block_num_end)
/// 3. Converts nanosecond timestamps to microsecond (PostgreSQL compatibility)
/// 4. Inserts data using PostgreSQL COPY binary protocol
/// 5. Updates incremental checkpoint with max block number
///
/// ## Arguments
/// - `batch`: Arrow RecordBatch containing user data
/// - `ranges`: Block ranges covered by this batch (for system metadata)
/// - `network`: Network identifier for checkpoint tracking
///
/// ## Returns
/// - `Ok(())`: Batch successfully processed and checkpoint updated
/// - `Err(_)`: Database error, type conversion error, or checkpoint failure
///
/// ## Error Handling
/// - Batch insert failures: CRITICAL - halts stream, logs error
/// - Checkpoint update failures: NON-CRITICAL - warns but continues
/// - Uses exponential backoff retry via `db_operation_with_retry()`
///
/// ## Performance
/// - Uses PostgreSQL COPY binary protocol for maximum throughput
/// - Zero-copy Arrow operations where possible
/// - Reusable hash buffers eliminate allocations
///
/// ## Example
/// ```rust
/// let batch = stream.next().await?;
/// let ranges = vec![BlockRange { start: 100, end: 199 }];
/// task.handle_batch(batch, ranges, "ethereum").await?;
/// ```
pub async fn handle_batch(
    &self,
    batch: RecordBatch,
    ranges: Vec<BlockRange>,
    network: &str,
) -> Result<()> {
    // Implementation
}
```

### 3. 💥 Error Documentation

🚨 **MANDATORY**: Error types MUST include comprehensive documentation:

```rust
/// Errors that can occur during streaming operations
///
/// This enum represents all error conditions encountered during
/// the streaming, processing, and persistence of dataset batches.
#[derive(Debug, thiserror::Error)]
pub enum StreamError {
    /// Failed to establish connection to Nozzle Arrow Flight server
    ///
    /// This occurs when:
    /// - Nozzle server is not running or unreachable
    /// - Network connectivity issues
    /// - Invalid AMP_FLIGHT_ADDR configuration
    /// - TLS/authentication failures
    ///
    /// **Recovery**: Automatic retry with exponential backoff
    #[error("failed to connect to Nozzle server: {0}")]
    ConnectionFailed(#[from] arrow_flight::error::FlightError),

    /// Failed to insert RecordBatch into PostgreSQL
    ///
    /// This occurs when:
    /// - Database connection lost during insert
    /// - Type conversion errors (Arrow → PostgreSQL)
    /// - Constraint violations (should be rare with hash-based PRIMARY KEY)
    /// - Disk space exhausted
    ///
    /// **Recovery**: Retry with exponential backoff up to circuit breaker limit
    /// **Critical**: Stream halts on failure to prevent data loss
    #[error("failed to insert batch for table '{table}': {source}")]
    InsertFailed {
        table: String,
        #[source]
        source: sqlx::Error,
    },

    /// Failed to update checkpoint after successful batch insert
    ///
    /// This occurs when:
    /// - Database connection issues
    /// - _ampsync_checkpoints table corruption
    ///
    /// **Recovery**: Warns but does NOT halt stream (best-effort)
    /// **Impact**: May reprocess some batches on restart
    #[error("failed to update checkpoint for table '{table}': {source}")]
    CheckpointFailed {
        table: String,
        #[source]
        source: sqlx::Error,
    },

    // ... additional variants
}
```

## 🚨 Configuration Management

### 1. 📋 Environment Variable Patterns

🚨 **MANDATORY**: All configuration MUST come from environment variables:

```rust
/// Ampsync configuration loaded from environment variables
///
/// All fields have sensible defaults where possible. Required fields
/// will return clear error messages if not set.
#[derive(Debug, Clone)]
pub struct AmpsyncConfig {
    // REQUIRED
    /// Dataset name to sync (must match published dataset in Nozzle)
    pub dataset_name: String,

    // REQUIRED (one of two options)
    /// PostgreSQL connection URL
    /// Format: postgresql://[user]:[password]@[host]:[port]/[database]
    pub database_url: Option<String>,

    /// Individual database connection components (alternative to database_url)
    pub database_user: Option<String>,
    pub database_name: Option<String>,
    pub database_password: Option<String>,
    pub database_host: String, // default: "localhost"
    pub database_port: u16,    // default: 5432

    // OPTIONAL - Dataset Configuration
    /// Specific dataset version to sync (if None, uses latest and polls for updates)
    pub dataset_version: Option<String>,

    /// How often to check for new versions (only used when dataset_version is None)
    pub version_poll_interval_secs: u64, // default: 5

    // OPTIONAL - Nozzle Connection
    /// Nozzle Arrow Flight server address
    pub amp_flight_addr: String, // default: "http://localhost:1602"

    /// Nozzle Admin API server address
    pub amp_admin_api_addr: String, // default: "http://localhost:1610"

    // OPTIONAL - Performance & Reliability
    /// Maximum concurrent batch operations across all tables
    pub max_concurrent_batches: usize, // default: 10

    /// Database connection retry circuit breaker duration (seconds)
    pub db_max_retry_duration_secs: u64, // default: 300

    /// Database operation retry circuit breaker duration (seconds)
    pub db_operation_max_retry_duration_secs: u64, // default: 60
}

impl AmpsyncConfig {
    /// Load configuration from environment variables
    ///
    /// ## Required Environment Variables
    /// - `DATASET_NAME`: Dataset name
    /// - `DATABASE_URL` OR (`DATABASE_USER` + `DATABASE_NAME`)
    ///
    /// ## Optional Environment Variables
    /// - `DATASET_VERSION`: Pin to specific version (disables auto-update)
    /// - `VERSION_POLL_INTERVAL_SECS`: Version polling interval (default: 5)
    /// - `AMP_FLIGHT_ADDR`: Arrow Flight server (default: http://localhost:1602)
    /// - `AMP_ADMIN_API_ADDR`: Admin API server (default: http://localhost:1610)
    /// - `MAX_CONCURRENT_BATCHES`: Concurrency limit (default: 10)
    /// - `DB_MAX_RETRY_DURATION_SECS`: Connection circuit breaker (default: 300)
    /// - `DB_OPERATION_MAX_RETRY_DURATION_SECS`: Operation circuit breaker (default: 60)
    /// - `RUST_LOG`: Logging configuration (default: info)
    ///
    /// ## Errors
    /// Returns detailed error if required variables are missing or invalid
    pub fn from_env() -> Result<Self> {
        // Implementation
    }
}
```

### 2. 🔐 Security Patterns

🚨 **MANDATORY**: Password redaction for safe logging:

```rust
/// Sanitize database URL for safe logging (redact passwords)
///
/// Handles complex passwords with special characters including '@'.
/// Uses sophisticated parsing to find the LAST '@' (the host separator).
///
/// ## Examples
/// ```rust
/// let url = "postgresql://user:p@ssw0rd@localhost:5432/db";
/// assert_eq!(
///     sanitize_database_url(url),
///     "postgresql://user:***@localhost:5432/db"
/// );
/// ```
pub fn sanitize_database_url(url: &str) -> String {
    // Find scheme (postgresql://)
    let scheme_end = url.find("://").map(|i| i + 3).unwrap_or(0);

    // Find LAST '@' (the host separator, not password '@')
    if let Some(host_sep) = url.rfind('@') {
        if host_sep > scheme_end {
            // Find password separator ':'
            if let Some(pass_sep) = url[scheme_end..host_sep].find(':') {
                let pass_sep = scheme_end + pass_sep;
                let before = &url[..pass_sep + 1];
                let after = &url[host_sep..];
                return format!("{}***{}", before, after);
            }
        }
    }

    url.to_string() // No password found
}
```

## 🧪 Testing Standards

### 1. 📊 Integration Test Patterns

🚨 **MANDATORY**: All features MUST have integration tests:

```rust
/// Integration test using temporary PostgreSQL database
///
/// Tests the complete checkpoint workflow:
/// 1. Create table with system metadata columns
/// 2. Insert batches and update incremental checkpoints
/// 3. Save watermark checkpoint
/// 4. Load resume point and verify correct strategy selected
///
/// Uses `pgtemp` crate for isolated test database.
#[tokio::test]
async fn test_hybrid_checkpoint_strategy() -> Result<()> {
    // Setup temporary database
    let pg = pgtemp::PgTempDB::async_new().await;
    let config = AmpsyncConfig {
        database_url: Some(pg.connection_uri()),
        dataset_name: "test_dataset".to_string(),
        ..Default::default()
    };

    let db_engine = AmpsyncDbEngine::new(&config).await?;

    // Create test table
    let schema = Schema::new(vec![
        Field::new("value", DataType::Int64, false),
    ]);
    db_engine.create_table("test_table", &schema).await?;

    // Test incremental checkpoint
    db_engine.update_incremental_checkpoint("test_table", "ethereum", 100).await?;

    // Verify incremental resume point
    let resume = db_engine.load_resume_point("test_table").await?;
    assert!(matches!(resume, ResumePoint::Incremental { max_block_num: 100, .. }));

    // Test watermark checkpoint
    let watermark = ResumeWatermark {
        watermarks: vec![NetworkWatermark {
            network: "ethereum".to_string(),
            block_num: 200,
            block_hash: vec![0xab; 32],
        }],
    };
    db_engine.save_watermark("test_table", &watermark).await?;

    // Verify watermark resume point (preferred)
    let resume = db_engine.load_resume_point("test_table").await?;
    assert!(matches!(resume, ResumePoint::Watermark(_)));

    Ok(())
}
```

### 2. 🧪 HTTP Mocking Patterns

🚨 **MANDATORY**: Test version polling with HTTP mocks:

```rust
/// Test version polling with mockito HTTP server
///
/// Verifies that:
/// 1. Only versions endpoint is called (not manifest endpoint - efficient!)
/// 2. Version changes are detected correctly
/// 3. Error handling works (non-fatal, continues polling)
#[tokio::test]
async fn test_version_polling_efficiency() {
    let mut server = mockito::Server::new_async().await;

    // Mock versions endpoint
    let versions_mock = server
        .mock("GET", "/datasets/test_dataset/versions")
        .with_status(200)
        .with_body(r#"[
            {"version": "0.1.0", "qualified_version": "0.1.0-LTcyNjgzMjc1NA"},
            {"version": "0.2.0", "qualified_version": "0.2.0-LTg1NzM2Njk0Mg"}
        ]"#)
        .expect(2) // Called twice during test
        .create_async()
        .await;

    // Mock manifest endpoint (should NOT be called during polling)
    let manifest_mock = server
        .mock("GET", "/datasets/test_dataset/versions/0.2.0/manifest")
        .expect(0) // NEVER called during polling
        .create_async()
        .await;

    let config = AmpsyncConfig {
        dataset_name: "test_dataset".to_string(),
        dataset_version: None, // Enable polling
        version_poll_interval_secs: 1,
        amp_admin_api_addr: server.url(),
        ..Default::default()
    };

    let (version_tx, mut version_rx) = mpsc::channel(10);
    let shutdown = CancellationToken::new();

    // Spawn polling task
    let poll_handle = tokio::spawn(version_poll_task(
        config.clone(),
        version_tx,
        shutdown.clone(),
    ));

    // Wait for version detection
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify version was detected
    let new_version = tokio::time::timeout(
        Duration::from_secs(1),
        version_rx.recv()
    ).await.ok().flatten();

    assert_eq!(new_version, Some("0.2.0".to_string()));

    // Shutdown and verify
    shutdown.cancel();
    poll_handle.await.ok();

    versions_mock.assert_async().await; // Called as expected
    manifest_mock.assert_async().await;   // Never called (efficient!)
}
```

## 📋 Logging Patterns

### 1. 🏗️ Structured Logging

🚨 **MANDATORY**: Use structured logging throughout ALL modules:

```rust
// Success operations - info level
tracing::info!(
    table=%table_name,
    rows=batch.num_rows(),
    duration_ms=elapsed.as_millis(),
    "batch_inserted"
);

// Expected errors - debug/warn level
tracing::debug!(
    table=%table_name,
    error=?err,
    "stream_ended_reconnecting"
);

// Critical errors - error level
tracing::error!(
    table=%table_name,
    error=?err,
    context="batch_insert",
    "CRITICAL: data_loss_risk"
);

// Performance metrics
tracing::debug!(
    table=%table_name,
    old_size=old_batch_size,
    new_size=new_batch_size,
    reason="processing_time",
    "adaptive_batch_size_adjusted"
);

// Circuit breakers
tracing::error!(
    operation=%operation_name,
    elapsed=?start.elapsed(),
    max_duration=?max_duration,
    "db_connection_circuit_breaker_triggered"
);
```

### 2. 🔍 Context Information

🚨 **MANDATORY**: Include relevant context in ALL log messages:

- Table name
- Operation type
- Block numbers
- Error details
- Performance metrics (duration, row count, batch size)
- Retry attempts and delays

## 🔄 Migration and Compatibility

🚨 **MANDATORY**: Support backward-compatible schema migrations:

```rust
/// Migrate from old checkpoint schema to new hybrid checkpoint schema
///
/// Old schema: Single `max_block_num` column
/// New schema: Separate `incremental_block_num` and `watermark_*` columns
///
/// Migration strategy:
/// 1. Detect old schema (missing watermark_block_num column)
/// 2. Backup old table as _ampsync_checkpoints_backup
/// 3. Create new table with hybrid schema
/// 4. Migrate old checkpoints to incremental_block_num
/// 5. Network defaults to "unknown" for migrated checkpoints
/// 6. Watermarks will be established on first Watermark event
pub async fn migrate_checkpoint_schema(&self) -> Result<()> {
    // Check if migration needed
    let has_watermark_col: bool = sqlx::query_scalar(
        "SELECT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = '_ampsync_checkpoints'
            AND column_name = 'watermark_block_num'
        )"
    )
        .fetch_one(&self.pool)
        .await?;

    if has_watermark_col {
        tracing::debug!("checkpoint_schema_already_migrated");
        return Ok(());
    }

    tracing::info!("migrating_checkpoint_schema");

    // Backup old table
    sqlx::query(
        "CREATE TABLE _ampsync_checkpoints_backup AS
         SELECT * FROM _ampsync_checkpoints"
    )
        .execute(&self.pool)
        .await?;

    // Drop old table
    sqlx::query("DROP TABLE _ampsync_checkpoints")
        .execute(&self.pool)
        .await?;

    // Create new table with hybrid schema
    self.create_checkpoint_table().await?;

    // Migrate old checkpoints
    sqlx::query(
        "INSERT INTO _ampsync_checkpoints (
            table_name, network, incremental_block_num, incremental_updated_at
        )
        SELECT
            table_name,
            'unknown' as network,
            max_block_num as incremental_block_num,
            updated_at as incremental_updated_at
        FROM _ampsync_checkpoints_backup"
    )
        .execute(&self.pool)
        .await?;

    tracing::info!("checkpoint_schema_migrated_successfully");

    Ok(())
}
```

## ⚡ Performance Guidelines

### 1. 🚀 Memory Management

🚨 **MANDATORY**: Optimize memory usage:

```rust
// ✅ Use RecordBatch.slice() for zero-copy splitting
let smaller_batch = batch.slice(0, target_rows);

// ✅ Pre-calculate buffer sizes
let buffer_size = calculate_buffer_size( & batch);
let mut buffer = Vec::with_capacity(buffer_size);

// ✅ Reuse buffers across batches
struct BatchProcessor {
    hash_buffer: Vec<u8>, // Reused for hashing
    temp_buffer: Vec<u8>, // Reused for serialization
}

// ✅ Use semaphore to limit concurrent operations
let _permit = concurrency_limit.acquire().await?;

// ❌ DON'T load entire tables into memory
// ❌ DON'T create new buffers for each row
// ❌ DON'T clone RecordBatches unnecessarily
```

### 2. 🔥 Database Performance

🚨 **MANDATORY**: Use PostgreSQL COPY protocol:

```rust
/// Insert RecordBatch using PostgreSQL COPY binary protocol
///
/// This is the FASTEST bulk insert method for PostgreSQL:
/// - Binary format (more efficient than text)
/// - Bulk operation (not row-by-row inserts)
/// - Bypasses SQL parsing overhead
///
/// Performance: Can handle 10,000+ rows/second with proper tuning
pub async fn insert_record_batch(
    &self,
    table_name: &str,
    batch: RecordBatch,
) -> Result<()> {
    // ✅ Inject system metadata
    let batch = inject_system_metadata(&batch, &ranges)?;

    // ✅ Convert to PostgreSQL binary format
    let mut encoder = ArrowToPostgresBinaryEncoder::new(&batch.schema());
    let binary_data = encoder.encode_batch(&batch)?;

    // ✅ Use COPY protocol via temp table approach
    let temp_table = format!("_ampsync_temp_{}", uuid::Uuid::new_v4());

    let mut tx = self.pool.begin().await?;

    // Create temp table
    tx.execute(&format!(
        "CREATE TEMP TABLE {} (LIKE {} INCLUDING ALL)",
        temp_table, table_name
    )).await?;

    // COPY to temp table
    let copy_query = format!("COPY {} FROM STDIN WITH (FORMAT BINARY)", temp_table);
    // ... COPY implementation ...

    // Insert from temp with conflict handling
    tx.execute(&format!(
        "INSERT INTO {} SELECT * FROM {} ON CONFLICT DO NOTHING",
        table_name, temp_table
    )).await?;

    // Drop temp table
    tx.execute(&format!("DROP TABLE {}", temp_table)).await?;

    tx.commit().await?;

    Ok(())
}
```

## 🚨 Critical Reminders for AI Agents

**🔥 ABSOLUTELY NON-NEGOTIABLE:**

1. **Data Consistency First**: NEVER compromise data integrity for performance
2. **Always Update Checkpoints**: After EVERY successful batch insert (incremental) and Watermark event (watermark)
3. **Never Silently Drop Data**: If insert fails, halt stream and log clearly
4. **Always Handle Reorgs**: Blockchain reorganizations MUST delete affected rows atomically
5. **Always Use Retry Logic**: Database operations MUST use exponential backoff with circuit breakers
6. **Format Immediately**: Run `just fmt-file <file>` after editing ANY Rust file
7. **Validate Before Commit**: `just check-crate ampsync` and `cargo test -p ampsync` MUST pass
8. **Document Everything**: Every module, function, and error type needs comprehensive docs
9. **Test Everything**: New features MUST have integration tests
10. **Security First**: Always redact passwords in logs, never expose secrets

**🚨 When in Doubt:**

- Read `AGENTS.md` for technical implementation details
- Read `README.md` for user-facing documentation
- Check existing tests for patterns
- Ask for clarification rather than guess

**✅ Success Criteria:**

- All tests pass: `cargo test -p ampsync` ✅
- No compiler warnings: `just check-crate ampsync` ✅
- Formatted correctly: `just fmt-file <files>` ✅
- Data consistency maintained ✅
- Checkpoints updated correctly ✅
- Version polling works (when enabled) ✅

---

🚨 **CRITICAL**: This guide MUST be referenced when implementing new features or modifying existing ones to ensure
consistency with established patterns in the `ampsync` crate. 🚫 **NO EXCEPTIONS**.

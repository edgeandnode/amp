# Ampsync - Technical Overview for AI Agents

## Project Summary

Ampsync is a high-performance synchronization service that streams dataset changes from a Nozzle server and syncs them to a PostgreSQL database. It enables applications to work with Nozzle datasets in their local PostgreSQL instance while maintaining real-time updates and handling blockchain reorganizations automatically.

**Critical Context**: This is a **streaming database synchronization service**, not a batch ETL tool. It maintains continuous connections, handles real-time updates, and must never lose data during blockchain reorganizations.

## Architecture Overview

### Data Flow
1. **Configuration**: Load dataset name and optional version from environment variables
2. **Schema Fetching**: Query Nozzle Admin API to get Arrow schemas and dataset version
   - **First-Run Behavior**: If dataset not published yet, polls indefinitely (2-30s backoff) until available
   - Logs helpful message: "Have you run 'nozzle dump --dataset <name>'?"
   - No restart needed - automatically detects when dataset becomes available
3. **SQL Generation**: Automatically generate SQL queries from Arrow schema for each table
   - Format: `SELECT col1, col2, ... FROM network.table SETTINGS stream = true`
   - Reserved keywords automatically quoted (e.g., "to", "from", "select")
4. **Schema Setup**: Create PostgreSQL tables from Arrow schemas (with evolution support)
5. **Checkpoint Recovery**: Smart resumption strategy using hybrid checkpoints
   - **Watermark (preferred)**: Hash-verified resumption point passed to `query()` (server-side)
   - **Incremental (fallback)**: Best-effort checkpoint, adds `WHERE block_num > X` to SQL (client-side)
   - **None**: No checkpoint available, starts from the beginning
   - **Benefits**: Minimizes reprocessing (typically < 1 batch) while providing fork detection
6. **Streaming**: Execute SQL queries with `SETTINGS stream = true` on Nozzle Arrow Flight server
7. **Reorg Detection**: Wrap streams with `with_reorg()` to detect blockchain reorganizations
8. **Batch Processing**: Convert Arrow RecordBatches to PostgreSQL binary format and bulk insert via COPY
   - **Incremental Checkpoint**: Update progress tracking after each successful batch insertion
   - **Purpose**: Minimize reprocessing between watermark events (typically emitted when ranges complete)
9. **Watermark Processing**: Save canonical checkpoints when server signals ranges are complete
   - **Hash-Verified**: Includes block number + block hash for fork detection
   - **Multi-Network**: Supports cross-chain datasets with per-network checkpoints
   - **Atomic Updates**: Transaction ensures all networks updated together
10. **Version Polling**: When DATASET_VERSION not specified, poll for new versions every 5 seconds
    - Detects when new dataset version is published
    - Gracefully restarts streams with new version
    - Configurable interval via `VERSION_POLL_INTERVAL_SECS`

### Technology Stack
- **Language**: Rust (async/await heavily used)
- **Wire Format**: Apache Arrow (RecordBatch streaming)
- **Database**: PostgreSQL (target sync destination)
- **Data Encoding**: pgpq library for Arrow → PostgreSQL COPY binary format
- **HTTP Client**: reqwest for Admin API queries
- **Concurrency**: tokio async runtime with per-table tasks and semaphore-based backpressure

## Key Components

### 1. Main Binary (`src/main.rs`)
- **Purpose**: Entry point, streaming orchestration, version polling coordination
- **Key Functions**:
  - `ampsync_runner()`: Main loop handling config loading, stream spawning, version change events
  - `spawn_stream_tasks()`: Creates per-table async tasks for streaming data
  - `shutdown_streams_gracefully()`: Ensures in-flight batches complete before shutdown
  - `version_poll_task()`: Background task that polls for new dataset versions (when DATASET_VERSION not set)
- **Signal Handling**: SIGTERM/SIGINT for Docker-compatible graceful shutdown
- **Version Polling**: Monitors admin-api for new versions, triggers graceful reload when detected

### 2. Sync Engine (`src/sync_engine.rs`)
- **Purpose**: Database operations, schema management, bulk data insertion
- **Key Struct**: `AmpsyncDbEngine` - Main interface for all database operations
- **Capabilities**:
  - Arrow schema → PostgreSQL DDL conversion
  - Table creation with schema evolution (add columns)
  - Checkpoint tracking and recovery
  - High-performance bulk insert using PostgreSQL COPY protocol
  - Adaptive batch sizing based on performance metrics
  - Blockchain reorg handling (delete invalidated rows)
- **Critical**: All database errors use exponential backoff retry logic

### 3. Config & Manifest (`src/manifest.rs`)
- **Purpose**: Fetch dataset schemas from Admin API, generate SQL queries
- **Key Functions**:
  - `fetch_manifest()`: Fetches schemas from Admin API for a dataset name/version
  - `resolve_qualified_version()`: Resolves version (e.g., "0.2.0" → "0.2.0-LTcyNjgzMjc1NA")
  - `generate_query_from_schema()`: Automatically generates SQL from Arrow schema fields
- **SQL Generation**: Creates `SELECT col1, col2, ... FROM network.table SETTINGS stream = true`
  - Automatically quotes SQL reserved keywords (using `quote_column_name()` from sync_engine)
- **Version Resolution**:
  - If DATASET_VERSION specified: Matches version prefix to find qualified version
  - If DATASET_VERSION not specified: Returns latest version from admin-api
- **Startup Polling**: `fetch_manifest_with_startup_poll()` polls indefinitely if dataset not found

### 4. PostgreSQL Binary Encoder (`src/pgpq/`)
- **Purpose**: Convert Arrow RecordBatches to PostgreSQL COPY binary format
- **Performance**: Zero-copy where possible, pre-calculated buffer sizes
- **Type Support**: Comprehensive Arrow → PostgreSQL type mapping
- **Critical**: Decimal128, UInt64, and timestamp conversions require special handling

### 5. SQL Validator (`src/sql_validator.rs`)
- **Purpose**: Validate and sanitize SQL queries for incremental streaming
- **Validation Rules**: Rejects ORDER BY, GROUP BY, DISTINCT, aggregates, window functions
- **Sanitization**: Removes ORDER BY and LIMIT clauses automatically
- **Parser**: Uses DataFusion SQL parser for robust validation
- **Note**: Used internally for validation, but SQL is now generated automatically from schema

## Critical Architecture Patterns

### 🔑 System Metadata Architecture

**ALL tables use consistent 3-column system metadata**, regardless of whether `block_num` exists in user's schema:

#### Injected System Columns

**1. `_id` (BYTEA NOT NULL)** - PRIMARY KEY
- **Purpose**: Deterministic, content-based deduplication
- **Hash Algorithm**: xxh3_128 (high-performance 128-bit hash)
- **Hash Input**: `row_content + block_range + row_index`
  - `row_content`: Deterministic serialization of ALL user columns (see `serialize_arrow_value()`)
  - `block_range`: `block_num_start` + `block_num_end` from batch metadata
  - `row_index`: Position within batch (ensures uniqueness even for identical rows)
- **Serialization**: Little-endian byte order, portable across platforms
- **Collision Protection**: PRIMARY KEY constraint ensures hash collisions fail loudly (not silently)
- **Benefits**:
  - Prevents duplicate inserts on reconnect
  - Works with `ON CONFLICT DO NOTHING` for idempotency
  - Consistent across ALL tables (no conditional logic)

**2. `_block_num_start` (BIGINT NOT NULL)**
- **Purpose**: First block number in batch range
- **Used for**: Batch boundary tracking, conservative reorg deletion
- **Source**: `MIN(ranges.start())` from batch metadata

**3. `_block_num_end` (BIGINT NOT NULL)**
- **Purpose**: Last block number in batch range
- **Used for**: Reorg handling (DELETE WHERE `_block_num_end` >= reorg_block)
- **Source**: `MAX(ranges.end())` from batch metadata
- **Reorg Strategy**: Conservative - deletes entire batch if ANY block is invalidated

#### User Schema Columns

**If user's query includes `block_num`**:
- `block_num` column is preserved as a separate user column
- INDEX automatically created on `block_num` for query performance
- PRIMARY KEY remains on `_id` (not `block_num`)
- Example DDL:
  ```sql
  CREATE TABLE blocks (
    _id BYTEA NOT NULL,
    _block_num_start BIGINT NOT NULL,
    _block_num_end BIGINT NOT NULL,
    block_num NUMERIC(20) NOT NULL,  -- User column
    timestamp TIMESTAMPTZ NOT NULL,
    hash BYTEA NOT NULL,
    PRIMARY KEY (_id)
  );
  CREATE INDEX blocks_block_num_idx ON blocks (block_num);
  ```

**If user's query does NOT include `block_num`**:
- Only 3 system columns + user columns
- No `block_num` column or index
- Example DDL:
  ```sql
  CREATE TABLE transfers (
    _id BYTEA NOT NULL,
    _block_num_start BIGINT NOT NULL,
    _block_num_end BIGINT NOT NULL,
    from_addr TEXT NOT NULL,
    to_addr TEXT NOT NULL,
    value NUMERIC(38, 0) NOT NULL,
    PRIMARY KEY (_id)
  );
  ```

#### Why This Design?

**Simplicity**:
- Single code path for ALL tables (no conditional branching)
- No cache needed for tracking which tables have `block_num`
- Easier to reason about and maintain

**Robustness**:
- Hash-based PRIMARY KEY prevents silent data loss from duplicates
- Works correctly even when user queries don't SELECT `block_num`
- Conservative reorg handling is safe (deletes entire batch if unsure)

**Performance**:
- xxh3_128 is extremely fast (30-50 GB/s)
- Reusable hash buffer eliminates allocations (see `batch_utils.rs`)
- INDEX on user's `block_num` provides query performance when needed

#### Implementation Details

**Batch Processing** (`batch_utils.rs::inject_system_metadata()`):
1. Validate batch size (MAX_BATCH_ROWS: 50,000, MAX_BATCH_BYTES: 100MB)
2. Extract block range from metadata
3. For each row:
   - Serialize ALL columns deterministically (little-endian)
   - Append block range (start + end)
   - Append row index
   - Compute xxh3_128 hash → `_id`
4. Create 3 system column arrays
5. Prepend to user column arrays
6. Return new RecordBatch with all columns

**DDL Generation** (`sync_engine.rs::arrow_schema_to_postgres_ddl()`):
1. ALWAYS inject 3 system column definitions first
2. Add user columns from schema
3. PRIMARY KEY on `_id`
4. Check if user schema has `block_num`
5. If yes, create INDEX separately in `create_table()`

**Reorg Handling** (`sync_engine.rs::handle_reorg()`):
- ALWAYS uses `_block_num_end` for deletion (no conditional logic)
- Query: `DELETE FROM table WHERE _block_num_end >= reorg_block`
- Conservative: May delete more than strictly necessary, but safe

### 🚨 Data Consistency Guarantees

**Never Lose Data**:
- If batch insert fails, stream is halted (not silently dropped)
- **Dual checkpoint system** minimizes data reprocessing:
  - **Incremental checkpoints**: Updated after each batch (best-effort progress tracking)
  - **Watermark checkpoints**: Updated on Watermark events (canonical, hash-verified)
- On restart, smart resumption strategy:
  - Prefers watermark (hash-verified, detects forks) → minimal reprocessing
  - Falls back to incremental (best-effort) → typically < 1 batch reprocessed
  - Starts from beginning if no checkpoints exist
- Reorg handling is atomic (delete + wait for corrected data)

**Idempotency**:
- ALL tables have PRIMARY KEY on `_id` (deterministic hash-based)
- Hash includes: row_content + block_range + row_index (ensures uniqueness within batch)
- Duplicate inserts use `ON CONFLICT DO NOTHING` via temp table approach
- Safe to restart at any point

**Fork Detection**:
- Watermark checkpoints include block hash for cryptographic verification
- Detects blockchain forks that simple block number comparison cannot
- Multi-network support enables cross-chain dataset synchronization

### 🔄 Version Polling Flow

**Version Detection** (When DATASET_VERSION not specified):
- Polls admin-api **versions endpoint ONLY** every 5 seconds (configurable via `VERSION_POLL_INTERVAL_SECS`)
- Fetches latest version using `fetch_latest_version()` - does NOT fetch schema (efficient!)
- Compares current version with newly fetched version
- Skips missed ticks if polling falls behind (prevents backlog)
- **Optimization**: Only fetches full schema when version actually changes

**Critical State Management**:
1. New version detected by polling task → Send version to main loop via channel
2. Main loop cancels all stream tasks via `CancellationToken`
3. Await all tasks with timeout (graceful shutdown)
4. Fetch new manifest with explicit version from Admin API
5. Apply schema migrations (add new columns if needed)
6. Spawn new stream tasks with updated manifest
7. **Important**: Database connection pool is REUSED across reloads

**What Triggers Reload**:
- New dataset version published to admin-api
- Only when DATASET_VERSION env var is NOT set
- Detects version changes every 5 seconds (configurable)

**What Can Change**:
- Dataset version (automatic detection and reload)
- Table schemas (adding new columns)
- SQL queries (regenerated from new schema)

**What Cannot Change**:
- Column type changes (rejected with error to prevent data corruption)
- Dropping columns (rejected with error to prevent data loss)
- Database connection (requires restart)
- Dataset name (requires restart)

**Debugging Version Polling**:
- Enable debug logs: `RUST_LOG=debug,ampsync=debug`
- Look for: `"new_version_detected"` → `"version_reload_successful"` or `"version_reload_failed"`
- Check polling status: `"version_poll_failed"` indicates admin-api issues
- Verify VERSION_POLL_INTERVAL_SECS if polling seems slow

### 🎯 Concurrency Model

**Per-Table Async Tasks**:
- Each table runs in its own `tokio::spawn` task
- Independent streams, independent error handling
- Each task has its own retry loop with exponential backoff

**Backpressure via Semaphore**:
- `MAX_CONCURRENT_BATCHES` (default: 10) limits concurrent batch processing
- Prevents OOM when many tables receive large batches simultaneously
- Stream won't pull more data until semaphore permit is available
- **Critical**: This is CROSS-TABLE backpressure, not per-table

**Adaptive Batch Sizing**:
- `AdaptiveBatchManager` tracks performance metrics per batch
- Adjusts batch size based on:
  - Processing time (target: 1 second per batch)
  - Memory usage (target: 50MB per batch)
  - Error rates (reduces size on failures)
- Uses `RecordBatch.slice()` for zero-copy splitting

### 🔐 Security Considerations

**Password Redaction**:
- `sanitize_database_url()` redacts passwords in logs
- Handles complex passwords with special characters (including '@')

**Docker Security**:
- Runs as non-root user (uid 1001)
- Minimal runtime dependencies (debian:bookworm-slim)
- No privileged operations required

**Database Permissions Needed**:
- CREATE TABLE (for schema setup)
- INSERT, DELETE, SELECT (for data operations)
- Should use dedicated user, not superuser

### 🛡️ Reliability Features

**Circuit Breakers**:
- **Connection Circuit Breaker**: Prevents indefinite connection retry loops (default: 300s)
  - Configurable via `DB_MAX_RETRY_DURATION_SECS`
  - Stops retrying after configured duration to avoid resource exhaustion
  - Logs "db_connection_circuit_breaker_triggered" when activated
- **Operation Circuit Breaker**: Prevents indefinite database operation retries (default: 60s)
  - Configurable via `DB_OPERATION_MAX_RETRY_DURATION_SECS`
  - Protects against prolonged database performance issues
  - Uses exponential backoff with intelligent retry logic

**SQL Reserved Word Handling**:
- Automatically quotes column names that are SQL reserved keywords
- Supports columns like "to", "from", "select", "array", "sum", "table", "blob", etc.
- Uses compile-time perfect hash set (phf) for O(1) lookups with zero runtime cost
- No user intervention required - handled transparently in DDL and DML operations

### 📊 Hybrid Checkpoint Strategy

**Problem**: Watermarks are emitted infrequently (when ranges complete). Without incremental checkpoints, streams could reprocess thousands of batches on reconnection.

**Solution**: Dual checkpoint system combining best-effort progress tracking with hash-verified canonical checkpoints.

#### Checkpoint Types

**1. Incremental Checkpoints** (sync_engine.rs:739-768)
- **Updated**: After each successful Batch insertion
- **Purpose**: Best-effort progress tracking between Watermark events
- **Storage**: `incremental_block_num`, `incremental_updated_at` columns
- **SQL**: Uses `GREATEST()` to prevent checkpoint regression
- **Benefits**: Minimizes reprocessing on reconnection (typically < 1 batch)
- **Failure Handling**: Non-critical - warns but doesn't halt stream

**2. Watermark Checkpoints** (sync_engine.rs:758-811)
- **Updated**: On `ResponseBatchWithReorg::Watermark` events
- **Purpose**: Canonical, hash-verified resumption points
- **Storage**: `watermark_block_num`, `watermark_block_hash`, `watermark_updated_at` columns
- **Benefits**:
  - Cryptographic verification via block hash (detects forks)
  - Server-side resumption (passed to `query()`, not WHERE clause)
  - Multi-network support (one checkpoint per network)
- **Failure Handling**: Critical - halts stream to retry (prevents data loss)

#### Resumption Strategy (sync_engine.rs:776-838)

**`load_resume_point()` returns `ResumePoint` enum**:

```rust
pub enum ResumePoint {
    Watermark(ResumeWatermark),           // Preferred
    Incremental { network, max_block_num }, // Fallback
    None,                                  // Start from beginning
}
```

**Decision Logic**:
1. **Try watermark first**: If available, use hash-verified resumption (server-side)
2. **Fall back to incremental**: If no watermark, use best-effort checkpoint (client-side WHERE clause)
3. **Start from beginning**: If no checkpoints exist

**Stream Creation** (main.rs:323-359):
- `Watermark` → Pass to `SqlClient::query(..., resume_watermark)` (server handles resumption)
- `Incremental` → Add `WHERE block_num > X` to SQL query (client filters)
- `None` → No modification

#### Database Schema

```sql
CREATE TABLE _ampsync_checkpoints (
    table_name TEXT NOT NULL,
    network TEXT NOT NULL,
    -- Incremental (best-effort, updated per batch)
    incremental_block_num BIGINT,
    incremental_updated_at TIMESTAMPTZ,
    -- Watermark (canonical, hash-verified, updated on Watermark event)
    watermark_block_num BIGINT,
    watermark_block_hash BYTEA,
    watermark_updated_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (table_name, network)
)
```

#### Migration from Old Schema

**Backward Compatible** (sync_engine.rs:571-670):
- Detects old schema (single `max_block_num` column)
- Migrates old checkpoints to `incremental_block_num` (preserves progress)
- Backs up old table as `_ampsync_checkpoints_backup` (safe rollback)
- Network defaults to "unknown" for migrated checkpoints
- Watermarks established on first Watermark event after migration

#### Performance Characteristics

**Before Hybrid Strategy**:
- Stream drops → Reprocess from last Watermark (could be 1000s of batches)
- Risk of significant data reprocessing

**After Hybrid Strategy**:
- Stream drops → Resume from incremental checkpoint (< 1 batch reprocessed)
- Watermark available → Hash-verified resumption (zero reprocessing)
- Database overhead: 1 UPDATE per batch (non-blocking, acceptable)

#### Example Flow

```
Batch 1 → Insert → Update incremental checkpoint (block 100)
Batch 2 → Insert → Update incremental checkpoint (block 200)
Batch 3 → Insert → Update incremental checkpoint (block 300)
Watermark → Save watermark checkpoint (block 300 + hash 0xabc...)
[Connection drops]
Restart → Load watermark → Resume from block 300 (hash-verified, server-side)
```

Without watermark:
```
Restart → Load incremental → Resume from block 300 (best-effort, WHERE clause)
```

## Configuration

### Environment Variables

**Required**:
- `DATASET_NAME`: Name of the dataset to sync (must be valid `datasets_common::name::Name`)
  - Example: `my_dataset`, `ethereum_blocks`
- `DATABASE_URL` OR (`DATABASE_USER` + `DATABASE_NAME`)

**Optional - Dataset Configuration**:
- `DATASET_VERSION`: Specific dataset version to sync
  - Format: Simple version like `0.1.0` (resolved to qualified version like `0.1.0-LTcyNjgzMjc1NA`)
  - If NOT specified: Automatically uses latest version and polls for new versions
  - If specified: Uses exact version, no automatic updates
- `VERSION_POLL_INTERVAL_SECS`: How often to check for new versions (default: 5 seconds)
  - Only used when `DATASET_VERSION` is NOT specified
  - Range: 1-3600 seconds recommended

**Optional - Nozzle Connection**:
- `AMP_FLIGHT_ADDR`: Nozzle Arrow Flight server (default: http://localhost:1602)
- `AMP_ADMIN_API_ADDR`: Nozzle Admin API server (default: http://localhost:1610)

**Optional - Performance & Reliability**:
- `MAX_CONCURRENT_BATCHES`: Concurrent batch limit (default: 10)
- `DB_MAX_RETRY_DURATION_SECS`: Connection retry circuit breaker duration (default: 300 seconds)
- `DB_OPERATION_MAX_RETRY_DURATION_SECS`: Database operation retry circuit breaker duration (default: 60 seconds)

**Optional - Logging**:
- `RUST_LOG`: Logging configuration (default: info)

**Database Connection Options**:
```bash
# Option 1: Single URL
DATABASE_URL=postgresql://user:pass@host:5432/db

# Option 2: Individual components
DATABASE_USER=postgres
DATABASE_NAME=mydb
DATABASE_PASSWORD=secret  # optional
DATABASE_HOST=localhost   # default: localhost
DATABASE_PORT=5432        # default: 5432
```

### How It Works

**Automatic SQL Generation**:
- No config files needed - SQL queries generated automatically from dataset schema
- Fetches Arrow schema from Admin API (`GET /datasets/{name}/versions/{version}/schema`)
- Generates SQL: `SELECT col1, col2, ... FROM network.table SETTINGS stream = true`
- Reserved keywords automatically quoted (e.g., "to", "from", "select")

**Version Management**:
- **Fixed Version Mode** (DATASET_VERSION set): Uses specified version, never changes
- **Auto-Update Mode** (DATASET_VERSION not set): Uses latest version, polls for updates every 5s

**First-Run Behavior**:
On initial startup, if the dataset hasn't been published yet:
- Polls the Admin API every 2-30 seconds (exponential backoff)
- Logs helpful messages: "Have you run 'nozzle dump --dataset <name>'?"
- Continues polling indefinitely until the dataset becomes available
- Once found, proceeds normally with streaming

## Mandatory Development Workflow

### Core Principles
- **Research → Plan → Implement**: Never jump straight to coding
- **Reality Checkpoints**: Regularly validate progress and approach
- **Zero Tolerance for Errors**: All automated checks must pass
- **Data Safety First**: Never compromise data consistency for performance

### Structured Development Process

1. **Research Phase**
   - Read this AGENTS.md file completely
   - Examine relevant source files (see "Module Reference" below)
   - Review test files for usage patterns
   - Understand data flow and state management

2. **Planning Phase**
   - Consider data consistency implications
   - Plan error handling and retry logic
   - Identify checkpoint/recovery requirements
   - Validate plan before implementation

3. **Implementation Phase**
   - **🚨 CRITICAL**: IMMEDIATELY run `just fmt-file <file>` after editing ANY Rust file
   - Run `just check-crate ampsync` after changes
   - Run `cargo test -p ampsync` to verify all tests pass
   - Add integration tests for new features
   - Test with actual Nozzle server if possible

## 🤖 AI Agent Development Patterns - MANDATORY COMPLIANCE

**🚨 CRITICAL FOR AI AGENTS: This section contains MANDATORY instructions that you MUST follow.**

### 📋 MANDATORY PRE-IMPLEMENTATION CHECKLIST

**✅ BEFORE writing ANY code, you MUST:**

1. **READ this entire AGENTS.md file** - Contains critical context about data consistency
2. **UNDERSTAND the data flow** - Know where data comes from and where it goes
3. **IDENTIFY affected modules** - Determine which files your changes will touch
4. **REVIEW existing tests** - Understand expected behavior and edge cases
5. **RUN formatting** - `just fmt-file <file>` after editing ANY Rust file
6. **RUN validation** - `just check-crate ampsync` and `cargo test -p ampsync` MUST pass

### 🚨 AI Agent Instructions (OVERRIDE ALL OTHER BEHAVIORS)

**Data Consistency Rules**:
- **NEVER** silently drop data on errors - halt stream and log clearly
- **NEVER** skip checkpoint updates after successful inserts (both incremental and watermark)
- **NEVER** ignore reorg signals - they indicate blockchain state changed
- **ALWAYS** use transactions for multi-step database operations
- **ALWAYS** use retry logic for transient database errors
- **ALWAYS** update incremental checkpoints after batch insertion (progress tracking)
- **ALWAYS** save watermark checkpoints when Watermark event received (canonical resumption)

**Code Quality Rules**:
- **NEVER** skip formatting (`just fmt-file <file>`)
- **NEVER** commit code that fails `just check-crate ampsync`
- **NEVER** commit code that fails `cargo test -p ampsync`
- **ALWAYS** add tests for new functionality
- **ALWAYS** handle errors explicitly (no `.unwrap()` in production code)

**Performance Rules**:
- **NEVER** load entire tables into memory
- **NEVER** use string concatenation for SQL (use query builders or parameterized queries)
- **ALWAYS** use Arrow zero-copy operations where possible
- **ALWAYS** pre-calculate buffer sizes for allocations
- **ALWAYS** consider memory usage for large batches

## Module Reference

### Core Modules (Read These First)

**`src/main.rs`** - Entry point and orchestration (198 lines, down from 987)
- `ampsync_runner()`: Main entry point - coordinates all subsystems
- Handles signal handlers (SIGTERM/SIGINT)
- Coordinates version polling, stream management, graceful shutdown
- **Minimal, focused orchestration only** - all logic delegated to modules

**`src/config.rs`** - Configuration management
- `AmpsyncConfig`: Main configuration struct
- `AmpsyncConfig::from_env()`: Load config from environment variables
- `sanitize_database_url()`: Password redaction for safe logging
- Database URL construction from components

**`src/stream_manager.rs`** - Stream task coordination
- `spawn_stream_tasks()`: Creates per-table streaming tasks
- `shutdown_streams_gracefully()`: Graceful shutdown with timeout
- Schema filtering (SELECT * vs SELECT col1, col2, ...)
- Table creation and checkpoint recovery

**`src/stream_task.rs`** - Per-table streaming logic
- `StreamTask`: Encapsulates single table's streaming state and behavior
- `StreamTask::run()`: Main streaming loop with reconnection
- `handle_batch()`: Batch processing, timestamp conversion, metadata injection
- `handle_reorg()`: Reorg detection and cleanup
- `handle_watermark()`: Watermark checkpoint persistence
- Exponential backoff retry logic

**`src/version_polling.rs`** - Version change detection
- `version_poll_task()`: Background polling for new dataset versions
- Polls admin-api every 5 seconds (configurable)
- Sends version changes to main loop via channel
- Only active when DATASET_VERSION not set

**`src/sync_engine.rs`** - Database operations (largest module)
- `AmpsyncDbEngine`: Main database interface
- `ResumePoint`: Enum for smart resumption strategy (Watermark/Incremental/None)
- `arrow_schema_to_postgres_ddl()`: Schema conversion
- `insert_record_batch()`: Batch insertion with adaptive sizing
- `handle_reorg()`: Reorg handling
- **Checkpoint Methods** (hybrid strategy):
  - `load_resume_point()`: Smart resumption (watermark-first, fallback to incremental)
  - `load_watermark()`: Load hash-verified checkpoint
  - `save_watermark()`: Save canonical checkpoint with block hash
  - `update_incremental_checkpoint()`: Update best-effort progress tracking
  - `extract_max_block_from_ranges()`: Helper to extract checkpoint from metadata

**`src/manifest.rs`** - Schema fetching and Admin API integration
- `fetch_manifest()`: Main entry point for schema fetching
- `resolve_qualified_version()`: Version resolution (prefix matching or latest)
- `generate_query_from_schema()`: Automatic SQL generation from Arrow schema

### Support Modules

**`src/conn.rs`** - PostgreSQL connection pooling
- `DbConnPool`: Wrapper around sqlx Pool
- Connection retry logic with exponential backoff

**`src/pgpq/`** - PostgreSQL COPY binary encoding
- `ArrowToPostgresBinaryEncoder`: Main encoder
- `encoders.rs`: Type-specific encoding logic
- `pg_schema.rs`: Arrow → PostgreSQL type mapping

**`src/sql_validator.rs`** - SQL validation and sanitization
- `validate_incremental_query()`: Check for non-incremental patterns
- `extract_select_columns()`: Parse SQL to determine selected columns
- Note: Used internally for validation

**`src/batch_utils.rs`** - RecordBatch utilities and system metadata injection
- `inject_system_metadata()`: Injects 3 system columns (`_id`, `_block_num_start`, `_block_num_end`) into ALL batches
- `serialize_arrow_value()`: Deterministic Arrow value serialization for hashing (little-endian, portable)
- `convert_nanosecond_timestamps()`: Timestamp conversion for PostgreSQL (nanosecond → microsecond)

## Testing Strategy

### Unit Tests (in each module)
- Arrow type mappings (`sync_engine.rs`)
- SQL validation rules (`sql_validator.rs`)
- SQL generation from schema (`manifest.rs`)
- Binary encoding (`pgpq/encoders.rs`)

### Integration Tests (`tests/`)
- `checkpoint_test.rs`: Checkpoint tracking and recovery (9 tests)
- `circuit_breaker_test.rs`: Database retry circuit breaker functionality (3 tests)
- `decimal_insert_test.rs`: Decimal type handling (1 test)
- `injected_block_num_test.rs`: System metadata injection and reorg handling (2 tests)
- `reserved_words_test.rs`: SQL reserved keyword column handling (1 test)
- `schema_evolution_test.rs`: Schema migration scenarios (7 tests)
- `version_polling_test.rs`: Version polling and schema reload functionality (5 tests)

**Testing with PostgreSQL**:
- Tests use `pgtemp` crate for temporary databases
- Each test gets isolated database instance
- Tests are safe to run in parallel

**Testing with HTTP Mocking**:
- Version polling tests use `mockito` for HTTP mocking
- Tests verify that only the versions endpoint is called (not schema endpoint)
- Tests verify version change detection and error handling

## Common Implementation Patterns

### Adding New Arrow Type Support

1. Add mapping in `sync_engine::arrow_type_to_postgres_type()`
2. Add encoder in `pgpq/encoders.rs` if needed
3. Add test in `sync_engine::tests::test_arrow_type_mappings_comprehensive()`
4. Add integration test in `tests/decimal_insert_test.rs` style

### Adding New SQL Validation Rule

1. Add validation logic in `sql_validator::validate_incremental_query()`
2. Add test cases in `sql_validator::tests`
3. Document the rule in README.md under "SQL Validation"

### Adding New Environment Variable

1. Parse in `AmpsyncConfig::from_env()` in `src/main.rs`
2. Add field to `AmpsyncConfig` struct
3. Document in README.md and AGENTS.md under "Environment Variables"
4. Add validation test if needed

### Adding New Database Operation

1. Add method to `AmpsyncDbEngine` in `src/sync_engine.rs`
2. Use retry logic: wrap operation in `(|| async { ... }).retry(...).await`
3. Add comprehensive error messages with context
4. Add test in `tests/` directory
5. Consider checkpoint implications

## Performance Considerations

### Memory Management
- **RecordBatch Splitting**: Use `.slice()` for zero-copy
- **Buffer Pre-allocation**: Call `calculate_buffer_size()` before encoding
- **Semaphore Limits**: Tune `MAX_CONCURRENT_BATCHES` based on available memory
- **Connection Pooling**: Default 5 connections, avoid connection thrashing

### Database Performance
- **COPY Protocol**: Fastest bulk insert method for PostgreSQL
- **Binary Format**: More efficient than text format
- **Batch Sizing**: Adaptive manager optimizes for 1s processing time
- **Indexes**: Only PRIMARY KEY on block_num (user can add more)

### Network Optimization
- **Streaming**: Arrow Flight streams data incrementally, not all at once
- **Compression**: Arrow Flight supports compression (check client config)
- **Reconnection**: Exponential backoff prevents connection storms

## Debugging Tips

### Enable Detailed Logging
```bash
RUST_LOG=trace,ampsync=trace cargo run -p ampsync
```

### Common Log Patterns

**Normal Operation**:
- `"Successfully bulk inserted N rows into table 'X'"` - Batch processed
- `"Updated checkpoint for table 'X' to block N"` - Progress saved
- `"Batch performance: Nms for N rows, new batch size: N"` - Adaptive sizing

**Errors to Investigate**:
- `"CRITICAL: Failed to insert"` - Data loss risk, stream halted
- `"Failed to update checkpoint"` - May reprocess on restart
- `"Failed to handle reorg"` - Data consistency issue

**Performance Issues**:
- `"Pool timeout"` - Too many concurrent operations
- `"Reducing batch size due to repeated errors"` - Database struggling
- `"Semaphore closed"` - Shutdown in progress

### Testing Hot-Reload

1. Start ampsync with a config
2. Modify the config file (change SQL, add column)
3. Watch logs for: `"Manifest file changed, initiating hot-reload..."`
4. Verify: `"Successfully loaded new manifest: ..."`
5. Check database: `\d+ table_name` to see schema changes

## Important Conventions

### When Making Code Changes

**Database Operations**:
- Always use retry logic for transient errors
- Always update checkpoints after successful inserts
- Always handle reorgs by deleting affected rows
- Never use `.unwrap()` on database operations

**Error Handling**:
- Use `Result<T, BoxError>` for fallible operations
- Provide context in error messages (include table name, operation, etc.)
- Log errors at appropriate levels (error for critical, warn for retryable)
- Return errors, don't panic

**Async Operations**:
- Use `tokio::spawn` for independent tasks (per-table streams)
- Use `tokio::select!` for handling multiple async operations
- Use `CancellationToken` for graceful shutdown
- Always timeout long operations

**Testing**:
- Use `pgtemp` for isolated test databases
- Use `mockito` for HTTP mocking (Admin API tests)
- Use `tempfile` for config file tests
- Run tests with `cargo test -p ampsync` (not `cargo nextest`)

### File Naming Conventions
- Integration tests: `tests/*_test.rs`
- Unit tests: `#[cfg(test)] mod tests` in same file
- Modules: Snake_case (e.g., `sync_engine.rs`)

## Troubleshooting Guide for AI Agents

### "Dataset not found in admin-api. This is expected on first run."
- **Cause**: Normal startup behavior - dataset hasn't been published yet
- **Fix**: Run `nozzle dump --dataset <name>` to publish. Ampsync will auto-detect when available.
- **Behavior**: Polls indefinitely with exponential backoff (2-30s intervals)
- **Code Location**: `src/manifest.rs::fetch_manifest_with_startup_poll()`

### "Failed to fetch schema from admin-api"
- **Cause**: Dataset not published to Nozzle server, wrong endpoint, or version mismatch
- **Fix**: Verify dataset exists, check `AMP_ADMIN_API_ADDR`, confirm version matches DATASET_VERSION if set
- **Code Location**: `src/manifest.rs::fetch_manifest()`

### "version_poll_failed" (in logs)
- **Cause**: Version polling task failed to fetch manifest from admin-api
- **Fix**: Check admin-api availability, network connectivity, DATASET_NAME spelling
- **Behavior**: Non-fatal - polling continues with exponential backoff
- **Code Location**: `src/main.rs::version_poll_task()`

### "version_reload_failed" (in logs)
- **Cause**: New version detected but failed to reload manifest
- **Fix**: Check admin-api, verify new version is fully published
- **Behavior**: Continues with current version, will retry on next poll
- **Code Location**: `src/main.rs::ampsync_runner()` main loop

### "CRITICAL: Failed to insert N rows"
- **Cause**: Database error, connection issue, or type incompatibility
- **Fix**: Check PostgreSQL logs, verify types, check network
- **Code Location**: `src/main.rs::spawn_stream_tasks()` batch processing

### "Stream ended for table 'X'. Attempting reconnection..."
- **Cause**: Normal behavior - network blip, Nozzle server restart, or timeout
- **Fix**: No action needed if reconnection succeeds
- **Code Location**: `src/main.rs::spawn_stream_tasks()` retry loop

### Memory usage increasing
- **Cause**: Too many concurrent batches, large batch sizes, or batch size not adapting
- **Fix**: Lower `MAX_CONCURRENT_BATCHES`, check adaptive batch manager logs
- **Code Location**: `src/sync_engine.rs::AdaptiveBatchManager`

### "db_connection_circuit_breaker_triggered"
- **Cause**: Database connection retries exceeded configured timeout duration
- **Fix**: Check database availability, network connectivity. Increase `DB_MAX_RETRY_DURATION_SECS` if needed.
- **Default**: Stops after 300 seconds (5 minutes)
- **Code Location**: `src/conn.rs::DbConnPool::connect_with_max_duration()`

### Database operations timing out after 60 seconds
- **Cause**: Database operation retry circuit breaker triggered
- **Fix**: Check database performance, network latency. Increase `DB_OPERATION_MAX_RETRY_DURATION_SECS` if needed.
- **Default**: Stops after 60 seconds
- **Code Location**: `src/sync_engine.rs::db_max_retry_duration()`

## Quick Reference Commands

```bash
# Format code
just fmt-file src/main.rs

# Check compilation
just check-crate ampsync

# Run all tests
cargo test -p ampsync

# Run specific test
cargo test -p ampsync --test checkpoint_test

# Run with debug logging
RUST_LOG=debug cargo run -p ampsync

# Build release binary
cargo build --release -p ampsync

# Build Docker image
docker build -t ampsync:latest -f crates/bin/ampsync/Dockerfile .

# Run Docker image (auto-update mode)
docker run --rm \
  -e DATASET_NAME=my_dataset \
  -e DATABASE_URL=postgresql://user:pass@host:5432/db \
  -e AMP_ADMIN_API_ADDR=http://nozzle-server:1610 \
  ampsync:latest

# Run Docker image (fixed version mode)
docker run --rm \
  -e DATASET_NAME=my_dataset \
  -e DATASET_VERSION=0.1.0 \
  -e DATABASE_URL=postgresql://user:pass@host:5432/db \
  -e AMP_ADMIN_API_ADDR=http://nozzle-server:1610 \
  ampsync:latest
```

## Additional Resources

- **README.md**: User-facing documentation with usage examples
- **Dockerfile**: Multi-stage build configuration
- **examples/with-electricsql**: Complete working example setup
- **tests/**: Integration tests demonstrating features

## Final Notes for AI Agents

**Critical Reminders**:
1. This is a **streaming** service - it never stops running
2. **Data consistency** is more important than performance
3. **Checkpoints** prevent reprocessing - always update them
4. **Version polling** (when DATASET_VERSION not set) enables automatic updates
5. **Reorgs** are blockchain corrections - handle them correctly

**When in Doubt**:
- Read the test files - they show expected behavior
- Run the example in `examples/with-electricsql`
- Check logs with `RUST_LOG=debug`
- Ask for clarification rather than guess

**Success Criteria**:
- `just check-crate ampsync` passes ✅
- `cargo test -p ampsync` all tests pass ✅
- No compiler warnings ✅
- Data consistency maintained ✅
- Version polling works correctly (when DATASET_VERSION not set) ✅

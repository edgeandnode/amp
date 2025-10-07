# Ampsync - Technical Overview for AI Agents

## Project Summary

Ampsync is a high-performance synchronization service that streams dataset changes from a Nozzle server and syncs them to a PostgreSQL database. It enables applications to work with Nozzle datasets in their local PostgreSQL instance while maintaining real-time updates and handling blockchain reorganizations automatically.

**Critical Context**: This is a **streaming database synchronization service**, not a batch ETL tool. It maintains continuous connections, handles real-time updates, and must never lose data during blockchain reorganizations.

## Architecture Overview

### Data Flow
1. **Config Loading**: Parse nozzle config file (JSON/TS/JS) to extract table SQL queries
2. **Schema Fetching**: Query Nozzle Admin API to get Arrow schemas for all tables
   - **First-Run Behavior**: If dataset not published yet, polls indefinitely (2-30s backoff) until available
   - Logs helpful message: "Have you run 'nozzle dump --dataset <name>'?"
   - No restart needed - automatically detects when dataset becomes available
3. **Schema Setup**: Create PostgreSQL tables from Arrow schemas (with evolution support)
4. **Checkpoint Recovery**: Resume from last processed block using internal checkpoint table
5. **Streaming**: Execute SQL queries with `SETTINGS stream = true` on Nozzle Arrow Flight server
6. **Reorg Detection**: Wrap streams with `with_reorg()` to detect blockchain reorganizations
7. **Batch Processing**: Convert Arrow RecordBatches to PostgreSQL binary format and bulk insert via COPY
8. **Hot-Reload**: Watch config file for changes, gracefully restart streams with new configuration
   - **Hot-Reload Retry**: Limited retries (default: 3) when fetching updated manifest
   - Gives `nozzle dump` command time to complete (~2-30s depending on retries)
   - Configurable via `HOT_RELOAD_MAX_RETRIES` environment variable

### Technology Stack
- **Language**: Rust (async/await heavily used)
- **Wire Format**: Apache Arrow (RecordBatch streaming)
- **Database**: PostgreSQL (target sync destination)
- **Data Encoding**: pgpq library for Arrow → PostgreSQL COPY binary format
- **Config Parsing**: oxc_parser for JS/TS AST parsing, serde_json for JSON
- **File Watching**: notify crate with PollWatcher for hot-reload (polling-based for Docker compatibility)
- **HTTP Client**: reqwest for Admin API queries
- **Concurrency**: tokio async runtime with per-table tasks and semaphore-based backpressure

## Key Components

### 1. Main Binary (`src/main.rs`)
- **Purpose**: Entry point, streaming orchestration, hot-reload coordination
- **Key Functions**:
  - `ampsync_runner()`: Main loop handling config loading, stream spawning, hot-reload events
  - `spawn_stream_tasks()`: Creates per-table async tasks for streaming data
  - `shutdown_streams_gracefully()`: Ensures in-flight batches complete before shutdown
- **Signal Handling**: SIGTERM/SIGINT for Docker-compatible graceful shutdown
- **File Watching**: Monitors nozzle config file for changes, triggers reload on modification

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
- **Purpose**: Parse nozzle configs, fetch schemas from Admin API
- **Two-Tier Architecture**:
  - `DatasetDefinition`: User-facing config (just SQL queries in tables)
  - `Manifest`: Internal format with complete Arrow schemas from Admin API
- **Schema Resolution**: Queries Admin API to resolve version (e.g., "0.2.0" → "0.2.0-LTcyNjgzMjc1NA")
- **SQL Sanitization**: Automatically removes ORDER BY clauses (non-incremental queries not supported)
- **Supported Formats**: JSON, JS, TS (uses oxc_parser for AST parsing)
- **Startup Polling**: `fetch_manifest_with_startup_poll()` polls indefinitely if dataset not found
- **Hot-Reload Retry**: `fetch_manifest_with_retry()` retries with configurable limit for hot-reload scenarios

### 4. PostgreSQL Binary Encoder (`src/pgpq/`)
- **Purpose**: Convert Arrow RecordBatches to PostgreSQL COPY binary format
- **Performance**: Zero-copy where possible, pre-calculated buffer sizes
- **Type Support**: Comprehensive Arrow → PostgreSQL type mapping
- **Critical**: Decimal128, UInt64, and timestamp conversions require special handling

### 5. File Watcher (`src/file_watcher.rs`)
- **Purpose**: Hot-reload support via file system monitoring
- **Implementation**: Uses `PollWatcher` (polling-based) instead of native OS events
- **Polling Interval**: 2 seconds (configurable via `Config::with_poll_interval`)
- **Content Comparison**: Uses `with_compare_contents(true)` to detect actual content changes (not just mtime)
- **Debouncing**: 500ms delay to handle editors that write files in chunks
- **Docker Compatibility**: Polling mode works reliably with Docker volume mounts where native events (inotify/kqueue) often fail
- **Event Detection**: Accepts `Modify(Metadata(_))` events which PollWatcher emits when content changes
- **Detection Latency**: 2-4 seconds (2s poll interval + 500ms debounce)

### 6. SQL Validator (`src/sql_validator.rs`)
- **Purpose**: Validate and sanitize SQL queries for incremental streaming
- **Validation Rules**: Rejects ORDER BY, GROUP BY, DISTINCT, aggregates, window functions
- **Sanitization**: Removes ORDER BY and LIMIT clauses automatically
- **Parser**: Uses DataFusion SQL parser for robust validation

## Critical Architecture Patterns

### 🚨 Data Consistency Guarantees

**Never Lose Data**:
- If batch insert fails, stream is halted (not silently dropped)
- Checkpoint only updated after successful insert
- On restart, resumes from last good checkpoint
- Reorg handling is atomic (delete + wait for corrected data)

**Idempotency**:
- Tables have PRIMARY KEY on `block_num` (if column exists)
- Duplicate inserts use `ON CONFLICT DO NOTHING` via temp table approach
- Safe to restart at any point

### 🔄 Hot-Reload Flow

**File Watcher Detection** (Polling-Based):
- PollWatcher checks file system every 2 seconds
- Compares file contents (not just modification time)
- Debounces for 500ms to handle rapid saves
- Detection latency: 2-4 seconds total
- Works reliably in Docker (native events often fail with volume mounts)

**Critical State Management**:
1. File change detected (via polling) → Send event to main loop
2. Main loop cancels all stream tasks via `CancellationToken`
3. Await all tasks with timeout (graceful shutdown)
4. Load new config, fetch new schemas from Admin API
5. Apply schema migrations (add new columns if needed)
6. Spawn new stream tasks with updated config
7. **Important**: Database connection pool is REUSED across reloads

**What Can Change**:
- SQL queries in tables (with compatible schema)
- Adding new columns (automatic migration)
- Dataset version (will re-fetch schemas)

**What Cannot Change**:
- Column type changes (rejected with error to prevent data corruption)
- Dropping columns (rejected with error to prevent data loss)
- Database connection (requires restart)

**Debugging Hot-Reload**:
- Enable debug logs: `RUST_LOG=debug,ampsync::file_watcher=trace`
- Look for: `"File watcher received event"` → `"File change detected"` → `"Manifest file changed, initiating hot-reload"`
- If not detecting changes: wait 2-4 seconds, verify file mount in Docker, check DATASET_MANIFEST path

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

## Configuration

### Environment Variables

**Required**:
- `DATASET_MANIFEST`: Path to nozzle config file (.ts, .js, .json)
- `DATABASE_URL` OR (`DATABASE_USER` + `DATABASE_NAME`)

**Optional**:
- `AMP_FLIGHT_ADDR`: Nozzle Arrow Flight server (default: http://localhost:1602)
- `AMP_ADMIN_API_ADDR`: Nozzle Admin API server (default: http://localhost:1610)
- `MAX_CONCURRENT_BATCHES`: Concurrent batch limit (default: 10)
- `HOT_RELOAD_MAX_RETRIES`: Hot-reload retry attempts (default: 3)
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

### Nozzle Config Format

**Simple Format (User-Facing)**:
```typescript
export default defineDataset(() => ({
  name: "my-dataset",
  version: "0.1.0",
  network: "mainnet",
  tables: {
    blocks: {
      sql: "SELECT block_num, timestamp FROM anvil.blocks",
    },
  },
}))
```

**Key Points**:
- Tables only need `sql` field - schemas fetched from Admin API
- SQL queries are validated and sanitized automatically
- ORDER BY clauses are removed (non-incremental queries not supported)
- Version can be simple (e.g., "0.2.0") - resolved to qualified version via Admin API

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
- **NEVER** skip checkpoint updates after successful inserts
- **NEVER** ignore reorg signals - they indicate blockchain state changed
- **ALWAYS** use transactions for multi-step database operations
- **ALWAYS** use retry logic for transient database errors

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

**`src/main.rs`** - Start here to understand overall flow
- `ampsync_runner()`: Main entry point
- `spawn_stream_tasks()`: Per-table streaming setup
- `AmpsyncConfig`: Configuration management

**`src/sync_engine.rs`** - Database operations (most changes happen here)
- `AmpsyncDbEngine`: Main database interface
- `arrow_schema_to_postgres_ddl()`: Schema conversion
- `insert_record_batch()`: Batch insertion with adaptive sizing
- `handle_reorg()`: Reorg handling

**`src/manifest.rs`** - Config parsing and Admin API integration
- `fetch_manifest()`: Main entry point for schema fetching
- `load_dataset_definition()`: Parse nozzle config files
- `resolve_qualified_version()`: Version resolution

### Support Modules

**`src/conn.rs`** - PostgreSQL connection pooling
- `DbConnPool`: Wrapper around sqlx Pool
- Connection retry logic with exponential backoff

**`src/pgpq/`** - PostgreSQL COPY binary encoding
- `ArrowToPostgresBinaryEncoder`: Main encoder
- `encoders.rs`: Type-specific encoding logic
- `pg_schema.rs`: Arrow → PostgreSQL type mapping

**`src/file_watcher.rs`** - Hot-reload file monitoring
- `spawn_file_watcher()`: Start file watching
- Debouncing and event filtering

**`src/sql_validator.rs`** - SQL validation and sanitization
- `validate_incremental_query()`: Check for non-incremental patterns
- `sanitize_sql()`: Remove ORDER BY and LIMIT

**`src/batch_utils.rs`** - RecordBatch utilities
- `convert_nanosecond_timestamps()`: Timestamp conversion for PostgreSQL

**`src/dataset_definition.rs`** - User-facing config structures
- `DatasetDefinition`: Simple config format
- `TableDefinition`: Table with just SQL

## Testing Strategy

### Unit Tests (in each module)
- Arrow type mappings (`sync_engine.rs`)
- SQL validation rules (`sql_validator.rs`)
- File event filtering (`file_watcher.rs`)
- Binary encoding (`pgpq/encoders.rs`)

### Integration Tests (`tests/`)
- `checkpoint_test.rs`: Checkpoint tracking and recovery
- `decimal_insert_test.rs`: Decimal type handling
- `hot_reload_test.rs`: Config reload functionality
- `schema_evolution_test.rs`: Schema migration scenarios

**Testing with PostgreSQL**:
- Tests use `pgtemp` crate for temporary databases
- Each test gets isolated database instance
- Tests are safe to run in parallel

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
3. Document in README.md under "Environment Variables"
4. Add validation test in `tests/hot_reload_test.rs`

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

### "Failed to fetch manifest after N retries"
- **Cause**: Hot-reload detected config change but new dataset version not found after retries
- **Fix**: Ensure `nozzle dump` completed successfully. Increase `HOT_RELOAD_MAX_RETRIES` if needed.
- **Behavior**: Limited retries (default: 3) to allow dump command to complete
- **Code Location**: `src/manifest.rs::fetch_manifest_with_retry()`

### "Failed to fetch schema from admin-api"
- **Cause**: Dataset not published to Nozzle server, wrong endpoint, or version mismatch
- **Fix**: Verify dataset exists, check `AMP_ADMIN_API_ADDR`, confirm version matches
- **Code Location**: `src/manifest.rs::fetch_manifest()`

### "Schema mismatch: table exists in admin-api but not in config"
- **Cause**: Local config missing tables that exist in published dataset
- **Fix**: Add missing tables to nozzle config
- **Code Location**: `src/manifest.rs::fetch_manifest()`

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

## Quick Reference Commands

```bash
# Format code
just fmt-file src/main.rs

# Check compilation
just check-crate ampsync

# Run all tests
cargo test -p ampsync

# Run specific test
cargo test -p ampsync --test hot_reload_test

# Run with debug logging
RUST_LOG=debug cargo run -p ampsync

# Build release binary
cargo build --release -p ampsync

# Build Docker image
docker build -t ampsync:latest -f crates/bin/ampsync/Dockerfile .

# Run Docker image
docker run --rm \
  -e DATASET_MANIFEST=/home/ampsync/nozzle.config.ts \
  -e DATABASE_URL=postgresql://user:pass@host:5432/db \
  -v $(pwd)/nozzle.config.ts:/home/ampsync/nozzle.config.ts \
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
4. **Hot-reload** allows config changes without restart - test it
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
- Hot-reload works correctly ✅

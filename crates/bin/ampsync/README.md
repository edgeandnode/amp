# Ampsync

A high-performance synchronization service that streams dataset changes from a Nozzle server and syncs them to a
PostgreSQL database. Designed for production deployments with features like hot-reloading, adaptive batching, and
automatic schema evolution.

## Overview

Ampsync connects to a Nozzle server to stream dataset changes and synchronizes them to a PostgreSQL database. This
enables applications to work with Nozzle datasets in their local PostgreSQL instance while maintaining real-time updates
and handling blockchain reorganizations automatically.

## Key Features

- **Real-time Streaming**: Continuously syncs dataset changes as they occur
- **Automatic SQL Generation**: Generates SQL queries from schema - no config files needed
- **Version Polling**: Automatically detects and loads new dataset versions (when not pinned to specific version)
- **Automatic Schema Inference**: Fetches table schemas from Nozzle Admin API automatically
- **Schema Evolution**: Supports adding new columns to existing tables seamlessly
- **Progress Checkpointing**: Resumes from last processed block on restart (no reprocessing)
- **Blockchain Reorg Handling**: Automatically detects and handles chain reorganizations
- **High Performance**: PostgreSQL COPY protocol with binary format for maximum throughput
- **Adaptive Batching**: Dynamic batch size optimization based on performance metrics
- **Connection Pooling**: Efficient database connection management with exponential backoff retry
- **Circuit Breakers**: Configurable retry timeouts prevent indefinite hangs on connection/operation failures
- **SQL Reserved Word Handling**: Automatically quotes column names that are SQL reserved keywords
- **Graceful Shutdown**: Docker-compatible signal handling (SIGTERM/SIGINT)
- **Concurrent Processing**: Per-table async tasks for parallel data processing

## Configuration

The service is configured through environment variables:

### Required Environment Variables

#### Dataset Configuration

- **`DATASET_NAME`** - Name of the dataset to sync
    - **Type**: Dataset name (string, must be valid `datasets_common::name::Name`)
    - **Example**: `my_dataset`, `ethereum_blocks`
    - **Notes**: Must match a published dataset in Nozzle Admin API

#### Database Connection

Option 1: Single connection string

- **`DATABASE_URL`** - Full PostgreSQL connection string
    - **Type**: PostgreSQL URL (string)
    - **Format**: `postgresql://[user]:[password]@[host]:[port]/[database]`
    - **Example**: `postgresql://myuser:mypassword@localhost:5432/mydb`

Option 2: Individual components (all required except password)

- **`DATABASE_USER`** - Database username
    - **Type**: String
    - **Example**: `postgres`
- **`DATABASE_NAME`** - Database name
    - **Type**: String
    - **Example**: `myapp_db`
- **`DATABASE_PASSWORD`** - Database password
    - **Type**: String (optional)
    - **Example**: `secret123`
- **`DATABASE_HOST`** - Database host
    - **Type**: String
    - **Default**: `localhost`
    - **Example**: `db.example.com`
- **`DATABASE_PORT`** - Database port
    - **Type**: Integer
    - **Default**: `5432`
    - **Example**: `5432`

### Optional Environment Variables

#### Dataset Configuration

- **`DATASET_VERSION`** - Specific dataset version to sync
    - **Type**: Version string (simple version like `0.1.0`)
    - **Example**: `0.1.0`, `1.2.3`
    - **Default**: None (uses latest version)
    - **Notes**: If specified, syncs this exact version. If not specified, automatically uses latest version and polls for updates.

- **`VERSION_POLL_INTERVAL_SECS`** - How often to check for new dataset versions
    - **Type**: Integer (seconds)
    - **Default**: `5`
    - **Range**: `1-3600` (recommended)
    - **Notes**: Only used when `DATASET_VERSION` is NOT specified. Controls how frequently ampsync checks for new versions.

#### Nozzle Connection

- **`AMP_FLIGHT_ADDR`** - URL to Nozzle Arrow Flight server
    - **Type**: HTTP/HTTPS URL
    - **Default**: `http://localhost:1602`
    - **Example**: `https://nozzle.example.com:1602`

- **`AMP_ADMIN_API_ADDR`** - URL to Nozzle Admin API server
    - **Type**: HTTP/HTTPS URL
    - **Default**: `http://localhost:1610`
    - **Example**: `https://nozzle.example.com:1610`
    - **Notes**: Used to fetch dataset schemas and versions

#### Performance Tuning

- **`MAX_CONCURRENT_BATCHES`** - Maximum concurrent batch operations across all tables
    - **Type**: Integer
    - **Default**: `10`
    - **Range**: `1-100` (recommended)
    - **Notes**: Controls backpressure to prevent OOM. Lower values reduce memory usage but may decrease throughput.

#### Reliability & Error Handling

- **`DB_MAX_RETRY_DURATION_SECS`** - Maximum duration for database connection retries (circuit breaker)
    - **Type**: Integer
    - **Default**: `300` (5 minutes)
    - **Range**: `30-3600` (recommended)
    - **Notes**: Prevents indefinite retry loops when database is unavailable. Stops retrying after this duration to avoid resource exhaustion. Logs "db_connection_circuit_breaker_triggered" when activated.

- **`DB_OPERATION_MAX_RETRY_DURATION_SECS`** - Maximum duration for database operation retries (circuit breaker)
    - **Type**: Integer
    - **Default**: `60` (1 minute)
    - **Range**: `10-300` (recommended)
    - **Notes**: Prevents indefinite retries for database operations (inserts, checkpoints, etc.). Protects against prolonged database performance issues. Uses exponential backoff.

#### Logging

- **`RUST_LOG`** - Logging level configuration
    - **Type**: Comma-separated log directives
    - **Default**: `info`
    - **Examples**:
        - `debug` - Debug all modules
        - `info,ampsync=debug` - Info globally, debug for ampsync
        - `warn,sqlx=error` - Warn globally, only errors from sqlx
    - **Levels**: `error`, `warn`, `info`, `debug`, `trace`

### How It Works

**Automatic SQL Generation**:
1. Ampsync fetches the complete dataset schema from Nozzle Admin API (`GET /datasets/{name}/versions/{version}/schema`)
2. Automatically generates SQL queries for each table: `SELECT col1, col2, ... FROM network.table SETTINGS stream = true`
3. Quotes SQL reserved keywords automatically (e.g., "to", "from", "select")
4. Creates PostgreSQL tables with the fetched Arrow schema
5. Starts streaming data using the generated SQL queries

This means you don't need to maintain any config files - everything is driven by the dataset schema in Nozzle.

**Version Management**:
- **Auto-Update Mode** (default): When `DATASET_VERSION` is NOT set, ampsync uses the latest version and polls for updates every 5 seconds
- **Fixed Version Mode**: When `DATASET_VERSION` is set, ampsync uses that specific version and never auto-updates

**First-Run Behavior**: On initial startup, if the dataset hasn't been published yet (no `nozzle dump` run), ampsync will wait patiently:
- Polls the Admin API every 2-30 seconds (exponential backoff)
- Logs helpful messages: "Have you run 'nozzle dump --dataset <name>'?"
- Continues polling indefinitely until the dataset becomes available
- Once found, proceeds normally with streaming

## Usage

### Docker Compose Example

```yaml
services:
  postgres:
    image: postgres:17-alpine
    environment:
      POSTGRES_DB: myapp
      POSTGRES_USER: myuser
      POSTGRES_PASSWORD: mypassword
    ports:
      - "5432:5432"

  ampsync:
    image: ghcr.io/edgeandnode/ampsync:latest
    environment:
      # Dataset configuration
      DATASET_NAME: my_dataset
      # DATASET_VERSION: 0.1.0  # Optional: pin to specific version

      # Nozzle server endpoints
      AMP_FLIGHT_ADDR: http://nozzle-server:1602
      AMP_ADMIN_API_ADDR: http://nozzle-server:1610

      # Database connection
      DATABASE_URL: postgresql://myuser:mypassword@postgres:5432/myapp

      # Logging
      RUST_LOG: info,ampsync=debug
    depends_on:
      - postgres
    restart: unless-stopped
```

### Running with Cargo (Development)

```bash
# Set environment variables
export DATASET_NAME=my_dataset
# export DATASET_VERSION=0.1.0  # Optional: pin to specific version
export DATABASE_URL=postgresql://user:pass@localhost:5432/mydb
export AMP_FLIGHT_ADDR=http://localhost:1602
export AMP_ADMIN_API_ADDR=http://localhost:1610
export RUST_LOG=info,ampsync=debug

# Run the service
cargo run --release -p ampsync
```

### Running with Docker Build

```bash
# Build the Docker image (from repository root)
docker build -t ampsync:local -f crates/bin/ampsync/Dockerfile .

# Run the container (auto-update mode)
docker run --rm \
  -e DATASET_NAME=my_dataset \
  -e DATABASE_URL=postgresql://user:pass@host.docker.internal:5432/mydb \
  -e AMP_FLIGHT_ADDR=http://host.docker.internal:1602 \
  -e AMP_ADMIN_API_ADDR=http://host.docker.internal:1610 \
  ampsync:local

# Run the container (fixed version mode)
docker run --rm \
  -e DATASET_NAME=my_dataset \
  -e DATASET_VERSION=0.1.0 \
  -e DATABASE_URL=postgresql://user:pass@host.docker.internal:5432/mydb \
  -e AMP_FLIGHT_ADDR=http://host.docker.internal:1602 \
  -e AMP_ADMIN_API_ADDR=http://host.docker.internal:1610 \
  ampsync:local
```

### Complete Example with ElectricSQL

See [examples/with-electricsql](examples/with-electricsql) for a complete working example that includes:

- Anvil (local Ethereum node)
- Nozzle server
- PostgreSQL database
- Ampsync service
- ElectricSQL for reactive queries

```bash
cd crates/bin/ampsync/examples/with-electricsql
docker compose up
```

## Architecture

### Data Flow

1. **Configuration**: Loads dataset name and optional version from environment variables
2. **Schema Fetching**: Fetches Arrow schemas from Admin API for the dataset
3. **SQL Generation**: Automatically generates SQL queries from schema: `SELECT col1, col2, ... FROM network.table SETTINGS stream = true`
4. **Schema Setup**: Creates PostgreSQL tables based on fetched Arrow schemas
5. **Checkpoint Recovery**: Determines resumption strategy:
    - **Watermark available**: Hash-verified resumption (server-side, via `query()` parameter)
    - **Incremental only**: Best-effort resumption (client-side, adds `WHERE block_num > X`)
    - **None**: Starts from the beginning
6. **Streaming**: Executes generated SQL queries with `SETTINGS stream = true` on Nozzle server
7. **Reorg Detection**: Wraps streams with `with_reorg()` to detect blockchain reorganizations
8. **Batch Processing**:
    - Receives `Batch` events from streams
    - Converts Arrow data to PostgreSQL binary format using pgpq
    - Inserts data using PostgreSQL COPY protocol for high throughput
    - Updates **incremental checkpoint** after successful insertion (progress tracking)
9. **Watermark Processing**:
    - Receives `Watermark` events when ranges are complete
    - Saves **watermark checkpoint** with block hash (canonical, hash-verified)
    - Preferred for resumption over incremental checkpoints
10. **Reorg Handling**: Deletes affected rows and waits for corrected data
11. **Version Polling** (when DATASET_VERSION not set): Polls for new versions, gracefully reloads when detected

### Performance Architecture

- **Concurrent Processing**: Each table runs in its own async task
- **Adaptive Batching**: Dynamically adjusts batch sizes based on:
    - Processing time (target: 1 second per batch)
    - Memory usage (target: 50MB per batch)
    - Error rates (reduces batch size on failures)
- **Connection Pooling**: Configurable PostgreSQL connection pool
- **Exponential Backoff**: Automatic retry for transient database errors
- **Backpressure**: Semaphore-based concurrency limiting prevents OOM

### Database Schema

#### User Tables

Created automatically from your nozzle config with schemas fetched from Admin API.

**System Metadata Columns** (automatically injected into ALL tables):

- **`_id` (BYTEA)**: PRIMARY KEY - Deterministic hash for deduplication
  - Computed from: row content + block range + row index
  - Uses xxh3_128 (high-performance 128-bit hash)
  - Prevents duplicate inserts on reconnect
  - Hash collisions fail loudly (PRIMARY KEY constraint)

- **`_block_num_start` (BIGINT)**: First block number in batch range
  - Used for batch boundary tracking

- **`_block_num_end` (BIGINT)**: Last block number in batch range
  - Used for blockchain reorganization handling
  - Reorg deletes: `DELETE WHERE _block_num_end >= reorg_block`

**User Schema Columns**:

- Column types automatically mapped from Arrow to PostgreSQL
- If your query includes `block_num`, it's preserved as a separate column with INDEX
- Supports schema evolution (adding new columns via hot-reload)
- Automatically quotes SQL reserved keyword column names ("to", "from", "select", etc.)

**Example Table Structure**:

```sql
-- Table WITH block_num in user's query
CREATE TABLE blocks (
  _id BYTEA NOT NULL,              -- System: PRIMARY KEY
  _block_num_start BIGINT NOT NULL, -- System: Batch start
  _block_num_end BIGINT NOT NULL,   -- System: Batch end
  block_num NUMERIC(20) NOT NULL,   -- User: From query
  timestamp TIMESTAMPTZ NOT NULL,   -- User: From query
  hash BYTEA NOT NULL,              -- User: From query
  PRIMARY KEY (_id)
);
CREATE INDEX blocks_block_num_idx ON blocks (block_num);

-- Table WITHOUT block_num in user's query
CREATE TABLE transfers (
  _id BYTEA NOT NULL,               -- System: PRIMARY KEY
  _block_num_start BIGINT NOT NULL, -- System: Batch start
  _block_num_end BIGINT NOT NULL,   -- System: Batch end
  from_addr TEXT NOT NULL,          -- User: From query
  to_addr TEXT NOT NULL,            -- User: From query
  value NUMERIC(38, 0) NOT NULL,    -- User: From query
  PRIMARY KEY (_id)
);
```

**Key Benefits**:

- **Idempotency**: Safe to restart at any point (hash-based deduplication)
- **Reorg Safety**: Conservative deletion ensures no missed invalidations
- **Consistency**: All tables use identical PRIMARY KEY strategy
- **Performance**: Fast hashing (30-50 GB/s) with reusable buffers

#### Internal Tables

- **`_ampsync_checkpoints`**: Hybrid checkpoint tracking for resumable streaming
    - **Schema**:
        - `table_name` (TEXT): Table identifier
        - `network` (TEXT): Network name (mainnet, sepolia, etc.)
        - `incremental_block_num` (BIGINT): Best-effort progress tracking (updated per batch)
        - `incremental_updated_at` (TIMESTAMPTZ): Last incremental checkpoint time
        - `watermark_block_num` (BIGINT): Canonical checkpoint with hash verification
        - `watermark_block_hash` (BYTEA): Block hash for fork detection
        - `watermark_updated_at` (TIMESTAMPTZ): Last watermark checkpoint time
        - `updated_at` (TIMESTAMPTZ): Last modification time
    - **Primary Key**: `(table_name, network)`
    - **Checkpoint Strategy**:
        - **Incremental checkpoints**: Updated after each batch insertion for progress tracking between watermarks
        - **Watermark checkpoints**: Updated on `Watermark` events from server (hash-verified, canonical)
        - **Resumption**: Prefers watermark (hash-verified), falls back to incremental, starts from beginning if neither exists
    - **Benefits**:
        - Minimizes reprocessing on reconnection (typically < 1 batch)
        - Hash-verified resumption detects blockchain forks
        - Multi-network support for cross-chain datasets

### Error Handling

Ampsync handles errors at multiple levels:

1. **Stream Errors**: Automatic reconnection with exponential backoff (max 5 retries)
2. **Database Errors**: Retry logic for transient failures (deadlocks, connection timeouts)
3. **Batch Failures**: Reduces batch size and retries
4. **Critical Errors**: Logs error, stops stream for that table to prevent data loss
5. **Reorg Errors**: Halts stream to ensure data consistency

### Version Polling

When `DATASET_VERSION` is NOT specified, ampsync automatically detects and loads new dataset versions:

**Polling Mechanism**:
1. Background task polls Admin API every 5 seconds (configurable via `VERSION_POLL_INTERVAL_SECS`)
2. Fetches latest version and compares with current version
3. When new version detected:
   - All active streams are gracefully stopped via cancellation token
   - New manifest fetched from Admin API with the new version
   - Tables are migrated if needed (adds new columns)
   - SQL queries regenerated from new schema
   - Streams are restarted with new configuration

**Version Management Modes**:
- **Auto-Update Mode** (DATASET_VERSION not set): Automatically detects and loads new versions
- **Fixed Version Mode** (DATASET_VERSION set): Uses specified version, never auto-updates

**What Can Change**:
- Dataset version (automatic detection and reload)
- Table schemas (adding new columns)
- SQL queries (regenerated from new schema)

**What Cannot Change**:
- Column type changes (rejected with error to prevent data corruption)
- Dropping columns (rejected with error to prevent data loss)
- Dataset name (requires restart)
- To remove columns or change types, manually alter the database and restart ampsync

## Development

### Building

```bash
# Build release binary
cargo build --release -p ampsync

# Build Docker image
docker build -t ampsync:latest -f crates/bin/ampsync/Dockerfile .
```

### Testing

```bash
# Run all tests
cargo test -p ampsync

# Run specific test
cargo test -p ampsync --test checkpoint_test

# Run with logging
RUST_LOG=debug cargo test -p ampsync -- --nocapture
```

**Integration Tests** (28 tests total):
- `checkpoint_test.rs`: Checkpoint tracking and recovery (9 tests)
- `circuit_breaker_test.rs`: Database retry circuit breaker functionality (3 tests)
- `decimal_insert_test.rs`: Decimal type handling (1 test)
- `injected_block_num_test.rs`: System metadata injection and reorg handling (2 tests)
- `reserved_words_test.rs`: SQL reserved keyword column handling (1 test)
- `schema_evolution_test.rs`: Schema migration scenarios (7 tests)
- `version_polling_test.rs`: Version polling and automatic schema reload (5 tests)

### Project Structure

```
crates/bin/ampsync/
├── src/
│   ├── main.rs              # Main entry point and orchestration
│   ├── lib.rs               # Public library interface
│   ├── config.rs            # Configuration management
│   ├── version_polling.rs   # Version change detection
│   ├── stream_manager.rs    # Stream task coordination and table setup
│   ├── stream_task.rs       # Per-table streaming logic
│   ├── sync_engine.rs       # Database operations, schema management
│   ├── conn.rs              # PostgreSQL connection pooling
│   ├── manifest.rs          # Schema fetching, Admin API client, SQL generation
│   ├── sql_validator.rs     # SQL query validation/sanitization
│   ├── batch_utils.rs       # RecordBatch utilities, system metadata injection
│   └── pgpq/                # PostgreSQL COPY protocol encoder
│       ├── mod.rs
│       ├── encoders.rs      # Arrow to PostgreSQL binary encoding
│       ├── pg_schema.rs     # Schema mapping
│       └── error.rs
├── tests/                   # Integration tests
├── examples/
│   └── with-electricsql/    # Complete example setup
├── Dockerfile               # Multi-stage Docker build
└── README.md
```

## Troubleshooting

### Common Issues

**"Dataset not found in admin-api. This is expected on first run."**

- **This is normal on first startup!** Ampsync is waiting for the dataset to be published.
- Run `nozzle dump --dataset <name>` to publish the dataset
- Ampsync will automatically detect when the dataset becomes available and start streaming
- No restart needed - it polls the Admin API automatically

**"Failed to fetch schema from admin-api: HTTP 404"**

- If this error persists after running `nozzle dump`, check:
  - Dataset name and version match between config and published dataset
  - `AMP_ADMIN_API_ADDR` points to the correct Admin API endpoint
  - Dataset was successfully published (check Nozzle server logs)

**"Database connection failed"**

- Verify PostgreSQL is running and accessible
- Check `DATABASE_URL` or individual database env vars
- Ensure database user has CREATE TABLE permissions
- Check network connectivity (especially in Docker environments)

**"Stream ended for table 'X'. Attempting reconnection..."**

- Normal behavior - streams reconnect automatically
- Check Nozzle server logs for issues
- Verify network connectivity to Nozzle server
- If persistent, check `RUST_LOG=debug` output for details

**Memory usage increasing over time**

- Reduce `MAX_CONCURRENT_BATCHES` (default: 10)
- Check for tables with very large batches
- Monitor adaptive batch manager adjustments in debug logs
- Consider database connection pool size

**"db_connection_circuit_breaker_triggered" error**

- Database connection retries exceeded configured timeout (default: 300 seconds)
- Check database availability and network connectivity
- Increase `DB_MAX_RETRY_DURATION_SECS` if database startup is slow
- Verify PostgreSQL is running and accessible

**Database operations timing out after 60 seconds**

- Database operation retry circuit breaker triggered
- Check database performance and network latency
- Increase `DB_OPERATION_MAX_RETRY_DURATION_SECS` if needed
- Monitor PostgreSQL logs for slow queries or locks

**"version_poll_failed" or "version_reload_failed" in logs**

- Version polling encountered an error while checking for new versions
- Check admin-api availability and network connectivity
- Verify `DATASET_NAME` spelling and that dataset exists in admin-api
- Non-fatal - polling will continue with exponential backoff
- If persistent, check `AMP_ADMIN_API_ADDR` configuration

**"Version reload failed: Columns dropped from schema (unsupported)"**

- Version update attempted to drop columns (not supported for data safety)
- To remove a column:
  1. Manually drop the column from PostgreSQL: `ALTER TABLE blocks DROP COLUMN column_name;`
  2. Publish new dataset version with column removed
  3. Restart ampsync (automatic reload won't work for this case)

### Debug Logging

Enable detailed logging to troubleshoot issues:

```bash
# Maximum detail
RUST_LOG=trace,ampsync=trace cargo run -p ampsync

# Debug ampsync, info for dependencies
RUST_LOG=info,ampsync=debug cargo run -p ampsync

# Only show warnings and errors
RUST_LOG=warn,ampsync=warn cargo run -p ampsync
```

### Health Monitoring

Monitor these log messages for health:

**Normal Operation**:
- `"Successfully bulk inserted N rows into table 'X'"` - Batch processed successfully
- `"incremental_checkpoint_updated"` (debug) - Progress tracking between watermarks
- `"watermark_saved"` (info) - Canonical checkpoint established
- `"Batch performance: Nms for N rows, new batch size: N"` - Adaptive batching working

**Resumption**:
- `"resuming_from_watermark"` - Best case: hash-verified resumption (minimal reprocessing)
- `"resuming_from_incremental_checkpoint"` - Fallback: best-effort resumption (some reprocessing)
- `"starting_from_beginning"` - No checkpoint available (full sync)

**Blockchain Events**:
- `"Reorg detected for table 'X'"` - Blockchain reorganization detected
- `"Successfully handled reorg for table 'X'"` - Reorg processing complete

**Warnings to Monitor**:
- `"incremental_checkpoint_update_failed"` - Non-critical, may reprocess data on restart
- `"watermark_save_failed"` - Critical, stream will reconnect to retry
- `"invalid_watermark_hash_size"` - Data corruption, watermark skipped

## Performance Tuning

### Batch Size Optimization

The adaptive batch manager automatically optimizes batch sizes, but you can influence it:

- **Memory-constrained environments**: Lower `MAX_CONCURRENT_BATCHES` (e.g., 3-5)
- **High-throughput scenarios**: Increase `MAX_CONCURRENT_BATCHES` (e.g., 15-20)
- Monitor logs for `"Batch performance"` messages to see adjustments

### Database Connection Pool

Default pool size: 5 connections per table. For many tables:

- Monitor `"Pool timeout"` errors in logs
- Consider running fewer tables per ampsync instance
- Scale horizontally with multiple ampsync instances (different configs)

### Network Optimization

For remote Nozzle servers:

- Use compression if available
- Deploy ampsync close to Nozzle server (same region/datacenter)
- Monitor stream reconnection frequency

## Security Considerations

- **Passwords in Logs**: Database passwords are automatically redacted in logs
- **File Permissions**: Ensure nozzle config file has appropriate permissions (readable by ampsync user)
- **Docker Security**: Runs as non-root user (uid 1001) with minimal attack surface
- **Network Security**: Use HTTPS for Nozzle endpoints in production (`AMP_FLIGHT_ADDR`, `AMP_ADMIN_API_ADDR`)
- **Database Access**: Use least-privilege database credentials (CREATE, INSERT, DELETE, SELECT on target tables)

## License

See repository root for license information.

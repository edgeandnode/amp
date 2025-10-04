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
- **Hot-Reload Support**: Automatically reloads when nozzle config changes (no restarts needed)
- **Automatic Schema Inference**: Fetches table schemas from Nozzle Admin API automatically
- **Schema Evolution**: Supports adding new columns to existing tables seamlessly
- **Progress Checkpointing**: Resumes from last processed block on restart (no reprocessing)
- **Blockchain Reorg Handling**: Automatically detects and handles chain reorganizations
- **High Performance**: PostgreSQL COPY protocol with binary format for maximum throughput
- **Adaptive Batching**: Dynamic batch size optimization based on performance metrics
- **Connection Pooling**: Efficient database connection management with exponential backoff retry
- **Graceful Shutdown**: Docker-compatible signal handling (SIGTERM/SIGINT)
- **Concurrent Processing**: Per-table async tasks for parallel data processing

## Configuration

The service is configured through environment variables:

### Required Environment Variables

#### Dataset Manifest

- **`DATASET_MANIFEST`** - Path to your nozzle config file
    - **Type**: File path (string)
    - **Example**: `./nozzle.config.ts` or `/app/nozzle.config.json`
    - **Supported formats**: `.ts`, `.js`, `.mts`, `.mjs`, `.json`
    - **Notes**: Can be relative or absolute path. File must exist and be readable.

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

#### Nozzle Connection

- **`AMP_FLIGHT_ADDR`** - URL to Nozzle Arrow Flight server
    - **Type**: HTTP/HTTPS URL
    - **Default**: `http://localhost:1602`
    - **Example**: `https://nozzle.example.com:1602`

- **`AMP_ADMIN_API_ADDR`** - URL to Nozzle Admin API server
    - **Type**: HTTP/HTTPS URL
    - **Default**: `http://localhost:1610`
    - **Example**: `https://nozzle.example.com:1610`
    - **Notes**: Used to fetch dataset schemas for table creation

#### Performance Tuning

- **`MAX_CONCURRENT_BATCHES`** - Maximum concurrent batch operations across all tables
    - **Type**: Integer
    - **Default**: `10`
    - **Range**: `1-100` (recommended)
    - **Notes**: Controls backpressure to prevent OOM. Lower values reduce memory usage but may decrease throughput.

#### Logging

- **`RUST_LOG`** - Logging level configuration
    - **Type**: Comma-separated log directives
    - **Default**: `info`
    - **Examples**:
        - `debug` - Debug all modules
        - `info,ampsync=debug` - Info globally, debug for ampsync
        - `warn,sqlx=error` - Warn globally, only errors from sqlx
    - **Levels**: `error`, `warn`, `info`, `debug`, `trace`

### How Schema Fetching Works

1. Ampsync parses your nozzle config file to extract table SQL queries
2. Fetches the complete schema from Nozzle Admin API (`GET /datasets/{name}/versions/{version}/schema`)
3. Combines the schema (from Admin API) with SQL queries (from config file)
4. Creates PostgreSQL tables with the fetched schema
5. Starts streaming data using your SQL queries

This means you maintain SQL queries in your config, and schemas are always in sync with the Nozzle server.

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
      DATASET_MANIFEST: /home/ampsync/nozzle.config.ts

      # Nozzle server endpoints
      AMP_FLIGHT_ADDR: http://nozzle-server:1602
      AMP_ADMIN_API_ADDR: http://nozzle-server:1610

      # Database connection
      DATABASE_URL: postgresql://myuser:mypassword@postgres:5432/myapp

      # Logging
      RUST_LOG: info,ampsync=debug
    volumes:
      # Mount config for hot-reload (writeable for file events)
      - ./nozzle.config.ts:/home/ampsync/nozzle.config.ts
    depends_on:
      - postgres
    restart: unless-stopped
```

### Running with Cargo (Development)

```bash
# Set environment variables
export DATASET_MANIFEST=./nozzle.config.ts
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

# Run the container
docker run --rm \
  -e DATASET_MANIFEST=/home/ampsync/nozzle.config.ts \
  -e DATABASE_URL=postgresql://user:pass@host.docker.internal:5432/mydb \
  -e AMP_FLIGHT_ADDR=http://host.docker.internal:1602 \
  -e AMP_ADMIN_API_ADDR=http://host.docker.internal:1610 \
  -v $(pwd)/nozzle.config.ts:/home/ampsync/nozzle.config.ts \
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

1. **Config Loading**: Ampsync reads your nozzle config file and fetches schemas from Admin API
2. **Schema Setup**: Creates PostgreSQL tables based on fetched Arrow schemas
3. **Checkpoint Recovery**: Resumes from last processed block (if any) using internal checkpoints
4. **Streaming**: Executes SQL queries with `SETTINGS stream = true` on Nozzle server
5. **Reorg Detection**: Wraps streams with `with_reorg()` to detect blockchain reorganizations
6. **Batch Processing**:
    - Receives RecordBatches from streams
    - Converts Arrow data to PostgreSQL binary format using pgpq
    - Inserts data using PostgreSQL COPY protocol for high throughput
    - Updates checkpoint after successful insertion
7. **Reorg Handling**: Deletes affected rows and waits for corrected data
8. **Hot-Reload**: Watches config file, gracefully restarts streams on changes

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

Created automatically from your nozzle config with schemas fetched from Admin API:

- Column types mapped from Arrow to PostgreSQL
- Primary key on `block_num` (if present)
- Supports schema evolution (adding new columns)

#### Internal Tables

- **`_ampsync_checkpoints`**: Tracks last processed `block_num` per table
    - Columns: `table_name`, `max_block_num`, `updated_at`
    - Enables resuming from last checkpoint on restart

### Error Handling

Ampsync handles errors at multiple levels:

1. **Stream Errors**: Automatic reconnection with exponential backoff (max 5 retries)
2. **Database Errors**: Retry logic for transient failures (deadlocks, connection timeouts)
3. **Batch Failures**: Reduces batch size and retries
4. **Critical Errors**: Logs error, stops stream for that table to prevent data loss
5. **Reorg Errors**: Halts stream to ensure data consistency

### Hot-Reload Mechanism

When the nozzle config file changes:

1. File watcher detects the change (debounced to 500ms)
2. All active streams are gracefully stopped via cancellation token
3. New config is loaded and validated
4. Schemas are fetched from Admin API
5. Tables are migrated if needed (adds new columns)
6. Streams are restarted with new configuration

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
cargo test -p ampsync --test hot_reload_test

# Run with logging
RUST_LOG=debug cargo test -p ampsync -- --nocapture
```

### Project Structure

```
crates/bin/ampsync/
├── src/
│   ├── main.rs              # Main entry point, streaming logic
│   ├── lib.rs               # Public library interface
│   ├── sync_engine.rs       # Database operations, schema management
│   ├── conn.rs              # PostgreSQL connection pooling
│   ├── manifest.rs          # Config parsing, Admin API client
│   ├── file_watcher.rs      # Hot-reload file watching
│   ├── sql_validator.rs     # SQL query validation/sanitization
│   ├── batch_utils.rs       # RecordBatch utilities
│   ├── dataset_definition.rs # User-facing config structures
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

**"Failed to fetch schema from admin-api: HTTP 404"**

- Ensure the dataset exists in Nozzle with the specified name and version
- Verify `AMP_ADMIN_API_ADDR` points to the correct Admin API endpoint
- Check that the dataset has been published to the Nozzle server

**"Schema mismatch: table 'X' exists in admin-api schema but not in local config"**

- Your local config is missing tables that exist in the published dataset
- Ensure your config includes all tables from the dataset
- Check that you're using the correct version of the dataset

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

- `"Successfully bulk inserted N rows into table 'X'"` - Normal operation
- `"Updated checkpoint for table 'X' to block N"` - Progress tracking
- `"Batch performance: Nms for N rows, new batch size: N"` - Adaptive batching
- `"Reorg detected for table 'X'"` - Blockchain reorganization handled
- `"Successfully handled reorg for table 'X'"` - Reorg processing complete

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

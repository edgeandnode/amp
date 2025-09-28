# Ampsync Binary Development Plan

## Overview

Build a Docker-deployable binary that syncs Nozzle dataset changes to a PostgreSQL database.

## Current Status: **Phase 5 Complete - Enterprise-Grade Sync Engine**

### Completed Features:
1. **JS/TS Config Parsing** - Advanced oxc_parser AST-based parsing for nozzle.config files
2. **Manifest Loading** - Converts parsed config to Rust Manifest struct with Arc sharing
3. **Database Schema Creation** - Comprehensive Arrow → PostgreSQL DDL conversion
4. **Table Management** - Idempotent table creation with IF NOT EXISTS
5. **Connection Pooling** - Production-ready sqlx PostgreSQL connection management with exponential backoff
6. **Error Handling** - Robust error propagation and logging throughout
7. **Streaming Implementation** - ResultStream queries with with_reorg for each table
8. **High-Performance Data Insertion** - pgpq-based bulk copy for maximum throughput
9. **Concurrent Processing** - Per-table async tasks for parallel data processing
10. **Graceful Shutdown** - Docker-compatible signal handling (SIGTERM/SIGINT)
11. **Clean Architecture** - Separation of concerns with pure functions and stateful operations
12. **Blockchain Reorg Handling** - Automatic deletion of invalidated data with block range tracking
13. **Exponential Backoff Retry Logic** - Database connection and operation retry with exponential backoff
14. **Adaptive Batch Sizing** - Dynamic batch optimization based on performance metrics and memory constraints

### Final Phase: Containerization
- Docker containerization for production deployment
- Multi-stage build optimization
- Health checks and container best practices

## Architecture Context

### Usage Flow
1. **Developer Setup**: Developer building an app on top of a Nozzle dataset
2. **Docker Compose**: Includes `ampsync` service (this binary) and PostgreSQL
3. **Data Flow**:
   - Ampsync connects to remote Nozzle server (or local for development)
   - Streams data from specified dataset using `with_reorg`
   - Stores streamed data in PostgreSQL tables

## Goals & Tasks

### 1. Docker Binary Setup

- [ ] Build a binary that is runnable as a Docker image
- [ ] Configure Dockerfile with proper base image and dependencies
- [ ] Set up proper entry point for the binary

### 2. Configuration

- [x] Accept environment variables:
    - `DATASET_MANIFEST` - Path to nozzle config file (required, e.g., "./nozzle.config.ts")
        - Validates file exists and has valid extension (ts, js, mts, mjs, json)
        - Can be relative path (will run in same repo as nozzle.config file)
        - Parses manifest using oxc_parser AST for JS/TS or serde_json for JSON
        - Includes parsed manifest in config as Arc<Manifest>
        - Supports: JSON files, simple JS exports, module.exports patterns
    - `NOZZLE_ENDPOINT` - URL to Nozzle server (defaults to http://localhost:1602)
    - `DATABASE_URL` (single connection string)
    - OR individual components:
        - `DATABASE_USER`
        - `DATABASE_PASSWORD`
        - `DATABASE_HOST`
        - `DATABASE_PORT`
        - `DATABASE_NAME`
- [x] Build `AmpsyncConfig` instance from environment (completed)

### 3. Database Connection Pool

- [x] Initialize `DbConnPool` instance using the config
- [x] Handle connection errors gracefully
- [x] Implement retry logic for database connectivity

### 4. Nozzle Client Integration

- [x] Initialize `SqlClient` with Nozzle endpoint
- [x] For each table in manifest tables:
    - [x] Build SQL query from manifest table definitions
    - [x] Execute query to get ResultStream
    - [x] Wrap stream with `nozzle-client::with_reorg` function
    - [x] Handle both data batches and reorg signals (data batches complete, reorg pending)

### 5. Data Synchronization

- [x] Database table creation:
    - [x] Parse manifest table schemas (Arrow format)
    - [x] Create PostgreSQL tables with IF NOT EXISTS from Arrow schema
    - [x] Handle data type mappings (Arrow → PostgreSQL)
    - [x] Implement proper error handling
- [x] Data streaming and insertion:
    - [x] Parse RecordBatch data from streams
    - [x] High-performance bulk copy using pgpq library
    - [x] PostgreSQL COPY protocol with binary format for maximum throughput
    - [x] Concurrent processing with per-table async tasks
    - [x] Proper error handling and logging
- [x] When listener receives reorg signal:
    - [x] Delete affected rows based on invalidation ranges using block_num column
    - [x] Handle multiple invalidation ranges efficiently with OR conditions
    - [x] Comprehensive logging and monitoring of reorg events
    - [x] Wait for corrected data in subsequent batches (automatic via stream)

## Technical Approach

### Phase 1: Foundation

1. Review existing `AmpsyncConfig` implementation
2. Understand `nozzle-client::with_reorg` API and data structures
3. Design database schema creation strategy

### Phase 2: Database Layer ✅ **COMPLETED**

1. ✅ Implement `DbConnPool` with connection pooling using `sqlx`
2. ✅ Create schema management module for table creation from Arrow schemas
3. ✅ Build data insertion/update logic with adaptive batching (COMPLETED)

### Phase 3: Integration

1. Wire up the Nozzle client listener
2. Connect listener events to database operations
3. Add proper logging and monitoring

### Phase 4: Containerization

1. Create Dockerfile
2. Set up multi-stage build for optimization
3. Configure health checks

## Key Considerations

### Error Handling

- Network failures between Nozzle and database
- Schema conflicts or migrations
- Data type mismatches
- Reorg handling (data rollbacks)

### Performance

- Batch size optimization
- Connection pool sizing
- Transaction boundaries
- Concurrent processing if applicable

### Monitoring

- Log levels and structured logging
- Metrics for sync lag
- Health check endpoint
- Error reporting

## Dependencies to Review

- `nozzle-client` crate API
- Database driver (likely PostgreSQL via `sqlx`)
- Arrow/DataFusion data types for schema mapping
- Existing Nozzle data structures

## Questions to Resolve

1. ~~What database types should we support?~~ → PostgreSQL only
2. How should we handle schema evolution? → Dynamic table creation based on Arrow schema
3. What's the expected data volume and latency requirements?
4. ~~Should we support multiple datasets or focus on one?~~ → One dataset per ampsync instance
5. ~~How do we handle reorgs?~~ → Delete affected rows and re-insert corrected data

## Research Findings

### Manifest Parsing Implementation
- **JS/TS Parsing**: Uses oxc_parser for AST-based parsing of JavaScript/TypeScript files
- **Supported Patterns**:
  - `export default { ... }` - Direct object exports ✅
  - `module.exports = { ... }` - CommonJS exports ✅
  - JSON files - Direct serde_json parsing ✅
  - Complex patterns like `defineDataset(() => ({ ... }))` - Foundation implemented
- **AST Visitor**: Converts JavaScript expressions to JSON Values, then to Manifest struct
- **Error Handling**: Comprehensive parse error reporting and fallback strategies
- **Dependencies**: oxc_allocator, oxc_ast, oxc_parser, oxc_span (v0.38)

### nozzle-client::with_reorg API
- **Function**: `with_reorg(result_stream: ResultStream) -> BoxStream<Result<ResponseBatchWithReorg, Error>>`
- **Purpose**: Wraps a ResultStream to detect blockchain reorganizations
- **Returns**: Stream that yields either:
  - `ResponseBatchWithReorg::Batch { data: RecordBatch, metadata: Metadata }` - normal data
  - `ResponseBatchWithReorg::Reorg { invalidation: Vec<InvalidationRange> }` - reorg signal
- **RecordBatch**: Arrow format with schema and columnar data
- **Metadata**: Contains block ranges for the batch

### Database Connection Pattern (from existing code)
- Using `sqlx` with PostgreSQL
- `DbConnPool` wrapper around `Pool<Postgres>`
- Connection pooling with configurable size
- Already implemented in `src/conn.rs`

### Arrow to SQL Schema Mapping Strategy
- RecordBatch contains `schema()` method returning Arrow schema
- Schema has fields with names and DataTypes
- Need to map Arrow DataTypes to PostgreSQL types:
  - `DataType::Int64` → `BIGINT`
  - `DataType::Utf8` → `TEXT`
  - `DataType::Boolean` → `BOOLEAN`
  - `DataType::Binary/FixedSizeBinary` → `BYTEA`
  - `DataType::Timestamp` → `TIMESTAMP`
  - `DataType::Decimal128` → `NUMERIC`
  - etc.

## Next Implementation Steps

1. ~~**Setup SqlClient connection**~~ ✅ **COMPLETED**
   - ✅ Initialize `SqlClient::new()` with nozzle server endpoint
   - ✅ Configure query with SQL and streaming settings
   - ✅ NOZZLE_ENDPOINT environment variable support

2. ~~**Manifest parsing and configuration**~~ ✅ **COMPLETED**
   - ✅ Parse nozzle.config files (JSON, JS, TS) using oxc_parser
   - ✅ Validate manifest structure and include in AmpsyncConfig
   - ✅ Remove redundant DATASET_NAME env var (derived from manifest)

3. ~~**Implement schema converter**~~ ✅ **COMPLETED**
   - ✅ Create function to convert Arrow Schema to PostgreSQL CREATE TABLE statement
   - ✅ Handle data type mappings (comprehensive Arrow → PostgreSQL conversion)
   - ✅ Generate appropriate column definitions with nullable/non-nullable support
   - ✅ Use IF NOT EXISTS for idempotent table creation

4. **Build sync loop** 🔄 **NEXT**
   - Use `with_reorg()` to wrap query stream
   - Handle both Batch and Reorg variants
   - For Batch: insert data (tables already created)
   - For Reorg: handle data invalidation/rollback

5. **Data insertion logic**
   - Convert RecordBatch rows to SQL INSERT statements
   - Use batch inserts for performance
   - Handle transactions properly

6. **Main binary structure** ✅ **COMPLETED**
   - ✅ Parse config from environment
   - ✅ Initialize database pool
   - ✅ Create nozzle client
   - ✅ Load and parse manifest
   - ✅ Create database tables from manifest schemas
   - ✅ Run async sync loop with streaming queries
   - ✅ Handle graceful shutdown

## 🚀 **Latest Major Accomplishments**

### Performance-Optimized Data Pipeline
- **pgpq Integration**: Implemented high-performance bulk data insertion using PostgreSQL's COPY protocol
- **Binary Format**: Arrow RecordBatches are encoded directly to PostgreSQL binary format for maximum throughput
- **Concurrent Processing**: Each table runs in its own async task for parallel data processing
- **Zero-Copy Operations**: Minimized data copying through direct Arrow to PostgreSQL binary conversion

### Architecture Improvements
- **Clean Separation**: Split pure functions (schema conversion) from stateful operations (database engine)
- **Better Testing**: Pure functions can be tested without database dependencies
- **Rust-Idiomatic**: Proper use of Deref trait and stateless functions where appropriate
- **Error Handling**: Comprehensive error propagation and logging throughout the pipeline

### Production Readiness
- **Docker Signals**: Proper SIGTERM/SIGINT handling for container environments
- **Connection Pooling**: Production-ready PostgreSQL connection management
- **Streaming Architecture**: Fault-tolerant stream processing with proper error isolation
- **Blockchain Reorg Handling**: Automatic data consistency maintenance during chain reorganizations

### Blockchain Reorganization Handling
- **Automatic Detection**: Uses `with_reorg` stream wrapper to detect blockchain reorganizations
- **Efficient Deletion**: SQL DELETE queries with block range conditions (e.g., `block_num >= X AND block_num <= Y`)
- **Multiple Range Support**: Handles multiple invalidation ranges in a single operation using OR conditions
- **Zero Downtime**: Reorg handling happens asynchronously without stopping data flow
- **Monitoring**: Comprehensive logging of reorg events and deletion impact
- **Data Consistency**: Ensures database remains consistent with the canonical blockchain state

### Advanced Performance Optimizations

#### Exponential Backoff Retry Logic
- **Database Connection Retry**: Automatic retry for connection failures with exponential backoff
- **Operation Retry**: Retry logic for transient database errors (deadlocks, connection timeouts)
- **Smart Error Classification**: Distinguishes between retryable and permanent errors
- **Configurable Retry Policy**: Min delay 50ms, max delay 5s, max attempts 5
- **PostgreSQL Error Codes**: Handles specific error codes (53300, 40001, 40P01, etc.)
- **Comprehensive Logging**: Warns on retry attempts with timing information

#### Adaptive Batch Sizing System
- **AdaptiveBatchManager**: Intelligent batch size optimization based on real-world performance
- **Performance Tracking**: Records processing time and throughput for each batch
- **Memory Constraints**: Estimates memory usage and constrains batch sizes (50MB target)
- **Dynamic Adjustment**: Adapts batch sizes based on recent performance samples
  - Target 1 second processing time per batch
  - Increases batch size if processing too fast (< 500ms)
  - Decreases batch size if processing too slow (> 1000ms)
- **Error-Based Adjustment**: Reduces batch sizes on repeated failures
- **Thread-Safe Design**: Uses Arc<Mutex<>> for concurrent access across async tasks
- **Configurable Limits**: Min 100 rows, max 50,000 rows, configurable targets
- **Smart Chunking**: Splits large RecordBatches into optimal-sized chunks
- **Zero-Copy Slicing**: Uses Arrow's slice() method for efficient batch splitting
- **Real-Time Monitoring**: Comprehensive logging of batch performance and adjustments

#### Key Performance Benefits
- **Self-Optimizing**: Automatically finds optimal batch sizes for different data patterns
- **Memory Safe**: Prevents OOM errors through intelligent memory estimation
- **Fault Tolerant**: Adapts to errors by reducing batch sizes
- **High Throughput**: Maximizes PostgreSQL COPY performance while respecting system limits
- **Production Ready**: Handles varying load conditions and data characteristics
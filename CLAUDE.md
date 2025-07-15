# Project Nozzle - Technical Overview for Claude

## Project Summary

Project Nozzle is a high-performance ETL (Extract, Transform, Load) architecture for blockchain data services on The Graph. It focuses on extracting blockchain data from various sources, transforming it via SQL queries, and serving it through multiple query interfaces.

## Architecture Overview

### Data Flow
1. **Extract**: Pull data from blockchain sources (EVM RPC, Firehose, Substreams)
2. **Transform**: Process data using SQL queries with custom UDFs
3. **Store**: Save as Parquet files (columnar format optimized for analytics)
4. **Serve**: Provide query interfaces (Arrow Flight gRPC, JSON Lines HTTP)

### Technology Stack
- **Language**: Rust (2024 edition)
- **Query Engine**: Apache DataFusion (v48)
- **Storage Format**: Apache Parquet
- **Wire Format**: Apache Arrow
- **Database**: PostgreSQL (for metadata)
- **Runtime**: Tokio (async runtime)

## Key Components

### 1. Main Binary (`nozzle`)
- Central command dispatcher
- Commands:
  - `dump`: Extract data from sources to Parquet
  - `server`: Start query servers
  - `worker`: Run distributed worker node
  - `generate-manifest`: Create dataset manifests

### 2. Data Extraction (`dump`)
- Parallel extraction with configurable workers
- Resumable extraction (tracks progress)
- Supports:
  - Configurable partition sizes
  - Compression options
  - Incremental dumps
  - Memory management for large datasets

### 3. Query Serving (`server`)
- **Arrow Flight Server** (port 1602): High-performance binary protocol
- **JSON Lines Server** (port 1603): Simple HTTP interface
- **Registry Service** (port 1611): Dataset discovery
- **Admin API** (port 1610): Management interface
- Features:
  - SQL query execution via DataFusion
  - Streaming query support
  - Query optimization

### 4. Data Sources

#### EVM RPC (`evm-rpc-datasets`)
- Connects to Ethereum-compatible JSON-RPC endpoints
- Tables: blocks, transactions, logs
- Batched requests for efficiency

#### Firehose (`firehose-datasets`)
- StreamingFast Firehose protocol (gRPC)
- Real-time blockchain data streaming
- Tables: blocks, transactions, logs, calls
- Protocol buffer-based

#### Substreams (`substreams-datasets`)
- Processes Substreams packages
- Dynamic schema inference
- Entity-based transformations

### 5. Core Libraries

#### `common`
- Shared utilities and abstractions
- Configuration management
- EVM-specific UDFs (User-Defined Functions)
- Catalog management
- Attestation support

#### `metadata-db`
- PostgreSQL-based metadata storage
- Tracks:
  - File metadata
  - Worker nodes
  - Job scheduling
  - Progress tracking
- Uses LISTEN/NOTIFY for distributed coordination

#### `dataset-store`
- Dataset management
- SQL dataset support
- Manifest parsing
- JavaScript UDF support

## Configuration

### Environment Variables
- `NOZZLE_CONFIG`: Path to main config file
- `NOZZLE_LOG`: Logging level (error/warn/info/debug/trace)
- `NOZZLE_CONFIG_*`: Override config values

### Key Directories
1. **dataset_defs_dir**: Dataset definitions (input)
2. **providers_dir**: External service configs
3. **data_dir**: Parquet file storage (output)

### Storage Support
- Local filesystem
- S3-compatible stores
- Google Cloud Storage
- Azure Blob Storage

## SQL Capabilities

### Custom UDFs (User-Defined Functions)
- `evm_decode_log`: Decode EVM event logs
- `evm_topic`: Get event topic hash
- `eth_call`: Execute RPC calls during queries
- `evm_decode_params`: Decode function parameters
- `evm_encode_params`: Encode function parameters
- `attestation_hash`: Generate data attestations

### Dataset Definition
- Raw datasets: Direct extraction from sources
- SQL datasets: Views over other datasets
- Materialized views for performance

## Usage Examples

### Dump Command
```bash
# Extract first 4 million blocks with 2 parallel jobs
cargo run --release -p nozzle -- dump --dataset eth_firehose -e 4000000 -j 2
```

### Query via HTTP
```bash
curl -X POST http://localhost:1603 --data "select * from eth_rpc.logs limit 10"
```

### Python Integration
- Arrow Flight client available
- Marimo notebook examples provided
- Direct DataFrame integration

## Development Notes

### Prerequisites
- PostgreSQL for metadata (via docker-compose)
- Rust toolchain
- Optional: Python environment for client

### Project Structure
- Workspace-based Cargo project
- 20 member crates
- Extensive use of async/await
- Type-safe SQL building

### Testing
- Integration tests in `tests/` directory
- Test configurations provided
- Mock data available

### Performance Considerations
- Memory usage scales with:
  - Number of parallel jobs
  - Partition size
  - Number of tables
- Snmalloc allocator optional for performance
- Streaming support for large result sets

## Important Conventions

### When Making Code Changes
1. Never expose secrets/keys
2. Maintain type safety
3. Prefer async operations
4. Always run `cargo test`
5. Always run `cargo +nightly fmt`
6. Fix all warnings

### SQL Naming
- Dataset name = schema name
- Tables referenced as `dataset.table`
- Views can reference other views

### File Formats
- Parquet files named by start block
- Partitioned by block ranges
- Compressed by default

## Security Notes
- This is currently a private repository
- May be open-sourced (don't commit sensitive data)
- All authentication tokens in environment variables
- Dataset isolation via schemas

## Future Considerations
- Distributed processing capabilities
- Advanced query optimization
- More data source types
- Enhanced monitoring/observability

## More info

Look into `glossary.md` for more.

## Planning

AI generated plans for issues are registered in `issue_context/`.

## Commit messages

- Don't credit Claude Code
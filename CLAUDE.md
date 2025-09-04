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
- **Language**: Rust
- **Query Engine**: Apache DataFusion
- **Storage Format**: Apache Parquet
- **Wire Format**: Apache Arrow
- **Database**: PostgreSQL (for metadata)

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

### 3. Query Serving (`server`)
- **Arrow Flight Server** (port 1602): High-performance binary protocol
- **JSON Lines Server** (port 1603): Simple HTTP interface
- **Registry Service** (port 1611): Dataset discovery
- **Admin API** (port 1610): Management interface
- Features:
  - SQL query execution via DataFusion
  - Streaming query support

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

## Development Patterns Reference

The `.patterns/` directory contains comprehensive development patterns and best practices for this codebase. **Always
reference these patterns before implementing new functionality** to ensure consistency with established codebase
conventions.

### Available Patterns:

- **[.patterns/README.md](.patterns/README.md)** - Overview of development patterns and usage guidelines
- **[testing-patterns.md](.patterns/testing-patterns.md)** - Testing strategies and best practices for writing tests

### Pattern Usage Guidelines:

1. **Before coding**: Review relevant patterns in `.patterns/` directory
2. **During implementation**: Follow established conventions and coding patterns
3. **For complex features**: Use patterns as templates for consistent implementation
4. **When stuck**: Reference similar implementations in existing codebase following these patterns

## Crate-Specific Guidelines

Some crates in this workspace have their own detailed contributing guidelines that supplement the global patterns. **You MUST check and follow these crate-specific guidelines when working on the respective crate.**

### metadata-db

- **[metadata-db/CONTRIBUTING.md](metadata-db/CONTRIBUTING.md)** - **REQUIRED reading** when working on the `metadata-db` crate. Contains comprehensive development guidelines including database design patterns, testing strategies, and API conventions that must be followed.

## 🔐 Crate-Specific Security Guidelines

**🚨 CRITICAL: Some crates have specialized security requirements that MUST be reviewed before making any changes.**

Security guidelines provide essential protection for sensitive operations, data handling, and compliance requirements. These documents contain mandatory security checklists, coding patterns, and review processes that ensure code meets enterprise security standards.

### `metadata-db` Security Requirements

- **[metadata-db/SECURITY.md](metadata-db/SECURITY.md)** - **🚨 MANDATORY SECURITY REVIEW** for all `metadata-db` changes. Contains comprehensive security checklist covering database security, access control, OWASP compliance, and secure coding patterns.

**⚠️ WARNING: Security violations may result in:**
- Immediate rejection of pull requests
- Required security audits and remediation
- Compliance violations and regulatory issues  
- Potential data breach risks

**🎯 AI Agent Instructions:**
- **ALWAYS** review crate-specific security guidelines BEFORE making changes
- **COMPLETE** all security checklists as part of development process
- **PRIORITIZE** security requirements over convenience or speed
- **ESCALATE** any security concerns or questions immediately
- **NEVER** bypass or ignore security requirements

## Development Notes

### Prerequisites
- PostgreSQL for metadata (via docker-compose)
- Rust toolchain
- Optional: Python environment for client

### Project Structure
- Workspace-based Cargo project
- Extensive use of async/await

### Testing
- Integration tests in `tests/` directory
- Test configurations provided
- Mock data available

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

## More info

Look into `docs/` for more.

## Planning

AI generated plans for issues are registered in `issue_context/`.

# Project Amp - Technical Overview for Claude

## Project Summary

Project Amp is a high-performance ETL (Extract, Transform, Load) architecture for blockchain data services on The Graph. It focuses on extracting blockchain data from various sources, transforming it via SQL queries, and serving it through multiple query interfaces.

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

### 1. Main Binary (`ampd`)
- Central command dispatcher
- Commands:
  - `dump`: Extract data from sources to Parquet
  - `dev`: Start development server
  - `server`: Start query servers
  - `worker`: Run distributed worker node
  - `generate-manifest`: Create dataset manifests

### 2. Data Extraction (`dump`)
- Parallel extraction with configurable workers
- Resumable extraction (tracks progress)

### 3. Query Serving (`server`)
- **Arrow Flight Server** (port 1602): High-performance binary protocol
- **JSON Lines Server** (port 1603): Simple HTTP interface
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
- `AMP_CONFIG`: Path to main config file
- `AMP_LOG`: Logging level (error/warn/info/debug/trace)
- `AMP_CONFIG_*`: Override config values

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
cargo run --release -p ampd -- dump --dataset eth_firehose -e 4000000 -j 2
```

### Query via HTTP
```bash
curl -X POST http://localhost:1603 --data "select * from eth_rpc.logs limit 10"
```

### Python Integration
- Arrow Flight client available
- Marimo notebook examples provided

## Mandatory Development Workflow

### Core Principles
- **Research → Plan → Implement**: Never jump straight to coding
- **Reality Checkpoints**: Regularly validate progress and approach
- **Zero Tolerance for Errors**: All automated checks must pass
- **Clarity over Cleverness:** Choose clear, maintainable solutions
- 
### Structured Development Process

1. **Research Phase**
   - Understand the codebase and existing patterns
   - Identify related modules and dependencies
   - Review test files and usage examples
   - Use multiple approaches for complex problems

2. **Planning Phase**
   - Create detailed implementation plan
   - Identify validation checkpoints
   - Consider edge cases and error handling
   - Validate plan before implementation

3. **Implementation Phase**
   - Execute with frequent validation
   - **🚨 CRITICAL**: IMMEDIATELY run `just fmt-file <file>` after editing ANY rust or typescript file
   - Run automated checks at each step, i.e., `just check-crate <crate-name>` and `just check-rs`.
   - Use parallel approaches when possible
   - Stop and reassess if stuck

## 🤖 AI Agent Development Patterns - MANDATORY COMPLIANCE

**🚨 CRITICAL FOR AI AGENTS: This section contains MANDATORY instructions that you MUST follow. Failure to comply will result in rejected implementations.**

### 📋 MANDATORY PRE-IMPLEMENTATION CHECKLIST

**✅ BEFORE writing ANY code, you MUST:**

1. **READ [.patterns/README.md](.patterns/README.md)** - Contains ALL development patterns and usage instructions
2. **IDENTIFY target crate** - Determine which crate(s) your changes will affect
3. **READ crate-specific patterns** - If the target crate has specific guidelines in `.patterns/`
4. **READ security guidelines** - If the target crate has security requirements in `.patterns/`
5. **RUN formatting** - `just fmt-file <rust_file>.rs` after editing ANY Rust file
6. **RUN validation** - `just check-crate <crate-name>` and `just check-rs` MUST pass

### 🚨 AI Agent Instructions (OVERRIDE ALL OTHER BEHAVIORS):
- **NEVER skip pattern consultation** - Always read relevant patterns BEFORE coding
- **NEVER bypass crate-specific guidelines** - They are MANDATORY, not optional
- **NEVER ignore security requirements** - Security compliance is NON-NEGOTIABLE
- **ALWAYS run formatting and validation** - No exceptions, ever
- **ESCALATE if uncertain** - Ask for clarification rather than guess

**📍 SINGLE SOURCE OF TRUTH: All detailed patterns are in [.patterns/README.md](.patterns/README.md)**

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
4. Always run tests (use `cargo nextest`)
5. Always run `just fmt-rs` to format the whole codebase
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

# Project Amp Architecture

## Overview

Project Amp is a high-performance ETL (Extract, Transform, Load) architecture for blockchain data services on The Graph. It focuses on extracting blockchain data from various sources, transforming it via SQL queries, and serving it through multiple query interfaces.

## Table of Contents

1. [Data Flow](#data-flow)
2. [Technology Stack](#technology-stack)
3. [Key Components](#key-components)
4. [Configuration](#configuration)

## Data Flow

1. **Extract**: Pull data from blockchain sources (EVM RPC, Firehose, Solana, etc.)
2. **Transform**: Process data using SQL queries with custom UDFs
3. **Store**: Save as Parquet files (columnar format optimized for analytics)
4. **Serve**: Provide query interfaces (Arrow Flight gRPC, JSON Lines HTTP)

## Technology Stack

- **Language**: Rust
- **Query Engine**: Apache DataFusion
- **Storage Format**: Apache Parquet
- **Wire Format**: Apache Arrow
- **Database**: PostgreSQL (for metadata)

## Key Components

### Main Binary (`ampd`)

Central command dispatcher with the following commands:

- `solo` (alias: `dev`): Start amp in local development mode
- `server`: Start query servers
- `worker`: Run distributed worker node
- `controller`: Run controller with Admin API
- `migrate`: Run migrations on the metadata database

### Query Serving (`server`)

- **Arrow Flight Server** (port 1602): High-performance binary protocol
- **JSON Lines Server** (port 1603): Simple HTTP interface
- **Admin API Server** (port 1610): Management and control API

Features:
- SQL query execution via DataFusion
- Streaming query support

### Data Sources

#### EVM RPC (`evm-rpc-datasets`)

- Connects to Ethereum-compatible JSON-RPC endpoints
- Tables: blocks, transactions, logs
- Batched requests for efficiency

#### Firehose (`firehose-datasets`)

- StreamingFast Firehose protocol (gRPC)
- Real-time blockchain data streaming
- Tables: blocks, transactions, logs, calls
- Protocol buffer-based

#### Solana (`solana`)

- Old Faithful archive support (CAR file format)
- Solana RPC WebSocket subscriptions
- Real-time block streaming

#### ETH Beacon (`eth-beacon`)

- Ethereum Beacon chain data extraction
- Consensus layer data support

### Core Libraries

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
- `AMP_CONFIG_*`: Override config values (use double underscore for nested values)
- `AMP_NODE_ID`: Node ID for workers

### Key Directories

1. **manifests_dir** (or `dataset_defs_dir`): Dataset manifests (input)
2. **providers_dir**: External service configs
3. **data_dir**: Parquet file storage (output)

### Storage Support

- Local filesystem
- S3-compatible stores
- Google Cloud Storage
- Azure Blob Storage

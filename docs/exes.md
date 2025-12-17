# Executables

This document provides an overview of the executables available in the Amp project. Each executable serves a specific purpose in the Amp ecosystem, from toolchain management to data extraction, query serving, and administration.

## Overview

Amp provides several executables designed for different use cases and user personas:

- **`ampup`** - Toolchain distribution and version management
- **`ampd`** - The Amp daemon (extraction, query serving, job orchestration)
- **`ampctl`** - Administration and control CLI for operators
- **`amp`** - Developer toolkit CLI for dataset development
- **`ampsync`** - PostgreSQL synchronization utility

## `ampup` - Toolchain Distribution and Management

**Purpose**: `ampup` is the official installer and version manager for the Amp toolchain. It handles downloading, installing, and switching between different versions of `ampd` and related binaries.

**Installation**: Get started with Amp by running the installation script from [ampup.sh](http://ampup.sh):

```bash
curl --proto '=https' --tlsv1.2 -sSf https://ampup.sh/install | sh
```

This script downloads and runs `ampup init`, which:

- Sets up the Amp installation directory (`$AMP_DIR` or `~/.amp`)
- Configures your shell's PATH
- Installs the latest version of `ampd` by default

**Key Features**:

- **Version management**: Install, list, switch between, and uninstall versions
- **Binary distribution**: Downloads pre-built binaries from GitHub releases
- **Build from source**: Can build and install from local or remote Git repositories
- **Cross-platform**: Supports Linux and macOS (x86_64 and aarch64)

**When to Use**:

- **Initial setup**: Setting up Amp on a new system
- **Version management**: Upgrading or downgrading Amp versions
- **Testing**: Managing multiple Amp versions for testing or compatibility
- **Production deployments**: Installing specific versions for production environments

**Common Commands**:

```bash
# Install latest version
ampup install

# Install specific version
ampup install v0.5.0

# List installed versions
ampup list

# Switch to a different version
ampup use v0.4.0

# Build from source
ampup build --path /path/to/amp-repo
```

## Main Executables

### `ampd` - The Amp Daemon

**Purpose**: `ampd` is the core Amp daemon that handles data extraction, transformation, and query serving. It is a multi-mode executable that can run in different operational configurations depending on deployment needs.

**Operational Modes**: `ampd` supports two primary operational modes, each suited for different deployment patterns:

1. **Single-Node Mode** (`ampd solo`) - Combined server + worker for local development
2. **Distributed Mode** - Separate controller, server, and worker processes for production

For detailed information about operational modes, deployment patterns, and scaling strategies, see [Operational Modes](modes.md).

**Key Commands**:

- `ampd solo` - Start solo server with embedded worker (single-node mode)
- `ampd server` - Run query server (Arrow Flight + JSON Lines)
- `ampd controller` - Run the controller responsible for job scheduling and exposing the engine administration interface
- `ampd worker` - Run extraction worker process

**When to Use**:

- **Development**: Use `ampd solo` for local testing and prototyping
- **Production extraction and serving**: Use distributed mode (`server`, `controller`, `worker`) for scalable deployments

### `ampctl` - Administration and Control CLI

**Purpose**: `ampctl` is the administration CLI for Amp engine operators. It provides a comprehensive command-line interface for managing all aspects of Amp infrastructure through the engine administration interface. This is the operator's swiss-army knife for Amp administration.

**Target Audience**: Infrastructure operators, DevOps engineers, and administrators responsible for managing Amp deployments. This tool focuses on production infrastructure management rather than dataset development workflows.

**Engine Administration Interface**: `ampctl` communicates with the Amp controller's engine administration interface (Admin API), providing CLI access to all administrative capabilities:

- **Dataset Management**: List, retrieve, register datasets with versioning; trigger extraction jobs; query schemas and manifests
- **Job Control**: List, monitor, stop, and delete jobs; trigger extractions with configurable end blocks; bulk cleanup operations
- **Storage Management**: List and query storage locations (local, S3, GCS, Azure); manage file metadata and Parquet statistics
- **Provider Configuration**: Create, retrieve, list, and delete provider configurations for EVM RPC and Firehose data sources
- **Worker Monitoring**: List active workers and monitor heartbeat status across distributed deployments
- **Schema Analysis**: Validate SQL queries against registered datasets; infer output schemas; extract network dependencies

**When to Use**:

- **Dataset management**: Managing datasets and providers in production environments
- **Job control**: Monitoring and controlling extraction jobs
- **Storage inspection**: Inspecting storage locations and file metadata
- **Worker monitoring**: Monitoring distributed worker health
- **Schema validation**: Validating dataset SQL queries and schemas

**Note**: This CLI is under active development. Future versions will add commands for all engine administration interface capabilities.

### `amp` - Dataset Development CLI (Typescript SDK)

**Purpose**: `amp` is the developer-oriented CLI implemented in TypeScript, designed to provide an integrated experience while developing and testing datasets. It is distributed as part of the `@edgeandnode/amp` npm package alongside the TypeScript SDK.

**Target Audience**: Dataset developers, data engineers, and developers building applications on top of Amp. Unlike `ampctl` which focuses on infrastructure operations, `amp` provides a development-focused workflow for dataset iteration and exploration.

**Key Features**:

- **Developer experience**: Streamlined workflow for dataset development and testing
- **TypeScript-based**: Implemented in TypeScript for consistency with the SDK
- **Engine administration integration**: Communicates with the Amp engine via the engine administration interface
- **Interactive tooling**: Provides interactive features for dataset exploration and debugging

**When to Use**:

- **Dataset development**: Developing and testing new datasets locally
- **Query iteration**: Iterating on dataset SQL queries and transformations
- **Schema exploration**: Exploring dataset schemas and metadata
- **Application integration**: Building applications that integrate with Amp

**Location**: The `amp` CLI is located in the `typescript/amp/` directory, separate from the Rust-based executables.

**Note**: This CLI is under active development. Future documentation will provide detailed command references and usage examples.

## Utilities

### `ampsync` - PostgreSQL Synchronization Utility

**Purpose**: `ampsync` is a high-performance synchronization service that streams dataset changes from an Amp server and syncs them to a PostgreSQL database. It enables applications to work with Amp datasets using standard PostgreSQL tools and queries.

**Target Audience**: Application developers who want to integrate Amp datasets with PostgreSQL-based applications, ORMs, and analytics tools.

**Key Features**:

- **Real-time streaming**: Continuously syncs dataset changes as they occur
- **Automatic schema management**: Fetches schemas from the engine administration interface and creates PostgreSQL tables automatically
- **Version polling**: Automatically detects and loads new dataset versions (hot-reload support)
- **Blockchain reorg handling**: Automatically handles chain reorganizations
- **Progress checkpointing**: Resumes from last processed block on restart

**When to Use**:

- **PostgreSQL integration**: Integrate Amp datasets with existing PostgreSQL-based applications
- **Analytics and reporting**: Enable SQL-based analytics and reporting on blockchain data
- **Standard tooling**: Build applications using standard PostgreSQL tools (ORMs, query builders)
- **Low-latency access**: Provide low-latency access to blockchain data for web applications

**Basic Usage**:

```bash
# Set environment variables
export AMP_DATASET_NAME=eth_mainnet
export DATABASE_URL=postgresql://user:pass@localhost:5432/mydb

# Run the sync service
ampsync sync
```

**Configuration**: `ampsync` is configured entirely through environment variables, including dataset name, version, database connection details, and performance tuning options.

For detailed configuration, troubleshooting, architecture details, and advanced usage, see the [ampsync README](../crates/bin/ampsync/README.md).

## See Also

- [Glossary](glossary.md) - Key terminology and architecture component definitions
- [Operational Modes](modes.md) - Detailed guide to `ampd` operational modes and deployment patterns
- [Configuration Guide](config.md) - Configuration options for all executables
- [User-Defined Functions](udfs.md) - Writing SQL queries with custom UDFs
- [Upgrading Guide](upgrading.md) - Upgrading Amp between versions
- [Telemetry Guide](telemetry.md) - Observability and monitoring
- [Ampsync README](../crates/bin/ampsync/README.md) - Detailed ampsync documentation

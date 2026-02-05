---
name: "rust-workspace"
description: "Workspace crate categories, dependency rules, member ordering. Load when creating crates, managing workspace structure, or reviewing dependency direction"
type: arch
scope: "global"
---

# Rust Workspace Patterns

**MANDATORY for ALL workspace-level organization in the Amp project**

## Table of Contents

1. [Crate Category Hierarchy](#1-crate-category-hierarchy)
2. [Dependency Rules per Category](#2-dependency-rules-per-category)
3. [Ordering Requirements](#3-ordering-requirements)
4. [Checklist](#checklist)

## 1. Crate Category Hierarchy

The Amp workspace follows a hierarchical organization under the `crates/` directory with distinct categories. Each category serves a specific architectural purpose.

### `crates/core/` - Foundation Libraries

**Purpose**: Shared functionality and domain logic used across the entire workspace.

**What belongs here:**

- **`common`**: Shared utilities, error types, configuration, EVM functions
- **`metadata-db`**: PostgreSQL database operations and worker coordination
- **`dataset-store`**: Dataset management, manifest parsing, SQL dataset support
- **`js-runtime`**: JavaScript UDF execution runtime
- **`dump`**: Core data extraction logic and parallel processing
- **`monitoring`**: Observability, metrics, and telemetry

### `crates/bin/` - Executable Applications

**Purpose**: Main entry points and command-line applications.

**What belongs here:**

- **`ampd`**: Main CLI application with subcommands (`dump`, `server`, `worker`)
- **`ampctl`**: Control plane CLI with commands for manifest generation (`manifest generate`) and registration (`manifest register`)
- **`ampsync`**: Syncing crate that streams data from Apache Arrow `RecordBatch` and inserts into a configured postgres database

### `crates/extractors/` - Data Source Connectors

**Purpose**: Specialized connectors for different blockchain data sources.

**What belongs here:**

- **`evm-rpc`**: Ethereum JSON-RPC data extraction (blocks, transactions, logs)
- **`firehose`**: StreamingFast Firehose protocol implementation

### `crates/services/` - Server Components

**Purpose**: HTTP/gRPC services and server-side functionality.

**What belongs here:**

- **`server`**: Arrow Flight gRPC server and JSON Lines HTTP server
- **`admin-api`**: Administrative HTTP API for management

### `crates/client/` - Client Libraries

Client libraries for connecting to Amp services.

- Language bindings and SDK functionality
- Can depend on `core` for shared data structures

### `tests/` - Integration Tests

Workspace-level integration tests.

- End-to-end testing across multiple crates
- Cross-crate integration scenarios

## 2. Dependency Rules per Category

| From \ To        | `core` | `bin` | `extractors` | `services` | `client` |
|------------------|:------:|:-----:|:------------:|:----------:|:--------:|
| **`core`**       | ✅     | ❌    | ❌           | ❌         | ❌       |
| **`bin`**        | ✅     | ❌    | ✅           | ✅         | ❌       |
| **`extractors`** | ✅     | ❌    | ❌           | ❌         | ❌       |
| **`services`**   | ✅     | ❌    | ❌           | ❌         | ❌       |
| **`client`**     | ✅     | ❌    | ❌           | ❌         | ❌       |

**Key rules:**

- **Core crates** can depend on other core crates. They NEVER depend on `bin`, `extractors`, or `services`
- **Bin crates** can depend on `core`, `extractors`, and `services`. No other category should depend on `bin` crates
- **Extractor crates** can depend on `core` only. Each extractor should be self-contained and independent from other extractors
- **Service crates** can depend on `core` only. They orchestrate core functionality for external interfaces
- **Client crates** can depend on `core` for shared data structures

## 3. Ordering Requirements

### Workspace Members

The root `Cargo.toml` `members` array MUST be ordered alphabetically.

**Rationale**: Ensures consistent merge conflict resolution and predictable workspace member listing.

### Dependencies in Cargo.toml

All `Cargo.toml` dependency sections (`[dependencies]`, `[dev-dependencies]`, `[build-dependencies]`) MUST be ordered alphabetically.

**Rationale**: Ensures consistent merge conflict resolution and maintainable dependency management.

## References

- [rust-crate](rust-crate.md) - Related: Crate manifest conventions

## Checklist

Before committing workspace changes, verify:

- [ ] New crates are placed in the correct category directory
- [ ] Dependency direction follows the rules (no upward or lateral violations)
- [ ] Workspace `members` array is alphabetically ordered
- [ ] All `Cargo.toml` dependency sections are alphabetically ordered
- [ ] Extractors remain independent from each other
- [ ] Core crates have no dependencies on bin, extractors, or services

---
name: "datasets-raw"
description: "Raw dataset schema definitions, block-based table model, BlockStreamer trait, retry strategy, and shared EVM types. Load when asking about raw datasets, block streaming, raw table schemas, or the BlockStreamer interface"
type: feature
status: stable
components: "crate:datasets-raw"
---

# Raw Datasets

## Summary

Raw datasets describe blockchain data through block-indexed table schemas. Each raw dataset defines a set of tables with explicit Arrow schemas, a target network, and a starting block number. The `datasets-raw` crate also defines the `BlockStreamer` trait — the interface that extractors implement to populate raw tables — along with retry logic, error classification, and shared EVM type definitions.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Architecture](#architecture)
3. [Usage](#usage)
4. [Implementation](#implementation)
5. [References](#references)

## Key Concepts

- **Block-Based Model**: Tables are indexed by block number. Each block produces rows across one or more tables, tagged with a `_block_num` column
- **Recoverable vs Fatal Errors**: Transient failures (network timeouts, rate limits) are recoverable and retried automatically. Permanent failures (invalid data, protocol errors) are fatal and abort the stream
- **Retry with Backoff**: Recoverable errors trigger progressive backoff. After 8 retries, warnings are logged; after 16, errors are logged. Retries continue indefinitely until the error becomes fatal or succeeds
- **Bucket Size**: An optional hint for parallel streaming — the provider can process multiple block ranges concurrently when a bucket size is specified

## Architecture

The raw dataset crate defines both the schema layer and the streaming interface:

```
Raw Dataset (schema definition)
  ├── Tables + Arrow Schemas (what the data looks like)
  └── BlockStreamer trait (how extractors populate the tables)
         ↓
    BlockStreamerWithRetry (auto-retry on recoverable errors)
         ↓
    Rows → TableRows → Arrow RecordBatch
```

### Schema and Streaming

1. A raw dataset manifest specifies `kind`, `network`, and `start_block`
2. The dataset exposes its table schemas via the `Dataset` trait
3. Extractors implement the `BlockStreamer` trait to populate tables
4. Each block produces a `Rows` value containing `TableRows` for each table
5. Each `TableRows` holds an Arrow `RecordBatch` conforming to the table's declared schema
6. All record batches include a `_block_num` column (reserved column name)

### Table Invariants

Every `TableRows` value is validated against these invariants:
- The `_block_num` column exists and matches the expected block range
- The record batch schema matches the table's declared Arrow schema
- Block ranges are consistent across all tables in a single `Rows`

## Usage

### Raw Dataset Manifest Fields

Raw dataset manifests extend the common manifest with:

| Field | Type | Description |
|-------|------|-------------|
| `kind` | string | Raw kind identifier (`evm-rpc`, `firehose`, `solana`) |
| `network` | string | Target blockchain network (e.g., `mainnet`, `base`) |
| `start_block` | u64 | First block number to extract |
| `finalized_blocks_only` | bool | Whether to restrict to finalized blocks |
| `tables` | object | Map of table name to table schema |

### Network Identifier

Every raw dataset table includes a `network` field identifying the blockchain network. The `NetworkId` type enforces that identifiers are non-empty strings. Values should follow [The Graph's networks registry](https://github.com/graphprotocol/networks-registry/blob/main/docs/networks-table.md) (e.g., `mainnet`, `base`, `arbitrum-one`, `solana-mainnet`).

The network identifier appears both at the manifest level and on each individual table definition.

### Shared EVM Types

The `datasets-raw` crate provides shared EVM table definitions used by both the EVM RPC and Firehose extractors:

- **blocks** — Block headers (number, hash, timestamp, gas, etc.)
- **logs** — Event logs (address, topics, data, log index)

These shared definitions ensure schema consistency across EVM-compatible extractors.

## Implementation

### Source Files

- `crates/core/datasets-raw/src/client.rs` — `BlockStreamer` trait, `BlockStreamError`, `BlockStreamerWithRetry` retry wrapper
- `crates/core/datasets-raw/src/rows.rs` — `Rows`, `TableRows` types with invariant validation
- `crates/core/datasets-raw/src/dataset.rs` — `Dataset` and `Table` concrete implementations
- `crates/core/datasets-raw/src/manifest.rs` — `RawDatasetManifest` and raw-specific `Table` types
- `crates/core/datasets-raw/src/evm.rs` — Shared EVM types (blocks, logs table schemas)

## References

- [datasets](datasets.md) - Base: Dataset system overview
- [datasets-manifest](datasets-manifest.md) - Related: Manifest format and schema definitions

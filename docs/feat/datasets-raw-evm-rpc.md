---
name: "datasets-raw-evm-rpc"
description: "EVM RPC raw dataset definition with blocks, transactions, and logs table schemas. Load when asking about EVM datasets, EVM table schemas, or evm-rpc manifest format"
type: feature
status: stable
components: "crate:evm-rpc-datasets"
---

# EVM RPC Dataset

## Summary

The EVM RPC dataset defines the table schemas for block, transaction, and event log data from EVM-compatible blockchains. It declares three tables — `blocks`, `transactions`, and `logs` — covering the core data model of Ethereum and compatible chains. The dataset uses the shared EVM block and log schemas from `datasets-raw`, with EVM RPC-specific transaction fields.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Configuration](#configuration)
3. [Usage](#usage)
4. [Implementation](#implementation)
5. [References](#references)

## Key Concepts

- **EVM RPC**: Standard Ethereum JSON-RPC API — this dataset kind defines schemas matching the JSON-RPC response format
- **Receipt Fields**: Transaction table schema includes receipt fields (gas used, status, logs) that come from transaction receipts
- **Shared EVM Schemas**: The `blocks` and `logs` table schemas are shared with the Firehose dataset, ensuring consistent column definitions across EVM-compatible dataset kinds

## Configuration

For the complete field reference, see the [manifest schema](../manifest-schemas/evm-rpc.spec.json).

### Example Manifest

```json
{
  "kind": "evm-rpc",
  "network": "mainnet",
  "start_block": 0,
  "finalized_blocks_only": false,
  "tables": {
    "blocks": { "schema": { "arrow": { "fields": [] } }, "network": "mainnet" },
    "transactions": { "schema": { "arrow": { "fields": [] } }, "network": "mainnet" },
    "logs": { "schema": { "arrow": { "fields": [] } }, "network": "mainnet" }
  }
}
```

### Manifest Fields

| Field | Type | Description |
|-------|------|-------------|
| `kind` | `"evm-rpc"` | Dataset kind identifier |
| `network` | string | Target EVM network (e.g., `mainnet`, `base`, `polygon`) |
| `start_block` | u64 | First block number for data |
| `finalized_blocks_only` | bool | Restrict to finalized blocks |
| `tables` | object | Table definitions with schemas |

## Usage

This dataset declares three tables: `blocks`, `transactions`, and `logs`. For detailed column definitions, see the [table schema](../schemas/evm-rpc.md).

The `blocks` and `logs` tables use shared EVM schema definitions from `datasets-raw::evm`, while `transactions` uses a schema specific to the JSON-RPC response format (including receipt fields like gas used and status).

## Implementation

### Source Files

- `crates/extractors/evm-rpc/src/lib.rs` — `Manifest`, `dataset()` factory, re-exports
- `crates/extractors/evm-rpc/src/tables.rs` — `all()` function returning blocks, transactions, logs tables
- `crates/core/datasets-raw/src/evm.rs` — Shared EVM blocks and logs schemas

## References

- [datasets](datasets.md) - Base: Dataset system overview
- [datasets-raw](datasets-raw.md) - Base: Raw dataset architecture
- [datasets-manifest](datasets-manifest.md) - Related: Manifest format
- [provider-evm-rpc](provider-evm-rpc.md) - Related: JSON-RPC provider configuration

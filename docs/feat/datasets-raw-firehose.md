---
name: "datasets-raw-firehose"
description: "Firehose raw dataset definition with blocks, transactions, calls, and logs tables. Load when asking about Firehose datasets, call traces, or firehose manifest format"
type: feature
status: stable
components: "crate:firehose-datasets"
---

# Firehose Dataset

## Summary

The Firehose dataset defines table schemas for EVM-compatible blockchain data as provided by the StreamingFast Firehose gRPC protocol. It declares four tables — `blocks`, `transactions`, `calls`, and `logs` — covering the same core EVM data as the EVM RPC dataset plus detailed call traces. The `calls` table is the key differentiator, describing internal contract call data that is not available through standard JSON-RPC.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Manifest](#manifest)
3. [Schema](#schema)
4. [Implementation](#implementation)
5. [References](#references)

## Key Concepts

- **Firehose Protocol**: StreamingFast's high-throughput gRPC streaming protocol for blockchain data, providing richer data than standard JSON-RPC
- **Call Traces**: The `calls` table schema describes internal contract-to-contract calls within a transaction, including input/output data, gas usage, and call type (call, delegatecall, staticcall, create)
- **Shared EVM Schemas**: The `blocks` and `logs` tables use the same shared schemas as the EVM RPC dataset, ensuring consistent column definitions across EVM-compatible dataset kinds

## Manifest

See the [raw dataset manifest schema](../manifest-schemas/raw.spec.json) for the complete field reference, types, defaults, and examples.

## Schema

This dataset declares four tables: `blocks`, `transactions`, `calls`, and `logs`.

For detailed column definitions, see the [table schema](../schemas/firehose-evm.md).

## Implementation

### Source Files

- `crates/extractors/firehose/src/lib.rs` — `Manifest`, `dataset()` factory, re-exports
- `crates/extractors/firehose/src/tables.rs` — `all()` function returning blocks, transactions, calls, logs tables
- `crates/core/datasets-raw/src/evm.rs` — Shared EVM blocks and logs schemas

## References

- [datasets](datasets.md) - Base: Dataset system overview
- [datasets-raw](datasets-raw.md) - Base: Raw dataset architecture
- [datasets-manifest](datasets-manifest.md) - Related: Manifest format
- [provider-firehose](provider-firehose.md) - Related: Firehose provider configuration

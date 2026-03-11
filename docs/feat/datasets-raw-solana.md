---
name: "datasets-raw-solana"
description: "Solana raw dataset definition with block headers, transactions, messages, instructions, and block rewards tables. Load when asking about Solana datasets, slot-based data, or solana manifest format"
type: feature
status: experimental
components: "crate:solana-datasets"
---

# Solana Dataset

## Summary

The Solana dataset defines table schemas for Solana blockchain data. It declares five tables — `block_headers`, `transactions`, `messages`, `instructions`, and `block_rewards` — covering Solana's account-based transaction model. Solana slots are mapped to block numbers, with skipped slots producing no rows and creating natural gaps in the block number sequence.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Manifest](#manifest)
3. [Schema](#schema)
4. [Implementation](#implementation)
5. [Limitations](#limitations)
6. [References](#references)

## Key Concepts

- **Slot**: Solana's equivalent of a block number. Each slot may contain a confirmed block or be skipped by the leader
- **Skipped Slots**: Slots where no block was produced. These create gaps in the `_block_num` sequence — the extractor produces no rows for skipped slots
- **Commitment Level**: Solana's finality model (`processed`, `confirmed`, `finalized`). The `finalized_blocks_only` flag maps to the `finalized` commitment level
- **Non-Empty Slot**: A confirmed slot containing block data, transactions, and rewards. Represented by the `NonEmptySlot` type in the extractor

## Manifest

See the [raw dataset manifest schema](../manifest-schemas/raw.spec.json) for the complete field reference, types, defaults, and examples.

## Schema

This dataset declares five tables: `block_headers`, `transactions`, `messages`, `instructions`, and `block_rewards`. 

For detailed column definitions, see the [table schema](../schemas/solana.md).

### Slot-to-Block Mapping

Solana slots are treated as `BlockNum` values throughout the system. When a slot is skipped (no block produced), the extractor emits no rows for that slot. Downstream consumers should expect non-contiguous `_block_num` values. Chain integrity is maintained through blockhash/previous-blockhash validation rather than sequential slot numbering.

## Implementation

### Source Files

- `crates/extractors/solana/src/lib.rs` — `Manifest`, `dataset()` and `client()` factories, commitment config mapping
- `crates/extractors/solana/src/tables.rs` — `NonEmptySlot`, `all()` function, `into_db_rows()` conversion
- `crates/extractors/solana/src/tables/block_headers.rs` — Block header table schema
- `crates/extractors/solana/src/tables/transactions.rs` — Transaction table schema
- `crates/extractors/solana/src/tables/messages.rs` — Message table schema
- `crates/extractors/solana/src/tables/instructions.rs` — Instruction table schema
- `crates/extractors/solana/src/tables/block_rewards.rs` — Block rewards table schema

## Limitations

- Skipped slots create gaps in `_block_num` sequences, which may affect range-based queries
- Log messages may be truncated by the RPC endpoint (indicated by `TRUNCATED_LOG_MESSAGES_MARKER`)
- Commitment level mapping depends on the Solana RPC endpoint's support

## References

- [datasets](datasets.md) - Base: Dataset system overview
- [datasets-raw](datasets-raw.md) - Base: Raw dataset architecture
- [datasets-manifest](datasets-manifest.md) - Related: Manifest format
- [provider-solana](provider-solana.md) - Related: Solana provider configuration

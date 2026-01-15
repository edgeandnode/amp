---
name: "query-sql-streaming-joins"
description: "Incremental SQL JOIN execution in streaming queries. Load when asking about streaming joins, incremental joins, or real-time join updates"
status: "experimental"
components: "service:server,crate:common,crate:dump"
---

# Streaming SQL Joins

## Summary

Streaming SQL joins enable real-time, incremental JOIN operations on blockchain data. As new blocks arrive, join results are computed incrementally without re-processing historical data. This is achieved through an incremental view maintenance algorithm that decomposes each [microbatch](query-sql-streaming.md#key-concepts) into three terms: new-left with historical-right, historical-left with new-right, and new-left with new-right.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Architecture](#architecture)
3. [Usage](#usage)
4. [Implementation](#implementation)
5. [Limitations](#limitations)
6. [References](#references)

## Key Concepts

- **Incremental Join**: A join computation method that processes only new data (delta) against historical data, avoiding full recomputation on each update

For general streaming concepts (microbatch, delta, history, block range), see [Streaming SQL Queries](query-sql-streaming.md#key-concepts).

## Architecture

### Incremental Join Algorithm

The core algorithm implements the inner join update rule from incremental view maintenance theory:

```
Δ(L⋈R) = (ΔL ⋈ R[t-1]) ∪ (L[t-1] ⋈ ΔR) ∪ (ΔL ⋈ ΔR)
```

Where:
- `Δ` = Delta/new data in current microbatch
- `[t-1]` = Historical data (before current microbatch)
- `L` = Left side of join
- `R` = Right side of join

The three terms represent:
1. **Term 1**: New left data joined with historical right data
2. **Term 2**: Historical left data joined with new right data
3. **Term 3**: New left data joined with new right data

These terms are unioned to produce the complete incremental result for each microbatch.

### Block Number Propagation

The internal `_block_num` column tracks which block each row belongs to. For joins, the output `_block_num` is computed as:

```sql
GREATEST(left._block_num, right._block_num)
```

This ensures correct ordering when rows from different blocks are joined.

## Usage

### Basic Streaming Join

Join blocks with their parent blocks:

```sql
SELECT child.block_num, parent.block_num as parent_num
FROM eth_rpc.blocks child
JOIN eth_rpc.blocks parent ON child.parent_hash = parent.hash
SETTINGS stream = true
```

### Cross-Table Join

Join blocks with their transactions:

```sql
SELECT b.block_num, t.tx_hash, t.tx_index
FROM eth_rpc.blocks b
INNER JOIN eth_rpc.transactions t ON b.block_num = t.block_num
SETTINGS stream = true
```

### Supported Join Types

| Join Type | Supported | Notes |
|-----------|-----------|-------|
| INNER JOIN | Yes | Fully incremental |
| LEFT SEMI | Yes | Treated as filtered inner join |
| RIGHT SEMI | Yes | Treated as filtered inner join |
| LEFT OUTER | No | Requires state for unmatched rows |
| RIGHT OUTER | No | Requires state for unmatched rows |
| FULL OUTER | No | Requires state for unmatched rows |
| LEFT ANTI | No | Requires state for non-matches |
| RIGHT ANTI | No | Requires state for non-matches |

## Implementation

### Source Files

- `crates/core/common/src/incrementalizer.rs` - Implements the incremental join algorithm and SQL transformation logic

## Limitations

### Unsupported Join Types

Outer and anti joins are not supported because they require maintaining state about unmatched rows across microbatches. The incremental algorithm only works for joins where output rows are produced immediately when matching rows are found.

For general streaming limitations (aggregates, DISTINCT, LIMIT, etc.), see [Streaming SQL Queries](query-sql-streaming.md#limitations).

## References

- [query-sql-streaming](query-sql-streaming.md) - Base: Streaming query fundamentals
- [query-sql-batch](query-sql-batch.md) - Alternative: One-shot batch query execution

---
name: "query-sql-streaming"
description: "Real-time SQL query execution with microbatch processing and block-based streaming. Load when asking about streaming queries, SETTINGS stream, microbatches, or real-time data"
type: feature
components: "service:server,crate:common,crate:dump"
---

# Streaming SQL Queries

## Summary

Streaming SQL queries enable real-time, continuous data delivery as new blockchain blocks arrive. Unlike batch queries that execute once and return, streaming queries run in a loop, processing data in discrete microbatches bounded by block ranges. Results are delivered incrementally via Arrow Flight, with support for resumption after disconnection and automatic handling of blockchain reorganizations.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Architecture](#architecture)
3. [Configuration](#configuration)
4. [Usage](#usage)
5. [Limitations](#limitations)
6. [Implementation](#implementation)
7. [References](#references)

## Key Concepts

- **Streaming Query**: A query that runs continuously, emitting results as new blocks arrive rather than executing once
- **Microbatch**: A discrete unit of streaming execution bounded by a block range; each microbatch processes blocks from `start` to `end`
- **Block Range**: The `[start, end]` interval of block numbers processed in a single microbatch, including the network, end block hash, and previous hash for chain validation
- **Delta**: New data arriving in the current microbatch (blocks in range `[start, end]`)
- **History**: Previously processed data (blocks before the current microbatch's `start`)
- **Watermark**: A checkpoint containing the last processed block number and hash, used for resumption and reorg detection
- **Incrementalizable**: A query plan property indicating it can be executed in microbatches without maintaining cross-batch state

## Architecture

### Execution Flow

1. **Query Submission**: Client submits SQL with `SETTINGS stream = true` via Arrow Flight
2. **Validation**: Plan is checked for incrementalizability (all operations must support microbatch execution)
3. **Initialization**: Streaming task spawns, sets up table update subscriptions
4. **Microbatch Loop**:
   - Wait for table updates (new blocks)
   - Calculate next microbatch range
   - Transform plan with block range constraints
   - Execute and stream results ordered by `_block_num`
   - Update watermark
5. **Repeat** until `end_block` reached or error occurs

### Message Protocol

The streaming query emits four types of messages:

| Message | Description |
|---------|-------------|
| `MicrobatchStart` | Signals start of a microbatch with block range and reorg flag |
| `Data` | Arrow RecordBatch containing query results |
| `BlockComplete` | Emitted when all data for a specific block has been sent |
| `MicrobatchEnd` | Signals completion of current microbatch |

**Message Sequence**:
```
MicrobatchStart { range: 100..=102, is_reorg: false }
  Data(batch_1)
  BlockComplete(100)
  Data(batch_2)
  BlockComplete(101)
  BlockComplete(102)
MicrobatchEnd { range: 100..=102 }
```

### Resume and Watermarks

Clients can resume a stream after disconnection:

1. Save the watermark from the last `MicrobatchEnd`
2. Include watermark in next request via `amp-resume` header
3. Stream continues from `watermark.block_number + 1`
4. If watermark hash doesn't match canonical chain, reorg recovery initiates

### Reorg Handling

When a blockchain reorganization occurs:

1. **Detection**: Previous watermark hash not found on canonical chain
2. **Recovery**: Walk backwards to find common ancestor block
3. **Signal**: Next microbatch has `is_reorg: true` flag
4. **Recompute**: Results from reorg base are recomputed with new canonical data

Clients receiving `is_reorg: true` should invalidate cached data from affected blocks.

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `server_microbatch_max_interval` | - | Maximum blocks per microbatch |
| `keep_alive_interval` | - | Seconds between keep-alive messages (minimum 30s) |

### Keep-Alive

During periods with sparse data, the server emits empty record batches at the configured interval to prevent client timeouts.

## Usage

### Basic Streaming Query

Enable streaming with the `SETTINGS` clause:

```sql
SELECT block_num, hash, timestamp
FROM eth_rpc.blocks
SETTINGS stream = true
```

### Arrow Flight Request

Via Arrow Flight with headers:

```
amp-stream: true       -- Enable streaming (alternative to SETTINGS)
amp-resume: {...}      -- Resume from watermark (JSON)
```

### Streaming vs Batch

| Aspect | Streaming | Batch |
|--------|-----------|-------|
| Activation | `SETTINGS stream = true` | Default |
| Execution | Continuous loop | Single execution |
| Operations | Incrementalizable only | Any SQL |
| State | Tracks watermark | Stateless |

## Limitations

Streaming queries require all operations to be incrementalizable. The following operations prevent incrementalization:

- **Aggregate functions** - Require maintaining running state across batches
- **DISTINCT** - Requires global deduplication state
- **LIMIT** - Requires counting across batches
- **ORDER BY** (global) - Requires seeing all data before sorting
- **Window functions** - Often require state and sorting
- **Recursive queries** - Inherently stateful

Non-incrementalizable queries will be rejected with a `NonIncrementalOp` error.

## Implementation

### Source Files

- `crates/core/dump/src/streaming_query.rs` - Streaming query engine
- `crates/core/common/src/stream_helpers.rs` - Stream detection
- `crates/core/common/src/plan_visitors.rs` - Plan validation
- `crates/services/server/src/flight.rs` - Arrow Flight integration

## References

- [query-sql-batch](query-sql-batch.md) - Alternative: One-shot batch query execution

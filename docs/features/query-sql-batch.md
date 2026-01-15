---
name: "query-sql-batch"
description: "One-shot SQL query execution that runs to completion before returning results. Load when asking about batch queries, one-shot queries, or non-streaming execution"
components: "service:server,crate:common,crate:dump"
---

# Batch SQL Queries

## Summary

Batch SQL queries execute once against a snapshot of the data and return the complete result set. Unlike streaming queries that continuously emit results as new blocks arrive, batch queries run to completion before returning. This is the default execution mode and supports the full range of SQL operations including aggregations, sorting, and limits.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Architecture](#architecture)
3. [Usage](#usage)
4. [Implementation](#implementation)
5. [Limitations](#limitations)
6. [References](#references)

## Key Concepts

- **Batch Query**: A one-shot query that executes against current data and returns a complete result set
- **Snapshot Execution**: Query runs against data available at execution time; no updates after query starts
- **Full SQL Support**: All SQL operations available (aggregates, DISTINCT, LIMIT, ORDER BY, window functions)

## Architecture

### Execution Model

Batch queries differ from streaming queries in several ways:

| Aspect | Batch | Streaming |
|--------|-------|-----------|
| Execution | Once, to completion | Continuous loop |
| Results | Complete set returned | Incremental updates |
| SQL Support | Full (any operation) | Limited (incrementalizable only) |
| State | Stateless | Tracks watermarks |
| Use Case | Ad-hoc queries, reports | Real-time feeds |

### Execution Flow

1. Client submits SQL query (without `SETTINGS stream = true`)
2. Query is parsed and planned via DataFusion
3. Plan executes against registered datasets
4. Results are collected and returned in requested format
5. Connection closes after response

## Usage

### Basic Query

Batch queries are the default - simply omit the streaming setting:

```sql
SELECT block_num, hash, timestamp
FROM eth_rpc.blocks
WHERE block_num > 1000000
LIMIT 100
```

### Aggregations

Batch queries support aggregations (not available in streaming):

```sql
SELECT
    DATE_TRUNC('day', timestamp) as day,
    COUNT(*) as block_count,
    AVG(gas_used) as avg_gas
FROM eth_rpc.blocks
GROUP BY DATE_TRUNC('day', timestamp)
ORDER BY day DESC
```

### Window Functions

Window functions are supported in batch mode:

```sql
SELECT
    block_num,
    gas_used,
    AVG(gas_used) OVER (ORDER BY block_num ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) as rolling_avg
FROM eth_rpc.blocks
```

## Implementation

### Source Files

- `crates/core/dump/src/query.rs` - Core query execution logic for batch queries
- `crates/core/common/src/datafusion/` - DataFusion integration and SQL planning

## Limitations

- **No real-time updates**: Results reflect data at query time only
- **Memory constraints**: Large result sets may require pagination or limits
- **No resumption**: If connection drops, query must be re-executed from start

## References

- [query-sql-streaming](query-sql-streaming.md) - Alternative: Real-time streaming queries
- [query-transport-jsonl](query-transport-jsonl.md) - Transport: HTTP/JSON Lines interface
- [query-transport-flight](query-transport-flight.md) - Transport: Arrow Flight RPC interface

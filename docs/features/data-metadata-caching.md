---
name: "data-metadata-caching"
description: "Parquet metadata caching with memory-weighted eviction, database persistence, and DataFusion integration. Load when working with footer caching, statistics computation, or query planning optimization"
type: "feature"
components: "crate:data-store,crate:metadata-db,crate:common"
---

# Data Metadata Caching

## Summary

The metadata caching system provides fast access to parquet file metadata without touching object storage on the query hot path.
It uses a two-tier architecture: raw footer bytes are persisted in PostgreSQL during file registration, while parsed metadata and computed statistics are cached in memory with memory-weighted eviction.
The cache integrates directly with DataFusion's query planning, enabling efficient file pruning and statistics-based optimization.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Architecture](#architecture)
3. [Usage](#usage)
4. [Cache Lifecycle](#cache-lifecycle)
5. [DataFusion Integration](#datafusion-integration)
6. [Footer Population](#footer-population)
7. [Performance Characteristics](#performance-characteristics)
8. [Implementation](#implementation)
9. [References](#references)

## Key Concepts

- **Parquet Footer**: The metadata section at the end of a parquet file containing schema, row group information, and column statistics. Can be megabytes for wide tables with many columns.
- **Footer Cache**: Database table storing raw footer bytes, enabling cache rebuilds without reading from object storage.
- **Memory-Weighted Eviction**: Cache eviction policy based on actual memory footprint of parsed metadata rather than entry count, preventing large footers from monopolizing cache space.
- **Cached Parquet Data**: The combination of parsed parquet metadata and computed statistics, stored together in the in-memory cache keyed by file ID.

## Architecture

### Why Caching Matters

Querying object storage directly is expensive for two reasons:

1. **Network latency**: List operations and file reads to S3/GCS have high latency compared to local database queries
2. **CPU overhead**: Parquet footer parsing is CPU-intensive, especially for wide tables where footers can be megabytes

Without caching, every query would need to:

- Read footer bytes from object storage for each file
- Parse the binary footer into metadata structures
- Compute statistics for query optimization

The caching system eliminates these costs on the query hot path by persisting footer bytes locally and caching parsed results in memory.

### Two-Tier Design

```
┌──────────────────────────────────────────────────────────────────┐
│                     Query Planning Request                       │
└──────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌──────────────────────────────────────────────────────────────────┐
│  TIER 2: In-Memory Cache                                         │
│                                                                  │
│  Stores parsed metadata and computed statistics                  │
│  Uses memory-weighted LRU eviction                               │
└──────────────────────────────────────────────────────────────────┘
                                │ cache miss
                                ▼
┌──────────────────────────────────────────────────────────────────┐
│  TIER 1: Database                                                │
│                                                                  │
│  Stores raw footer bytes                                         │
│  Persists across restarts                                        │
└──────────────────────────────────────────────────────────────────┘
                                │ fetch bytes
                                ▼
┌──────────────────────────────────────────────────────────────────┐
│  Parse & Compute                                                 │
│                                                                  │
│  Parse footer, compute statistics, store in memory cache         │
└──────────────────────────────────────────────────────────────────┘
```

**Tier 1: Database persistence.**
Raw footer bytes are stored in PostgreSQL during file registration.
This eliminates the need to read footers from remote object storage—footer bytes are available locally via fast database queries.

**Tier 2: In-memory caching.**
Parsed parquet metadata and computed statistics are cached in memory with memory-weighted eviction.
On cache miss, footer bytes are fetched from the database (not object storage), parsed, and statistics are computed before caching.

### Data Flow Overview

```
        WRITE PATH                                   READ PATH
        ──────────                                   ─────────

    ┌─────────────────┐                         ┌─────────────────┐
    │  Parquet File   │                         │  Query Request  │
    │     Written     │                         │                 │
    └────────┬────────┘                         └────────┬────────┘
             │                                           │
             ▼                                           ▼
    ┌─────────────────┐                         ┌─────────────────┐
    │ Extract footer  │                         │ Request file    │
    │ bytes from file │                         │    metadata     │
    └────────┬────────┘                         └────────┬────────┘
             │                                           │
             ▼                                           ▼
    ┌─────────────────┐                         ┌─────────────────┐
    │ Store footer in │                         │  Check memory   │
    │    database     │                         │     cache       │
    └─────────────────┘                         └────────┬────────┘
                                                         │
                                          ┌──────────────┴──────────────┐
                                          │                             │
                                    hit   │                             │  miss
                                          ▼                             ▼
                                 ┌─────────────────┐          ┌─────────────────┐
                                 │ Return cached   │          │ Fetch from DB   │
                                 │    metadata     │          └────────┬────────┘
                                 └─────────────────┘                   │
                                                                       ▼
                                                              ┌─────────────────┐
                                                              │  Parse footer,  │
                                                              │ compute stats,  │
                                                              │  store in cache │
                                                              └────────┬────────┘
                                                                       │
                                                                       ▼
                                                              ┌─────────────────┐
                                                              │ Return metadata │
                                                              └─────────────────┘
```


## Usage

### Configuring the Cache

The metadata cache is configured through the `[writer]` section of the configuration file:

| Config Location    | Parameter       | Default | Description                                   |
|--------------------|-----------------|---------|-----------------------------------------------|
| `[writer]` section | `cache_size_mb` | `1024`  | Memory budget for parsed metadata cache in MB |

Example configuration:

```toml
[writer]
cache_size_mb = 1024  # Memory budget for parsed metadata cache (default: 1024 MB)
```

The `cache_size_mb` parameter controls the memory budget for the in-memory cache. The cache uses memory-weighted eviction, so the actual number of entries varies based on footer sizes.

### Sizing Guidelines

| Deployment Size    | Suggested Cache Size | Rationale                                     |
|--------------------|----------------------|-----------------------------------------------|
| Development        | 256-512 MB           | Few tables, limited concurrent queries        |
| Small Production   | 1024 MB (default)    | Handles typical workloads with good hit rates |
| Large Production   | 2048-4096 MB         | Many tables, wide schemas, high concurrency   |
| Memory-Constrained | 128-256 MB           | Accept higher miss rate for memory savings    |

**Factors affecting cache sizing:**

- **Table width**: Wide tables (100+ columns) have larger footers
- **Number of files**: More files require more cache entries
- **Query patterns**: Repeated queries to same files benefit from larger cache
- **Concurrent queries**: Multiple queries accessing different files increase working set

### Memory Accounting

The cache tracks actual memory consumption, not entry count. Each cache entry is weighted by the memory size of its parsed parquet metadata. This ensures that large footers (from wide tables with many columns) consume proportionally more of the cache budget, preventing a few large entries from evicting many smaller ones.

## Cache Lifecycle

### Initialization

The cache is initialized at startup with a configured memory budget. The cache starts cold—entries are populated on-demand as queries request file metadata.

### Population Strategies

**On-Demand Population (Default):** Cache entries are created lazily during query execution. The first query touching a file incurs the database fetch and parsing cost, and subsequent queries benefit from the cache hit.

**Write-Time Registration:** Footer bytes are captured at write time and stored in the database. This ensures the database always has fresh footer bytes available, but does not pre-populate the in-memory cache. The in-memory cache is populated only when queries request the metadata.

### Cache Warm-Up

There is no explicit cache warm-up mechanism. The cache warms naturally as queries execute:

1. First query for a file: cache miss → database fetch → parse → cache → return
2. Subsequent queries for same file: cache hit → return immediately

For workloads with predictable file access patterns, the cache reaches steady-state quickly. For highly variable workloads, consider increasing `cache_size_mb` to reduce eviction pressure.

### Cache Invalidation

Cache entries are keyed by file ID, which is immutable. Files are never modified in place—new data creates new files with new IDs. This means:

- **No explicit invalidation needed**: Old file IDs naturally become unreferenced
- **Compaction creates new files**: Compacted files get new IDs, old IDs are orphaned
- **Garbage collection**: When files are deleted, their cache entries may persist until evicted, but will never be accessed

This immutable-file design eliminates cache coherence problems entirely.

## DataFusion Integration

The caching system integrates with DataFusion's query planning and execution. When DataFusion needs file metadata, the system intercepts the request and serves it from cache instead of reading from object storage.

This design means:

- **Metadata reads**: Served from cache (fast, no network I/O)
- **Data reads**: Still go to object storage (necessary for actual row data)

### Statistics for Query Optimization

Cached statistics enable DataFusion's query optimizer to:

1. **Predicate pushdown**: Skip files/row groups where predicates cannot match
2. **Partition pruning**: Eliminate partitions based on statistics
3. **Cost estimation**: Better query plans through accurate row count estimates

Statistics include:

- Row counts (total and per-column null counts)
- Min/max values for columns (when available)
- Distinct value estimates

## Footer Population

Footer bytes are captured during file registration, not during query execution. This happens in the write path:

### Write-Time Footer Extraction

When a parquet file is written and committed, the following steps occur:

1. **Write file**: The parquet file is flushed to object storage
2. **Get object metadata**: Object metadata (size, etag, version) is retrieved
3. **Extract footer**: Footer bytes are read from the newly written file
4. **Register in database**: File metadata and footer bytes are stored in the database

### Footer Extraction Process

The footer extraction reads the parquet metadata from the file, then serializes it back to bytes for database storage. This approach ensures footer bytes are captured once at write time and never need to be re-read from object storage.

## Performance Characteristics

### Latency Comparison

| Operation              | Typical Latency | Notes                                            |
|------------------------|-----------------|--------------------------------------------------|
| Cache hit              | < 1 μs          | Memory access                                    |
| Cache miss (DB fetch)  | 1-10 ms         | Database round-trip, depends on footer size      |
| Object storage read    | 50-500 ms       | Network I/O, varies by cloud provider and region |
| Footer parsing         | 0.1-10 ms       | CPU-bound, scales with footer size               |
| Statistics computation | 0.1-1 ms        | CPU-bound, scales with column count              |

### Cache Hit Rate Expectations

| Workload Pattern            | Expected Hit Rate | Notes                          |
|-----------------------------|-------------------|--------------------------------|
| Repeated analytical queries | 95%+              | Same files accessed repeatedly |
| Dashboard/reporting         | 80-95%            | Regular access patterns        |
| Ad-hoc exploration          | 50-80%            | Variable file access           |
| Bulk data processing        | Variable          | Depends on data locality       |

### Memory Efficiency

The memory-weighted eviction provides efficient cache utilization:

- Small footers (narrow tables): Many entries fit in cache
- Large footers (wide tables): Fewer entries, but proportionally weighted
- No fixed entry limit: Memory budget drives capacity

### Concurrency

The cache is thread-safe and lock-free for reads:

- Multiple queries can read cached metadata concurrently
- Cache misses are handled with deduplication (multiple callers waiting for same key get same result)
- No blocking between readers and writers

## Implementation

### Source Files

- `crates/core/data-store/src/lib.rs` - Cache initialization, `CachedParquetData` struct, `get_cached_parquet_metadata()`
- `crates/core/common/src/catalog/physical/reader.rs` - `AmpReaderFactory`, `AmpReader` (DataFusion integration)
- `crates/core/common/src/metadata.rs` - Footer extraction during writes
- `crates/core/metadata-db/src/files.rs` - Database operations for footer retrieval
- `crates/core/metadata-db/src/files/sql.rs` - SQL queries for `footer_cache` table
- `crates/core/metadata-db/migrations/20251204183330_footer_cache.sql` - Database migration
- `crates/core/dump/src/parquet_writer.rs` - Footer capture during file commit
- `crates/core/dump/src/config.rs` - `cache_size_mb` configuration

## References

- [data](data.md) - Parent: Data lake architecture overview

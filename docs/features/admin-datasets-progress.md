---
name: "admin-datasets-progress"
description: "Dataset progress API for monitoring sync state, block ranges, and job health. Load when asking about dataset freshness, sync status, or progress endpoints"
components: "crate:amp-data-store,crate:admin-api,crate:metadata-db"
---

# Dataset Progress API

## Summary

The Dataset Progress API provides visibility into the operational state of datasets, reporting sync metrics like `start_block`, `current_block`, job health status, and file statistics. This API serves as the "ground truth" for the engine's state, which Platform services can use to calculate higher-level metrics like freshness or block lag.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Architecture](#architecture)
3. [API Reference](#api-reference)
4. [Usage](#usage)
5. [Implementation](#implementation)
6. [Limitations](#limitations)

## Key Concepts

- **Progress**: The current state of data synchronization for a dataset, including the range of blocks that have been synced and the number of files produced
- **Current Block**: The highest block number that has been synced (end of the synced range)
- **Start Block**: The lowest block number that has been synced (beginning of the synced range)
- **Job Status**: The health state of the writer job (e.g., `RUNNING`, `FAILED`, `COMPLETED`)
- **Pull Model**: The current data strategy where progress is calculated on-demand from table snapshots rather than persisted separately

## Architecture

### Data Strategy

The API uses a **Pull Model** where progress is calculated on-demand:

1. **No new infrastructure** - Avoids introducing Kafka or message broker dependencies
2. **Leverages existing caches** - Postgres metadata and Foyer caches already exist
3. **Simpler operational model** - No event consumers to deploy and monitor

A **Push Model** (event-driven via Kafka) may be considered in the future for real-time dashboards or webhook notifications.

### Logic Location & Ownership

| Component       | Responsibility                                                      |
| --------------- | ------------------------------------------------------------------- |
| **`DataStore`** | `get_table_progress()` - computes sync stats from table snapshot    |
| **`DataStore`** | `get_tables_writer_info()` - wraps metadata_db query for job info   |
| **Admin API**   | Orchestration - resolves dataset, iterates tables, combines results |
| **Admin API**   | Presentation - formats JSON response for REST clients               |
| **Gateway**     | Authentication - scopes access to dataset owners                    |

**Key Principle**: Admin API handlers do not interact with `metadata_db` directly. All database access is encapsulated in `DataStore` methods, keeping handlers as pure orchestration and presentation logic.

### Handler Flow

```
1. Extract path parameters → Reference
2. Resolve dataset revision via DatasetStore
3. Get dataset definition to enumerate tables
4. Call DataStore::get_tables_writer_info() for job info
5. For each table:
   a. Call DataStore::get_table_progress()
   b. Combine with writer info
6. Format and return JSON response
```

## API Reference

### Endpoints

| Endpoint                                                           | Description                         |
| ------------------------------------------------------------------ | ----------------------------------- |
| `GET /datasets/{ns}/{name}/versions/{rev}/progress`                | Dataset-level progress (all tables) |
| `GET /datasets/{ns}/{name}/versions/{rev}/tables/{table}/progress` | Single table progress               |

### Response Format

#### Dataset-Level Response

```json
{
  "dataset_namespace": "ethereum",
  "dataset_name": "mainnet",
  "revision": "0.0.0",
  "manifest_hash": "2dbf16e8...",
  "tables": [
    {
      "table_name": "blocks",
      "current_block": 11111,
      "start_block": 10000,
      "job_id": 1,
      "job_status": "RUNNING",
      "files_count": 47,
      "total_size_bytes": 2147483648
    }
  ]
}
```

#### Table-Level Response

```json
{
  "table_name": "blocks",
  "current_block": 11111,
  "start_block": 10000,
  "job_id": 1,
  "job_status": "RUNNING",
  "files_count": 47,
  "total_size_bytes": 2147483648
}
```

## Usage

### Get Dataset Progress

```bash
curl -X GET "http://localhost:8080/datasets/ethereum/mainnet/versions/0.0.0/progress" \
  -H "Authorization: Bearer $TOKEN"
```

### Get Table Progress

```bash
curl -X GET "http://localhost:8080/datasets/ethereum/mainnet/versions/0.0.0/tables/blocks/progress" \
  -H "Authorization: Bearer $TOKEN"
```

## Implementation

### DataStore Methods

#### `get_table_progress()`

Computes progress for a single physical table.

```rust
pub async fn get_table_progress(
    &self,
    manifest_hash: &Hash,
    table_name: &TableName,
) -> Result<Option<TableProgress>, GetTableProgressError>
```

**Internal Dependencies:**

| Dependency                            | Purpose                                                  |
| ------------------------------------- | -------------------------------------------------------- |
| `PhysicalTable::get_active()`         | Retrieve the active table revision from metadata         |
| `PhysicalTable::snapshot()`           | Create a point-in-time snapshot of the table             |
| `TableSnapshot::synced_range()`       | Compute the contiguous block range (handles gaps/reorgs) |
| `TableSnapshot::canonical_segments()` | Get the list of canonical parquet files                  |

#### `get_tables_writer_info()`

Retrieves writer job information for all active tables in a dataset.

```rust
pub async fn get_tables_writer_info(
    &self,
    manifest_hash: &Hash,
) -> Result<Vec<TableWriterInfo>, GetWriterInfoError>
```

### Return Types

```rust
/// Progress statistics for a single table.
pub struct TableProgress {
    /// Highest block number synced (end of synced range)
    pub current_block: u64,
    /// Lowest block number synced (start of synced range)
    pub start_block: u64,
    /// Number of canonical parquet files
    pub files_count: u64,
    /// Total size of canonical files in bytes
    pub total_size_bytes: u64,
}

/// Writer job information for a table.
pub struct TableWriterInfo {
    pub table_name: TableName,
    pub job_id: Option<JobId>,
    pub job_status: Option<JobStatus>,
}
```

### Source Files

- `crates/amp-data-store/src/lib.rs` - DataStore methods for progress calculation
- `crates/admin-api/src/handlers/datasets.rs` - HTTP handler orchestration
- `crates/metadata-db/src/progress.rs` - Database queries for writer info

## Limitations

- **Polling required**: Platform services must poll each dataset individually; no push/subscription model currently exists
- **Single-network datasets**: Multi-chain derived datasets may need `Vec<BlockRange>` per network in the future
- **No chainhead**: Response does not include chainhead for lag calculation (requires provider RPC access)
- **No off-chain progress**: Datasets without block numbers (e.g., IPFS sources) have no defined progress metric
- **Bulk endpoint not available**: No endpoint to retrieve progress for multiple datasets in a single request

## References

- [admin](admin.md) - Base: Admin API overview

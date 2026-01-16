---
name: "admin-datasets-progress"
description: "Dataset progress API for monitoring sync state, block ranges, and job health. Load when asking about dataset freshness, sync status, or progress endpoints"
components: "crate:amp-data-store,crate:admin-api,crate:metadata-db"
---

# Dataset Progress API

## Summary

The Dataset Progress API provides visibility into the sync state of datasets, reporting metrics like `start_block`, `current_block`, job health status, and file statistics. This allows anyone to track the progress of a dataset sync at a point in time, either programmatically over the RESTful API or via the administration CLI (ampctl).

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Architecture](#architecture)
3. [API Reference](#api-reference)
4. [Usage](#usage)
5. [Implementation](#implementation)

## Key Concepts

- **Progress**: The current state of data synchronization for a dataset, including the range of blocks that have been synced and the number of files produced
- **Current Block**: The highest block number that has been synced (end of the synced range)
- **Start Block**: The lowest block number that has been synced (beginning of the synced range)
- **Job Status**: The status of the table's currently assigned writer job (from `physical_tables.writer`). Possible values: `RUNNING`, `FAILED`, `COMPLETED`
- **Pull Model**: The current data strategy where progress is calculated on-demand from table snapshots rather than persisted separately

## Architecture

### Data Strategy

The API uses a **Pull Model** where progress is calculated on-demand:

1. **On-demand computation** - Progress is computed from table snapshots when requested
2. **Leverages existing infrastructure** - Uses Postgres metadata and Foyer caches
3. **Point-in-time accuracy** - Each request returns the current state at query time

A **Push Model** (event-driven) will be implemented in a future PR for real-time progress updates to support dashboards and streaming consumers.

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

| Endpoint                                                       | Method | Description                         |
| -------------------------------------------------------------- | ------ | ----------------------------------- |
| `/datasets/{ns}/{name}/versions/{rev}/progress`                | GET    | Dataset-level progress (all tables) |
| `/datasets/{ns}/{name}/versions/{rev}/tables/{table}/progress` | GET    | Single table progress               |

For request/response schemas, see [Admin API OpenAPI spec](../openapi-specs/admin.spec.json):

```bash
# Dataset progress endpoint
jq '.paths["/datasets/{namespace}/{name}/versions/{revision}/progress"]' docs/openapi-specs/admin.spec.json

# Table progress endpoint
jq '.paths["/datasets/{namespace}/{name}/versions/{revision}/tables/{table}/progress"]' docs/openapi-specs/admin.spec.json
```

## Usage

**Get dataset progress:**

View sync progress for all tables in a dataset, including current block, job status, and file statistics.

```bash
# Get progress for a dataset version
ampctl dataset progress ethereum/mainnet@0.0.0

# Example output:
# Dataset: ethereum/mainnet@0.0.0
# Manifest: 2dbf16e8...
#
# Table          Current Block   Start Block   Status    Files   Size
# blocks         21,500,000      0             RUNNING   1,247   128.5 GB
# transactions   21,499,850      0             RUNNING   3,891   512.2 GB
# logs           21,499,900      0             RUNNING   2,156   256.8 GB
```

**Get single table progress:**

View detailed progress for a specific table within a dataset.

```bash
# Get progress for a single table
ampctl dataset progress ethereum/mainnet@0.0.0 --table blocks

# Example output:
# Table: blocks
# Current Block: 21,500,000
# Start Block: 0
# Job ID: 42
# Status: RUNNING
# Files: 1,247
# Total Size: 128.5 GB
```

**JSON output for scripting:**

Use JSON format to pipe output to jq or other tools for automated processing and monitoring.

```bash
# Dataset progress as JSON
ampctl dataset progress ethereum/mainnet@0.0.0 --json

# Extract current block for a table with jq
ampctl dataset progress ethereum/mainnet@0.0.0 --json | jq '.tables.blocks.current_block'
```

**Direct API access:**

Query the Admin API directly using curl for integrations or when ampctl is not available.

```bash
# Get dataset progress
curl http://localhost:1610/datasets/ethereum/mainnet/versions/0.0.0/progress

# Get single table progress
curl http://localhost:1610/datasets/ethereum/mainnet/versions/0.0.0/tables/blocks/progress
```

## Implementation

### How "Current Block" is Defined

The `current_block` field represents the highest block number in the **contiguous synced range** for a table. This is computed from the table's canonical chain of parquet files:

1. **Canonical segments**: Only files that form a valid, non-overlapping chain are considered. Orphaned segments (from reorgs) are excluded.

2. **Synced range**: The range spans from the lowest block in the first canonical segment (`start_block`) to the highest block in the last canonical segment (`current_block`).

3. **Gap handling**: If there are gaps in the canonical chain, the synced range reflects only the contiguous portion. This ensures `current_block` accurately represents verified, queryable data.

This approach provides a reliable "ground truth" for sync progress, unaffected by reorgs or incomplete writes.

### Source Files

- `crates/services/admin-api/src/handlers/datasets/progress.rs` - API endpoint handlers
- `crates/clients/admin/src/datasets.rs` - Client library
- `crates/core/metadata-db/src/progress.rs` - Database operations for writer info

## References

- [admin](admin.md) - Base: Administration overview
- [app-ampctl](app-ampctl.md) - Related: CLI tool
- [admin-datasets](admin-datasets.md) - Related: Dataset management

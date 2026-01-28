---
name: "admin-jobs-progress"
description: "Job progress API for monitoring sync state, block ranges, and table health. Load when asking about job status, sync progress, or the /jobs/{id}/progress endpoint"
components: "crate:amp-data-store,crate:admin-api,crate:metadata-db"
---

# Job Progress API

## Summary

The Job Progress API provides visibility into the sync state of jobs, reporting metrics like `start_block`, `current_block`, and file statistics for all tables written by a specific job. This allows operators to track the progress of a dataset sync at a point in time, either programmatically over the RESTful API or via the administration CLI (ampctl).

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Architecture](#architecture)
3. [API Reference](#api-reference)
4. [Usage](#usage)
5. [Implementation](#implementation)

## Key Concepts

- **Progress**: The current state of data synchronization for tables written by a job, including the range of blocks that have been synced and the number of files produced
- **Current Block**: The highest block number that has been synced (end of the synced range)
- **Start Block**: The lowest block number that has been synced (beginning of the synced range)
- **Job Status**: The status of the job. Possible values: `RUNNING`, `FAILED`, `COMPLETED`
- **Pull Model**: The current data strategy where progress is calculated on-demand from table snapshots rather than persisted separately

## Architecture

### Data Strategy

The API uses a **Pull Model** where progress is calculated on-demand:

1. **On-demand computation** - Progress is computed from table snapshots when requested
2. **Leverages existing infrastructure** - Uses Postgres metadata and Foyer caches
3. **Point-in-time accuracy** - Each request returns the current state at query time

A **Push Model** (event-driven) is available via [Worker Event Streaming](app-ampd-worker-events.md) for real-time progress updates to support dashboards and streaming consumers.

### Logic Location & Ownership

| Component       | Responsibility                                                         |
| --------------- | ---------------------------------------------------------------------- |
| **`DataStore`** | `get_tables_by_writer()` - finds tables written by a specific job       |
| **`DataStore`** | Computes sync stats from table snapshots                               |
| **Admin API**   | Orchestration - resolves job, iterates tables, combines results        |
| **Admin API**   | Presentation - formats JSON response for REST clients                  |
| **Gateway**     | Authentication - scopes access to authorized users                     |

**Key Principle**: Admin API handlers do not interact with `metadata_db` directly. All database access is encapsulated in `DataStore` methods, keeping handlers as pure orchestration and presentation logic.

### Handler Flow

```
1. Extract job ID from path
2. Verify job exists via Scheduler
3. Call DataStore::get_tables_by_writer() to find tables
4. For each table:
   a. Resolve dataset reference
   b. Get table snapshot
   c. Compute progress (synced range, file count, size)
5. Format and return JSON response
```

### Workflow

Progress semantically belongs under jobs because jobs are the entity associated with sync progress. The recommended workflow for operators is:

1. **List jobs for a dataset**: `GET /datasets/{ns}/{name}/versions/{rev}/jobs`
2. **Pick a job**: Identify the relevant job ID from the response
3. **Query its progress**: `GET /jobs/{id}/progress`

This design follows the Single Responsibility Principle: `/datasets` manages dataset revisions, `/jobs` manages jobs and their execution state.

## API Reference

| Endpoint               | Method | Description                                |
| ---------------------- | ------ | ------------------------------------------ |
| `/jobs/{id}/progress`  | GET    | Progress for all tables written by the job |

For request/response schemas, see [Admin API OpenAPI spec](../openapi-specs/admin.spec.json):

```bash
# Job progress endpoint
jq '.paths["/jobs/{id}/progress"]' docs/openapi-specs/admin.spec.json
```

### Response Schema

```json
{
  "job_id": 42,
  "job_status": "RUNNING",
  "tables": {
    "blocks": {
      "current_block": 21500000,
      "start_block": 0,
      "files_count": 1247,
      "total_size_bytes": 137970286592
    },
    "transactions": {
      "current_block": 21499850,
      "start_block": 0,
      "files_count": 3891,
      "total_size_bytes": 549957091328
    }
  }
}
```

The table keys are the table names. Since a job writes to exactly one dataset, table names are unique within the response.

## Usage

**Get job progress:**

View sync progress for all tables written by a job, including current block and file statistics.

```bash
# First, list jobs for a dataset to get the job ID
ampctl dataset jobs ethereum/mainnet@0.0.0

# Example output:
# ID    Status    Created              Tables
# 42    RUNNING   2024-01-15 10:30:00  blocks, transactions, logs

# Then get progress for that job
curl http://localhost:1610/jobs/42/progress

# Example response:
# {
#   "job_id": 42,
#   "job_status": "RUNNING",
#   "tables": {
#     "blocks": {
#       "current_block": 21500000,
#       "start_block": 0,
#       "files_count": 1247,
#       "total_size_bytes": 137970286592
#     },
#     ...
#   }
# }
```

**JSON output for scripting:**

Use JSON format to pipe output to jq or other tools for automated processing and monitoring.

```bash
# Get progress and extract current block for a table with jq
curl http://localhost:1610/jobs/42/progress | jq '.tables["blocks"].current_block'
```

## Implementation

### How "Current Block" is Defined

The `current_block` field represents the highest block number in the **contiguous synced range** for a table. This is computed from the table's canonical chain of parquet files:

1. **Canonical segments**: Only files that form a valid, non-overlapping chain are considered. Orphaned segments (from reorgs) are excluded.

2. **Synced range**: The range spans from the lowest block in the first canonical segment (`start_block`) to the highest block in the last canonical segment (`current_block`).

3. **Gap handling**: If there are gaps in the canonical chain, the synced range reflects only the contiguous portion. This ensures `current_block` accurately represents verified, queryable data.

This approach provides a reliable "ground truth" for sync progress, unaffected by reorgs or incomplete writes.

### Source Files

- `crates/services/admin-api/src/handlers/jobs/progress.rs` - API endpoint handler
- `crates/core/metadata-db/src/physical_table.rs` - Database operations for writer table info
- `crates/core/data-store/src/lib.rs` - DataStore wrapper methods

## References

- [admin](admin.md) - Base: Administration overview
- [app-ampctl](app-ampctl.md) - Related: CLI tool
- [admin-datasets](admin-datasets.md) - Related: Dataset management
- [admin-jobs](admin-jobs.md) - Related: Job management
- [app-ampd-worker-events](app-ampd-worker-events.md) - Related: Push Model (Kafka) for real-time progress

---
title: Dataset Sync Progress API
status: Proposed / In-Review
author: "@mitchhs12"
reviewers: ["@LNSD", "@leoyvens", "@ChrisWhited"]
date: 2026-01-14
---

# Feature: Dataset Sync Progress API

## 1. Context & Motivation

As identified by the Product team, users currently lack visibility into the operational state of their datasets. Whether a developer is working locally in "Studio" or a consumer is viewing a dataset on the Platform UI, they need to know:

1. **Freshness:** Is the data up to date with the blockchain?
2. **Health:** Is the sync job running, or has it failed?
3. **Scale:** How many files and bytes have been processed?

This API provides the "Ground Truth" for the engine's state, which the Platform backend can then use to calculate higher-level metrics like `is_fresh` or `block_lag`.

---

## 2. Goals

- **Standardized Status:** Report job health (e.g., `RUNNING`, `FAILED`, `COMPLETED`).
- **Sync Metrics:** Provide `start_block`, `current_block`, and `chainhead` where available.
- **Ergonomics:** Move logic into `amp-data-store` to allow both the Admin API (REST) and potentially SQL queries to access this data consistently.
- **SDK Support:** Provide the foundation for `amp-typescript` and `amp-python` to surface status to end-users.

---

## 3. Architecture

### Architectural Concerns

An initial implementation placed all sync progress logic directly in the Admin API handler. During review, several architectural issues were identified:

**Issue 1: Direct Database Access in Handlers**

The handler queried `metadata_db` directly to retrieve writer job information:

```rust
// Problematic: Handler directly accesses metadata_db
let writer_infos = metadata_db::sync_progress::get_active_tables_with_writer_info(
    &ctx.metadata_db,
    manifest_hash,
).await?;
```

This violates the principle that Admin API handlers should not interact with the metadata database directly. Database access should be encapsulated within the data plane (`amp-data-store`), not scattered across API handlers.

**Issue 2: Ad-hoc Logic in Handlers**

The handler contained ~80 lines of logic for computing sync statistics: iterating over tables, getting physical table snapshots, calculating block ranges, and aggregating file statistics. This logic is too ad-hoc to live in an API handler, it belongs in the data plane where it can be:

- Reused by other components (CLI, SDKs, future SQL meta-queries)
- Unit tested in isolation
- Maintained alongside related data access code

**Issue 3: Integration with the Data Lake**

The sync progress calculation was not integrated with the Amp "data lake" interface (`amp-data-store`). As the data store is being actively developed and refactored, having this logic external to it creates maintenance burden and potential conflicts.

**Resolution**

The Admin API must serve as a **translation and orchestration layer**, not a business logic layer. To address these concerns:

1. Move sync progress calculation into `DataStore` methods
2. Wrap all `metadata_db` queries in `DataStore` methods
3. Keep handlers thin, they should only orchestrate calls and format responses

### Logic Location & Ownership

The logic is decoupled across layers to maintain architectural integrity:

| Component       | Responsibility                                                      |
| --------------- | ------------------------------------------------------------------- |
| **`DataStore`** | `get_table_progress()` - computes sync stats from table snapshot    |
| **`DataStore`** | `get_tables_writer_info()` - wraps metadata_db query for job info   |
| **Admin API**   | Orchestration - resolves dataset, iterates tables, combines results |
| **Admin API**   | Presentation - formats JSON response for REST clients               |
| **Gateway**     | Authentication - scopes access to dataset owners                    |

**Key Principle:** Admin API handlers must not interact with `metadata_db` directly. All database access is encapsulated in `DataStore` methods, keeping handlers as pure orchestration and presentation logic.

### Data Strategy: Pull vs. Push

- **Phase 1:** A **Pull Model**. The API queries the `DataStore`, which calculates the range on-demand. This utilizes existing Postgres/Foyer caches and avoids introducing a Kafka dependency immediately.
- **Potential Phase 2:** A **Push Model**. The worker or `DataStore` will emit events when new segments are committed, allowing for real-time updates without polling (e.g., via Kafka).

#### Why Pull First?

The Pull model is chosen because:

1. **No new infrastructure** - Avoids introducing Kafka or a message broker dependency
2. **Leverages existing caches** - Postgres metadata and Foyer caches already exist
3. **Simpler operational model** - No event consumers to deploy and monitor
4. **Sufficient for initial use case** - Platform UI polling at reasonable intervals (e.g., 5-10s) is acceptable for dashboard freshness

#### Polling Concerns

A concern was raised about the scalability of polling: _"Do we expect platform services to poll for ALL dataset revisions?"_

For the initial implementation, this concern is acknowledged but deferred. The current design requires Platform to poll each dataset individually. If this proves inefficient, a bulk endpoint can be added (see Future Considerations).

**When to move to Push:**

- If polling frequency needs to be sub-second (real-time dashboards)
- If the number of concurrent polling clients becomes a bottleneck
- If we need to trigger downstream actions on progress events (webhooks, notifications)

#### Alternatives Considered

| Alternative                          | Pros                                | Cons                                                                 | Decision                              |
| ------------------------------------ | ----------------------------------- | -------------------------------------------------------------------- | ------------------------------------- |
| **Kafka/Event Stream**               | Real-time, scales to many consumers | New infrastructure dependency, operational complexity                | Deferred to Phase 2                   |
| **Metrics/OpenTelemetry**            | Already have metrics infrastructure | Not designed for per-dataset queries, harder for Platform to consume | Not suitable for this use case        |
| **Persisted `current_block` column** | Fast reads, no computation          | Can become stale, doesn't handle reorgs correctly                    | Rejected in favor of `synced_range()` |
| **WebSocket subscription**           | Real-time, no polling               | Requires connection management, more complex client                  | Deferred to Phase 2                   |

---

## 4. Technical Specification

### 4.1 API Endpoints

| Endpoint                                                           | Description                         |
| ------------------------------------------------------------------ | ----------------------------------- |
| `GET /datasets/{ns}/{name}/versions/{rev}/progress`                | Dataset-level progress (all tables) |
| `GET /datasets/{ns}/{name}/versions/{rev}/tables/{table}/progress` | Single table progress               |

### 4.2 Response Format

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

### 4.3 DataStore API Additions

The following methods will be added to `amp_data_store::DataStore`:

#### `get_table_progress()`

Computes sync progress for a single physical table.

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

**Implementation Steps:**

1. Call `PhysicalTable::get_active()` to get the active revision
2. If no active revision exists, return `Ok(None)`
3. Create a snapshot via `snapshot(false, data_store)`
4. Extract `synced_range()` for `start_block` and `current_block`
5. Compute `files_count` and `total_size_bytes` from `canonical_segments()`

**Error Cases:**

| Error                                          | Cause                                                            |
| ---------------------------------------------- | ---------------------------------------------------------------- |
| `GetTableProgressError::PhysicalTableNotFound` | No active revision registered in metadata                        |
| `GetTableProgressError::SnapshotFailed`        | Failed to create table snapshot (e.g., object store unavailable) |
| `GetTableProgressError::MetadataError`         | Database query failed                                            |

---

#### `get_tables_writer_info()`

Retrieves writer job information for all active tables in a dataset.

```rust
pub async fn get_tables_writer_info(
    &self,
    manifest_hash: &Hash,
) -> Result<Vec<TableWriterInfo>, GetWriterInfoError>
```

**Internal Dependencies:**

| Dependency                                                         | Purpose                          |
| ------------------------------------------------------------------ | -------------------------------- |
| `metadata_db::sync_progress::get_active_tables_with_writer_info()` | Query job status from PostgreSQL |

**Implementation Steps:**

1. Call the existing `metadata_db` query function
2. Map results to `TableWriterInfo` structs

**Error Cases:**

| Error                               | Cause                   |
| ----------------------------------- | ----------------------- |
| `GetWriterInfoError::DatabaseError` | PostgreSQL query failed |

---

#### Return Types

```rust
/// Sync progress statistics for a single table.
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

### 4.4 Admin API Handler Flow

The handler acts as an orchestration layer:

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

---

## 5. Design Decisions & Trade-offs

#### Decision: No Direct metadata_db Access in Handlers

Per architectural guidelines, Admin API handlers must not query `metadata_db` directly. All database access is encapsulated in `DataStore` methods. This ensures:

- Consistent data access patterns across the codebase
- Easier testing and mocking
- Clear separation between orchestration (API) and data access (DataStore)

#### Decision: Decoupling "Freshness" Logic

The engine does not define "Freshness" (e.g., "Stable" vs "Lagging"). The engine reports `current_block` and the Platform Backend determines status based on the specific network's block time and user requirements.

#### Decision: Using TableSnapshot for Accuracy

Instead of reading a potentially stale `current_block` column from a database, we derive progress from actual committed files using `TableSnapshot::synced_range()`.

- **Pro:** Guarantees accuracy regarding the canonical chain and handles reorgs correctly.
- **Con:** Higher computational cost, mitigated by metadata caching (Foyer).

#### Decision: Per-Table Endpoint

In addition to the dataset-level endpoint, a per-table endpoint is provided for:

- More granular monitoring of individual tables
- Reduced payload size when only one table is of interest
- Alignment with REST resource conventions

#### Decision: Authentication Scope

As noted in discussions, this endpoint is high-value for end-users. While hosted in the Admin API, it should not be restricted to "Admin-only" tokens, but rather scoped to the owner of the dataset via Gateway middleware.

---

## 6. Future Considerations

- **Bulk Progress Endpoint:** Add `GET /datasets/progress` to retrieve progress for multiple datasets in a single request. This would reduce HTTP overhead for Platform services that need to monitor many datasets. The endpoint would need to handle authorization (filter to datasets the caller can access) and pagination.

- **Push Model:** Implement event-driven updates via Kafka or similar, allowing clients to subscribe to progress changes rather than polling.

- **Multi-chain Derived Datasets:** For derived datasets that join tables from multiple blockchains (e.g., Arbitrum + Ethereum), the progress response may need to return a `Vec<BlockRange>` with one range per network, rather than a single `current_block`. This aligns with the existing `app_metadata` field ranges structure. The current design assumes single-network datasets.

- **Non-block Progress:** Strategies for defining "progress" for off-chain datasets that do not utilize block numbers (e.g., IPFS-based data sources).

- **Chainhead Integration:** Include `chainhead` in the response when provider connectivity allows, enabling lag calculation. This requires the DataStore to have access to provider RPC endpoints, which may not always be available.

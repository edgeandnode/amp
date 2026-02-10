---
name: "admin-revisions"
description: "Table revision activation and deactivation API for controlling which physical table revision is queryable. Load when asking about revision management, activate/deactivate endpoints, or table revision lifecycle"
type: "feature"
status: "development"
components: "service:admin-api,crate:amp-data-store,crate:metadata-db"
---

# Table Revision Management

## Summary

The Table Revision Management API provides endpoints to activate and deactivate physical table revisions, controlling which revision of a table is served for queries. Activation atomically switches the queryable revision by deactivating all existing revisions and activating the specified one in a single transaction. Deactivation marks all revisions for a table as inactive so queries return errors.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Architecture](#architecture)
3. [API Reference](#api-reference)
4. [Usage](#usage)
5. [Implementation](#implementation)
6. [References](#references)

## Key Concepts

- **Table Revision**: An immutable snapshot of table data at a specific [location](../glossary.md#location) ID, representing one physical version of a table
- **Active Revision**: The revision currently served for queries; at most one revision per table can be active at a time
- **Location ID**: Unique identifier for a physical table revision in the metadata database
- **Activation**: Atomically switch which revision is queryable by deactivating all revisions and activating the specified one within a transaction
- **Deactivation**: Mark all revisions for a table as inactive so the table is no longer queryable

## Architecture

### Logic Location & Ownership

| Component         | Responsibility                                                                 |
| ----------------- | ------------------------------------------------------------------------------ |
| **Admin API**     | Orchestration - resolves dataset reference, delegates to DataStore              |
| **Admin API**     | Presentation - returns HTTP status codes to REST clients                       |
| **`DataStore`**   | Transaction management for activate (begin, mark inactive, mark active, commit) |
| **`DataStore`**   | Single-operation deactivate (mark inactive)                                    |
| **`metadata_db`** | SQL operations on `physical_tables` (mark_inactive_by_table_name, mark_active_by_id) |

**Key Principle**: Admin API handlers do not interact with `metadata_db` directly. All database access is encapsulated in `DataStore` methods, keeping handlers as pure orchestration and presentation logic.

### Handler Flow

**Activate (`POST /revisions/{id}/activate`):**

```
1. Parse ActivationPayload (dataset, table_name)
2. Parse location_id from path
2. Resolve dataset reference to manifest hash via DatasetsRegistry
3. If dataset not found → return DATASET_NOT_FOUND (404)
5. Call DataStore::activate_table_revision(reference, table_name, location_id)
   a. Begin transaction
   b. mark_inactive_by_table_name() — deactivate all revisions for the table
   c. mark_active_by_id() — activate the specified revision
   d. Commit transaction
6. Return HTTP 200 (no body)
```

**Deactivate (`POST /revisions/deactivate`):**

```
1. Parse DeactivationPayload (dataset, table_name)
2. Resolve dataset reference to manifest hash via DatasetsRegistry
3. If dataset not found → return DATASET_NOT_FOUND (404)
4. Call DataStore::deactivate_table_revision(reference, table_name)
   a. mark_inactive_by_table_name() — single DB call, no transaction wrapper
5. Return HTTP 200 (no body)
```

### Atomicity

- **Activate**: Wrapped in a database transaction to ensure exactly one revision is active. If any step fails, the transaction rolls back automatically
- **Deactivate**: Single database call (no transaction needed) that marks all revisions inactive

## API Reference

| Endpoint                 | Method | Description                                    |
| ------------------------ | ------ | ---------------------------------------------- |
| `/revisions/{id}/activate`    | POST   | Activate a specific table revision by location ID |
| `/revisions/deactivate`  | POST   | Deactivate all revisions for a table           |

### Request Schemas

**ActivationPayload:**

```json
{
  "dataset": "_/eth_rpc@0.0.0",
  "table_name": "blocks",
}
```

**DeactivationPayload:**

```json
{
  "dataset": "_/eth_rpc@0.0.0",
  "table_name": "blocks"
}
```

**Response (both endpoints):** HTTP 200 with no body on success.

### Error Codes

| Code                              | Status | Description                              |
| --------------------------------- | ------ | ---------------------------------------- |
| `DATASET_NOT_FOUND`               | 404    | Dataset or revision not found            |
| `ACTIVATE_TABLE_REVISION_ERROR`   | 500    | Database error during activation         |
| `DEACTIVATE_TABLE_REVISION_ERROR` | 500    | Database error during deactivation       |
| `RESOLVE_REVISION_ERROR`          | 500    | Failed to resolve dataset revision       |

## Usage

**Deactivate all revisions for a table:**

```bash
curl -X POST http://localhost:1610/revisions/deactivate \
  -H 'Content-Type: application/json' \
  -d '{"dataset": "_/eth_rpc@0.0.0", "table_name": "blocks"}'
```

**Activate a specific revision:**

```bash
curl -X POST http://localhost:1610/revisions/42/activate \
  -H 'Content-Type: application/json' \
  -d '{"dataset": "_/eth_rpc@0.0.0", "table_name": "blocks"}'
```

## Implementation

### Source Files

- `crates/services/admin-api/src/handlers/revisions/activate.rs` - Activate endpoint handler and error types
- `crates/services/admin-api/src/handlers/revisions/deactivate.rs` - Deactivate endpoint handler and error types
- `crates/core/data-store/src/lib.rs` - `activate_table_revision` (transactional) and `deactivate_table_revision` methods
- `crates/core/metadata-db/src/physical_table.rs` - `mark_inactive_by_table_name` and `mark_active_by_id` SQL operations

## References

- [admin](admin.md) - Base: Administration overview
- [data-store](data-store.md) - Related: Storage abstraction and revision lifecycle
- [admin-jobs-progress](admin-jobs-progress.md) - Related: Job progress uses table revision data

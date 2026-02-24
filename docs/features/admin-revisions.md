---
name: "admin-revisions"
description: "Table revision registration, activation, deactivation, deletion, and truncation API for controlling which physical table revision is queryable. Load when asking about revision management, register/activate/deactivate/delete/truncate endpoints, or table revision lifecycle"
type: "feature"
status: "development"
components: "service:admin-api,crate:amp-data-store,crate:metadata-db"
---

# Table Revision Management

## Summary

The Table Revision Management API provides endpoints to register, list, retrieve, activate, deactivate, delete, and truncate physical table revisions, controlling which revision of a table is served for queries. New revisions are registered as inactive records before they can be activated. All revisions can be listed with an optional active status filter. A single revision can be retrieved by its location ID. Activation atomically switches the queryable revision by deactivating all existing revisions and activating the specified one in a single transaction. Deactivation marks all revisions for a table as inactive so queries return errors. Deletion permanently removes an inactive revision and its associated file metadata, with guards to prevent deleting active revisions or those with running writer jobs. Truncation goes further than deletion by first deleting all files from object storage and their metadata rows before removing the revision record itself.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Architecture](#architecture)
3. [API Reference](#api-reference)
4. [Usage](#usage)
5. [Implementation](#implementation)
6. [References](#references)

## Key Concepts

- **Table Revision**: An immutable snapshot of table data at a specific [location](../glossary.md#physical-table-revision) ID, representing one physical version of a table
- **Active Revision**: The revision currently served for queries; at most one revision per table can be active at a time
- **Location ID**: Unique identifier for a physical table revision in the metadata database
- **Activation**: Atomically switch which revision is queryable by deactivating all revisions and activating the specified one within a transaction
- **Registration**: Creates an inactive, unassigned physical table revision record. Revisions must be registered before they can be activated. Registration is idempotent by path — registering the same path twice returns the existing location ID
- **Deactivation**: Mark all revisions for a table as inactive so the table is no longer queryable
- **Deletion**: Permanently removes an inactive revision and all associated file metadata from the database. Active revisions must be deactivated before they can be deleted
- **Truncation**: Deletes all files from object storage and their corresponding metadata rows, verifies cleanup, then deletes the revision record. Unlike deletion (which relies on CASCADE), truncation actively removes object storage files. The revision must be inactive and any writer job must be in a terminal state
- **Restoration**: Re-reads files from a revision's object storage path and registers their metadata in the database

## Architecture

### Logic Location & Ownership

| Component         | Responsibility                                                                 |
| ----------------- | ------------------------------------------------------------------------------ |
| **Admin API**     | Orchestration - resolves dataset reference, delegates to DataStore              |
| **Admin API**     | Presentation - returns HTTP status codes to REST clients                       |
| **`DataStore`**   | Single-operation register (builds metadata, inserts revision record)            |
| **`DataStore`**   | Transaction management for activate (begin, mark inactive, mark active, commit) |
| **`DataStore`**   | Single-operation deactivate (mark inactive)                                    |
| **`DataStore`**   | Single-operation get revision by location ID                                   |
| **`DataStore`**   | Single-operation delete revision by location ID (CASCADE deletes file metadata) |
| **`DataStore`**   | Multi-step truncate revision (stream files, delete from object store + metadata, verify, delete revision) |
| **`metadata_db`** | SQL operations on `physical_table_revisions` (register, list_all, get_by_location_id, delete_by_id) and `physical_tables` (mark_inactive_by_table_name, mark_active_by_id) |

**Key Principle**: Admin API handlers do not interact with `metadata_db` directly. All database access is encapsulated in `DataStore` methods, keeping handlers as pure orchestration and presentation logic.

### Handler Flow

Each handler follows the same pattern: parse request, resolve dataset reference, delegate to `DataStore`, and return an HTTP response. For detailed handler logic, see the doc comments in the handler source files listed in [Implementation](#implementation).

### Atomicity

- **Register**: Single database call (no transaction needed) that inserts an inactive revision record. Idempotent by path
- **Activate**: Wrapped in a database transaction to ensure exactly one revision is active. If any step fails, the transaction rolls back automatically
- **Deactivate**: Single database call (no transaction needed) that marks all revisions inactive
- **Delete**: Single database call (no transaction needed) that removes the revision record. Associated `file_metadata` entries are removed automatically via CASCADE foreign key constraints. The handler performs pre-checks (revision must be inactive, writer job must be in a terminal state) before issuing the delete
- **Truncate**: Multi-step operation: streams file metadata, deletes files from object storage and metadata rows with bounded concurrency, verifies no metadata rows remain, then deletes the revision record. Pre-checks match delete (inactive, terminal writer job). Not transactional — partial failures are recoverable by retrying

## API Reference

| Endpoint                      | Method | Description                                       |
| ----------------------------- | ------ | ------------------------------------------------- |
| `/revisions`                  | GET    | List revisions (optional `?active=true\|false` filter, `?limit=N` default 100) |
| `/revisions`                  | POST   | Register a new inactive table revision             |
| `/revisions/{id}`             | GET    | Retrieve a specific revision by location ID       |
| `/revisions/{id}`             | DELETE | Delete an inactive table revision by location ID  |
| `/revisions/{id}/activate`    | POST   | Activate a specific table revision by location ID |
| `/revisions/{id}/truncate`    | DELETE | Truncate a revision (delete files from object storage + metadata, then delete revision) |
| `/revisions/{id}/restore`     | POST   | Restore revision files from object storage        |
| `/revisions/deactivate`       | POST   | Deactivate all revisions for a table              |

### Request & Response Schemas

See the [Admin API OpenAPI spec](../openapi-specs/admin.spec.json) for complete request/response body schemas for all revision endpoints.

### Error Codes

See the [Admin API OpenAPI spec](../openapi-specs/admin.spec.json) for error codes and HTTP status mappings.

## Usage

**Register a new table revision:**

```bash
# Via ampctl
ampctl table register _/eth_rpc@0.0.0 blocks relative/path/to/revision

# Via API
curl -X POST http://localhost:1610/revisions \
  -H 'Content-Type: application/json' \
  -d '{"dataset": "_/eth_rpc@0.0.0", "table_name": "blocks", "path": "relative/path/to/revision"}'
```

**List table revisions:**

```bash
# Via ampctl
ampctl table list
ampctl table list --active true
ampctl table list --active true --limit 5

# Via API
curl http://localhost:1610/revisions
curl http://localhost:1610/revisions?active=true
```

**Get a specific revision:**

```bash
# Via ampctl
ampctl table get 42

# Via API
curl http://localhost:1610/revisions/42
```

**Activate a specific revision:**

Atomically deactivates all existing revisions for the table and activates the specified one.

```bash
# Via ampctl
ampctl table activate _/eth_rpc@0.0.0 blocks 42

# Via API
curl -X POST http://localhost:1610/revisions/42/activate \
  -H 'Content-Type: application/json' \
  -d '{"dataset": "_/eth_rpc@0.0.0", "table_name": "blocks"}'
```

**Deactivate all revisions for a table:**

```bash
# Via ampctl
ampctl table deactivate _/eth_rpc@0.0.0 blocks

# Via API
curl -X POST http://localhost:1610/revisions/deactivate \
  -H 'Content-Type: application/json' \
  -d '{"dataset": "_/eth_rpc@0.0.0", "table_name": "blocks"}'
```

**Delete an inactive revision:**

```bash
# Via ampctl (with confirmation prompt)
ampctl table delete 42

# Via ampctl (skip confirmation)
ampctl table delete 42 --force

# Via API
curl -X DELETE http://localhost:1610/revisions/42
```

**Truncate a revision (delete files + metadata + revision):**

Unlike `delete`, which only removes database records (relying on CASCADE for file metadata), `truncate` also deletes all files from object storage.

```bash
# Via ampctl (with confirmation prompt)
ampctl table truncate 42

# Via ampctl (skip confirmation)
ampctl table truncate 42 --force

# Via ampctl (custom concurrency)
ampctl table truncate 42 --force --concurrency 20

# Via API
curl -X DELETE http://localhost:1610/revisions/42/truncate

# Via API (custom concurrency)
curl -X DELETE http://localhost:1610/revisions/42/truncate?concurrency=20
```

**Restore a revision's file metadata:**

Lists files in object storage under the revision's path and registers their Parquet metadata into the database

```bash
# Via ampctl
ampctl table restore 42

# Via API
curl -X POST http://localhost:1610/revisions/42/restore
```

## Implementation

### Source Files

- `crates/services/admin-api/src/handlers/revisions/create.rs` - Register endpoint handler and error types
- `crates/services/admin-api/src/handlers/revisions/list_all.rs` - List endpoint handler with optional active filter and error types
- `crates/services/admin-api/src/handlers/revisions/get_by_id.rs` - Get by ID endpoint handler and error types
- `crates/services/admin-api/src/handlers/revisions/activate.rs` - Activate endpoint handler and error types
- `crates/services/admin-api/src/handlers/revisions/delete.rs` - Delete endpoint handler and error types
- `crates/services/admin-api/src/handlers/revisions/deactivate.rs` - Deactivate endpoint handler and error types
- `crates/services/admin-api/src/handlers/revisions/truncate.rs` - Truncate endpoint handler and error types
- `crates/services/admin-api/src/handlers/revisions/restore.rs` - Restore endpoint handler and error types
- `crates/core/data-store/src/lib.rs` - `register_table_revision`, `activate_table_revision` (transactional), `deactivate_table_revision`, `delete_table_revision`, and `truncate_revision` methods
- `crates/core/metadata-db/src/physical_table.rs` - `mark_inactive_by_table_name` and `mark_active_by_id` SQL operations on `physical_tables`
- `crates/core/metadata-db/src/physical_table_revision.rs` - `register`, `list_all`, `get_by_location_id`, and `delete_by_id` SQL operations on `physical_table_revisions`

## References

- [admin](admin.md) - Base: Administration overview
- [data-store](data-store.md) - Related: Storage abstraction and revision lifecycle
- [admin-jobs-progress](admin-jobs-progress.md) - Related: Job progress uses table revision data

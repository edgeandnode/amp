---
name: "app-ampctl-table"
description: "Table revision commands: list, get, register, activate, deactivate. Load when asking about managing table revisions via ampctl CLI"
type: feature
status: development
components: "app:ampctl,crate:admin-client,service:admin-api"
---

# Table Revision Management

## Summary

Table commands manage physical table revisions that control which version of table data is served to queries. Operators can list revisions with optional filters, inspect specific revisions by location ID, register new revisions from storage paths, and atomically activate or deactivate revisions to control query serving.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Usage](#usage)
3. [API Reference](#api-reference)
4. [Implementation](#implementation)
5. [References](#references)

## Key Concepts

- **Table Revision**: A physical version of a table's data at a specific storage [location](../glossary.md#physical-table-revision)
- **Active Revision**: The revision currently served for queries; at most one revision per table can be active
- **Location ID**: Unique identifier for a physical table revision in the metadata database
- **Activation**: Atomically deactivates all existing revisions and activates the specified one in a single transaction

## Usage

**List table revisions:**

View revisions with optional active status filter and result limit.

```bash
# List all table revisions
ampctl table list
ampctl table ls  # alias

# Filter by active status
ampctl table list --active true

# Limit the number of results
ampctl table list --limit 10

# Combine filters
ampctl table list --active true --limit 5
```

**Get revision details:**

Retrieve details for a specific revision by location ID, including path, active status, writer, and dataset metadata.

```bash
ampctl table get 1
ampctl table get 1 --json
```

**Register a new revision:**

Create an inactive, unlinked revision record in the metadata database. The revision must be activated afterwards to serve queries.

```bash
# Register with dataset FQN, table name, and storage path
ampctl table register edgeandnode/ethereum_mainnet@0.0.1 blocks custom/blocks
```

**Activate a revision:**

Atomically switch the active revision for a table. All existing revisions for the table are deactivated, and the specified revision becomes active.

```bash
ampctl table activate edgeandnode/ethereum_mainnet@0.0.1 blocks 1
```

**Deactivate all revisions for a table:**

Mark all revisions as inactive so the table is no longer queryable.

```bash
ampctl table deactivate edgeandnode/ethereum_mainnet@0.0.1 blocks
```

**JSON output for scripting:**

```bash
ampctl table list --json
ampctl table get 1 --json
```

## API Reference

For request/response schemas, see [Admin API OpenAPI spec](../openapi-specs/admin.spec.json):

```bash
jq '.paths | to_entries[] | select(.key | startswith("/revisions"))' docs/openapi-specs/admin.spec.json
```

## Implementation

### Source Files

- `crates/bin/ampctl/src/cmd/table/` - CLI command implementations
- `crates/services/admin-api/src/handlers/revisions/` - API endpoint handlers
- `crates/core/data-store/src/lib.rs` - DataStore revision methods

## References

- [app-ampctl](app-ampctl.md) - Base: ampctl overview
- [admin-revisions](admin-revisions.md) - Related: Table revision API details and architecture
- [admin](admin.md) - Related: Administration overview

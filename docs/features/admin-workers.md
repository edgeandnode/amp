---
name: "admin-workers"
description: "Worker node management and monitoring. Load when asking about workers, heartbeats, node status, or worker list"
components: "service:admin-api,crate:admin-client,crate:metadata-db"
---

# Worker Management

## Summary

Worker management provides visibility into the distributed extraction workers that execute data extraction jobs. Operators can list all registered workers, inspect individual worker details including version information, and monitor worker health through heartbeat timestamps.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Architecture](#architecture)
3. [Usage](#usage)
4. [API Reference](#api-reference)
5. [Implementation](#implementation)
6. [References](#references)

## Key Concepts

- **Worker**: A process running `ampd worker` that executes extraction jobs
- **Node ID**: Unique identifier for a worker instance (format: `^[a-zA-Z][a-zA-Z0-9_\-\.]*$`)
- **Heartbeat**: Periodic health signal sent by workers to the metadata database
- **Worker Info**: Build metadata including version, commit SHA, and build date

## Architecture

Workers register with the metadata database and maintain liveness through periodic heartbeats. The Admin API exposes worker information via the `SchedulerWorkers` trait.

### Data Flow

```
Worker → Heartbeat (1s) → Metadata DB ← Admin API ← ampctl/client
```

## Usage

**List all workers:**

View all registered workers and their last heartbeat times to get an overview of your worker fleet.

```bash
# Basic listing
ampctl worker list
ampctl worker ls  # alias

# Example output:
# worker-01h2xcejqtf2nbrexx3vqjhp41 (last heartbeat: 2025-01-15T17:20:15Z)
# indexer-node-1 (last heartbeat: 2025-01-15T17:18:45Z)
# eu-west-1a-worker (last heartbeat: 2025-01-15T17:20:10Z)
```

**Inspect specific worker:**

Get detailed information about a specific worker including build version, commit SHA, and lifecycle timestamps.

```bash
# Get detailed worker information
ampctl worker inspect worker-01
ampctl worker get worker-01  # alias

# Example output:
# Node ID: worker-01h2xcejqtf2nbrexx3vqjhp41
# Created: 2025-01-01T12:00:00Z
# Registered: 2025-01-15T16:45:30Z
# Heartbeat: 2025-01-15T17:20:15Z
#
# Worker Info:
#   Version: v0.0.22-15-g8b065bde
#   Commit: 8b065bde9c1a2f3e4d5c6b7a8e9f0a1b2c3d4e5f
#   Commit Timestamp: 2025-01-15T14:30:00Z
#   Build Date: 2025-01-15T15:45:30Z
```

**JSON output for scripting:**

Use JSON format to pipe output to jq or other tools for automated processing and monitoring.

```bash
# List workers as JSON
ampctl worker list --json

# Inspect worker as JSON
ampctl worker inspect worker-01 --json

# Extract specific fields with jq
ampctl worker list --json | jq -r '.workers[] | "\(.node_id): \(.heartbeat_at)"'
ampctl worker inspect worker-01 --json | jq -r '.info.version'
```

**Direct API access:**

Query the Admin API directly using curl for integrations or when ampctl is not available.

```bash
# List all workers
curl http://localhost:1610/workers

# Get worker details
curl http://localhost:1610/workers/worker-01
```

## API Reference

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/workers` | GET | List all workers |
| `/workers/{id}` | GET | Get worker details by node ID |

For request/response schemas, see [Admin API OpenAPI spec](../openapi-specs/admin.spec.json):

```bash
# List workers endpoint
jq '.paths["/workers"]' docs/openapi-specs/admin.spec.json

# Get worker endpoint
jq '.paths["/workers/{id}"]' docs/openapi-specs/admin.spec.json
```

## Implementation

### Database Schema

Workers are stored in the `workers` table:

| Column | Type | Description |
|--------|------|-------------|
| `node_id` | TEXT | Unique worker identifier |
| `heartbeat_at` | TIMESTAMPTZ | Last heartbeat timestamp |
| `created_at` | TIMESTAMPTZ | Initial registration |
| `registered_at` | TIMESTAMPTZ | Last registration |
| `info` | JSONB | Build metadata |

### Source Files

- `crates/services/admin-api/src/handlers/workers/` - API endpoint handlers
- `crates/clients/admin/src/workers.rs` - Client library
- `crates/core/metadata-db/src/workers.rs` - Database operations

## References

- [admin](admin.md) - Base: Administration overview
- [app-ampctl](app-ampctl.md) - Related: CLI tool
- [app-ampd-worker](app-ampd-worker.md) - Related: Worker process

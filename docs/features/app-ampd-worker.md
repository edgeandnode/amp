---
name: "app-ampd-worker"
description: "ampd worker for blockchain data extraction. Load when asking about workers, extraction jobs, or data ingestion"
type: feature
components: "app:ampd,service:worker,crate:config"
---

# ampd Worker

## Summary

The ampd worker executes scheduled extraction jobs in distributed deployments. Workers coordinate with the controller via the metadata database, enabling horizontal scaling of data extraction while keeping query serving separate.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Architecture](#architecture)
3. [Configuration](#configuration)
4. [Usage](#usage)
5. [References](#references)

## Key Concepts

- **Extraction Worker**: Process that executes data extraction jobs
- **Node ID**: Unique identifier for each worker instance
- **Heartbeat**: Periodic health signal sent to metadata database
- **Job Assignment**: Workers receive jobs via PostgreSQL LISTEN/NOTIFY

## Architecture

Workers operate in a coordination loop:

1. Register with metadata database using node ID
2. Maintain heartbeat every 1 second
3. Listen for job notifications
4. Execute assigned extraction jobs
5. Write Parquet files to configured storage
6. Update job status in database

For detailed deployment patterns, see [Operational Modes](../modes.md).

### Worker Coordination

| Mechanism | Description |
|-----------|-------------|
| Heartbeat | 1-second interval health signal |
| LISTEN/NOTIFY | PostgreSQL-based job notifications |
| State Reconciliation | 60-second periodic state sync |
| Graceful Resume | Jobs resume on worker restart |

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `node_id` | Required | Unique worker identifier |

### Environment Variables

```bash
# Worker-specific settings inherit from ampd configuration
export AMP_CONFIG_METADATA_DB_URL="postgresql://..."
```

### CLI Requirements and Directory Defaults

`ampd worker` requires `--config` (or `AMP_CONFIG`) to be provided, and `--node-id` is mandatory. Default `data`, `providers`, and `manifests` directory paths are resolved relative to the config file's parent directory only when the config file does not specify those paths. When the config file specifies `data_dir`, `providers_dir`, or `manifests_dir`, those values are used directly.

This command does not create directories itself; it relies on the configured paths and any downstream components to create or validate storage locations as needed.

## Usage

### Single Worker

```bash
# Start a worker with unique node ID
ampd worker --node-id worker-01
```

### Multiple Workers

```bash
# Distributed processing with multiple workers
ampd worker --node-id worker-01 &
ampd worker --node-id worker-02 &
ampd worker --node-id worker-03 &

# Descriptive node IDs for geographic distribution
ampd worker --node-id eu-west-1a-worker
ampd worker --node-id us-east-1b-worker
```

## References

- [app-ampd](app-ampd.md) - Base: ampd daemon overview
- [app-ampd-controller](app-ampd-controller.md) - Related: Job scheduling
- [Operational Modes](../modes.md) - Related: Deployment patterns

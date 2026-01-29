---
name: "app-ampd-controller"
description: "ampd controller for job scheduling and admin API. Load when asking about controller, job scheduling, or admin API"
type: feature
components: "app:ampd,service:controller,crate:config"
---

# ampd Controller

## Summary

The ampd controller provides the Admin API for managing Amp operations. It runs as a standalone service in distributed deployments, handling job scheduling, worker coordination, and administrative operations separate from query serving.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Architecture](#architecture)
3. [Configuration](#configuration)
4. [Usage](#usage)
5. [References](#references)

## Key Concepts

- **Admin API**: RESTful interface for managing Amp operations (port 1610)
- **Job Scheduling**: Orchestrates extraction jobs across workers
- **Worker Coordination**: Tracks worker health and assigns jobs

## Architecture

The controller provides centralized management for distributed deployments:

| Responsibility | Description |
|---------------|-------------|
| Admin API | RESTful management interface |
| Job Management | Create, monitor, stop extraction jobs |
| Worker Registry | Track worker heartbeats and status |
| Dataset Registry | Manage dataset definitions and versions |

For detailed deployment patterns, see [Operational Modes](../modes.md).

### Admin API Endpoints

| Endpoint | Description |
|----------|-------------|
| `/datasets` | Dataset management |
| `/jobs` | Job control and monitoring |
| `/workers` | Worker status |
| `/locations` | Storage locations |
| `/providers` | Data source configuration |

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `admin_addr` | `0.0.0.0:1610` | Admin API server binding |

### Config File

```toml
# .amp/config.toml
admin_addr = "0.0.0.0:1610"
```

### Environment Override

```bash
export AMP_CONFIG_ADMIN_ADDR="0.0.0.0:1610"
```

### CLI Requirements and Directory Defaults

`ampd controller` requires `--config` (or `AMP_CONFIG`) to be provided. Default `data`, `providers`, and `manifests` directory paths are resolved relative to the config file's parent directory only when the config file does not specify those paths. When the config file specifies `data_dir`, `providers_dir`, or `manifests_dir`, those values are used directly.

This command does not create directories itself; it relies on the configured paths and any downstream components to create or validate storage locations as needed.

## Usage

```bash
# Start the controller
ampd controller
```

### Admin API Examples

```bash
# List all datasets
curl http://localhost:1610/datasets

# Deploy a dataset
curl -X POST http://localhost:1610/datasets/my_namespace/eth_mainnet/versions/1.0.0/deploy

# List jobs
curl http://localhost:1610/jobs

# Check worker status
curl http://localhost:1610/workers
```

## References

- [app-ampd](app-ampd.md) - Base: ampd daemon overview
- [app-ampctl](app-ampctl.md) - Related: Admin CLI client
- [Operational Modes](../modes.md) - Related: Deployment patterns

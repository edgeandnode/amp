---
name: "app-ampd-solo"
description: "ampd solo mode for local development and testing. Load when asking about solo mode, development setup, or single-node deployments"
type: feature
components: "app:ampd,service:server,service:controller,service:worker"
---

# ampd Solo Mode

## Summary

Solo mode is ampd's single-node development mode that combines server, controller, and worker into a single process. It provides a simplified all-in-one deployment for local development, testing, and CI/CD pipelines. Solo mode automatically spawns an embedded worker and starts all query interfaces and admin API.

For detailed deployment patterns and when to use distributed modes, see [Operational Modes](../modes.md).

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Architecture](#architecture)
3. [Configuration](#configuration)
4. [References](#references)

## Key Concepts

- **Solo Mode**: Single-process mode combining all ampd services for development
- **Embedded Worker**: Automatically spawned worker with fixed node ID "worker"
- **Development Mode**: Designed for local testing, not production deployments
- **All-in-One**: Includes query server (Flight + JSONL), controller (Admin API), and extraction worker

## Architecture

Solo mode runs all three ampd services in a single process: query server (Flight + JSONL), controller (Admin API), and an embedded worker. The embedded worker has a fixed node ID "worker" and shares the same metadata database connection and datastore with the other services.

### Service Ports

| Service | Port | Protocol | Purpose |
|---------|------|----------|---------|
| Arrow Flight | 1602 | gRPC | High-performance queries |
| JSON Lines | 1603 | HTTP | Simple query interface |
| Admin API | 1610 | HTTP | Management operations |

### Initialization Sequence

1. Load configuration
2. Connect to metadata database
3. Create datastore (object storage)
4. Start controller (Admin API)
5. Start server (Flight + JSONL)
6. Spawn embedded worker with node ID "worker"
7. Worker registers and begins listening for jobs

## Configuration

### Default Behavior

By default, `ampd solo` starts all services:

```bash
# Starts Flight (1602) + JSONL (1603) + Admin API (1610) + embedded worker
ampd solo
```

### Optional Service Control

Disable specific interfaces using flags:

```bash
# Solo with only Flight queries (no JSONL, no Admin API)
ampd solo --flight

# Solo with Flight and Admin API (no JSONL)
ampd solo --flight --admin-server

# Solo with JSONL only (no Flight, no Admin API)
ampd solo --jsonl
```

**Note:** The embedded worker always runs regardless of flags.

### When to Use Solo Mode

**Use solo mode for:**
- Local development with fast iteration
- Testing the full extract-query pipeline
- CI/CD pipelines
- Learning Amp's capabilities
- Rapid prototyping

**Do NOT use solo mode for:**
- Production deployments
- Resource isolation (queries vs extraction)
- Horizontal scaling of workers
- High availability requirements
- Independent component failure handling

For complete deployment patterns, see [Operational Modes](../modes.md).

### Limitations

- **No Fault Isolation**: Worker crash terminates query server
- **Resource Contention**: Extraction competes with queries for CPU/memory
- **Single Worker Only**: Cannot scale extraction
- **Not Production-Ready**: Lacks robustness and high availability

## References

- [Operational Modes](../modes.md) - Related: Detailed deployment patterns
- [app-ampd](app-ampd.md) - Base: ampd daemon overview
- [app-ampd-server](app-ampd-server.md) - Related: Query server mode
- [app-ampd-controller](app-ampd-controller.md) - Related: Controller mode
- [app-ampd-worker](app-ampd-worker.md) - Related: Worker mode

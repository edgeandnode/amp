---
name: "app-ampd-solo-postgres"
description: "Auto-managed PostgreSQL for solo mode. Load when working with solo mode postgres management, metadata database lifecycle, or local dev database setup"
type: "component"
status: "unstable"
components: "service:metadata-db-postgres,app:ampd"
---

# Auto-Managed PostgreSQL for Solo Mode

## Summary

`ampd solo` automatically manages a local PostgreSQL instance with zero configuration. It uses system-installed binaries (`initdb`, `postgres`) to create and manage a database cluster, connecting via Unix sockets to avoid TCP port conflicts. Data persists across restarts, and the process shuts down gracefully on Ctrl+C or SIGTERM. No Docker, systemd, or manual setup required.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Configuration](#configuration)
3. [Architecture](#architecture)
4. [Prerequisites](#prerequisites)
5. [Limitations](#limitations)
6. [Implementation](#implementation)
7. [References](#references)

## Key Concepts

- **Managed PostgreSQL**: System-installed PostgreSQL binaries fully managed by ampd — startup, health checks, and graceful shutdown are automated
- **Data persistence**: On first run, `initdb` initializes a new database cluster; subsequent runs detect the existing cluster and reuse it
- **Unix socket connection**: PostgreSQL listens on a Unix domain socket inside the data directory, eliminating port conflicts and providing filesystem-level access control
- **Dev-mode defaults**: Non-durability settings applied automatically for local development performance (fsync off, synchronous_commit off, full_page_writes off, autovacuum off)
- **Structured concurrency**: The postgres service future participates in `tokio::select!` with other services, enabling immediate reaction to crashes, signals, and graceful shutdown coordination

## Configuration

### External Database Override

By default, `ampd solo` auto-manages a local PostgreSQL instance. To use an external database instead, provide the database URL via either:

- **Config file**: Set `metadata_db.url` in `config.toml`
- **Environment variable**: Set `AMP_CONFIG_METADATA_DB__URL`

When an external database URL is detected, managed PostgreSQL startup is skipped entirely. The provided URL is used directly for all metadata database operations.

This is useful for:
- Connecting to a shared development database
- Using a remote PostgreSQL instance
- Debugging with a persistent database that survives process restarts

## Architecture

### PostgreSQL Process Lifecycle

The managed PostgreSQL follows this lifecycle:

1. **Binary discovery**: Locates `initdb` and `postgres` binaries in PATH
2. **Orphan cleanup**: Detects and terminates leftover postgres processes from prior crashes by reading `postmaster.pid`
3. **Data directory setup**: Creates metadb directory with 0700 permissions if it does not exist
4. **Initialization**: If no `PG_VERSION` file exists, runs `initdb` with locale C, encoding UTF8, and trust authentication
5. **Server start**: Launches `postgres` with Unix socket in the data directory and dev-mode defaults
6. **Readiness probe**: Attempts Unix socket connection with exponential backoff (50ms to 2s, up to 60 retries, 30s total timeout)
7. **Graceful shutdown**: On signal or stop request, sends SIGTERM to postgres, waits for exit, falls back to SIGKILL if needed

### Connection URL Format

The managed postgres uses Unix socket connections:

```
postgresql:///postgres?host=/path/to/.amp/metadb
```

No TCP port is allocated. The socket file `.s.PGSQL.5432` is created inside the data directory by postgres.

### Dev-Mode PostgreSQL Defaults

These non-durability defaults are applied automatically for local development performance:

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| `fsync` | `off` | Skip WAL flush to disk for faster writes |
| `synchronous_commit` | `off` | Do not wait for WAL write confirmation |
| `full_page_writes` | `off` | Skip full-page images after checkpoint |
| `autovacuum` | `off` | Disable background vacuum for short-lived workloads |

These defaults match the [PostgreSQL non-durability recommendations](https://www.postgresql.org/docs/16/non-durability.html) and are appropriate for local development databases that can be recreated from scratch.

## Prerequisites

- **PostgreSQL 16+** installed on the system
- `initdb` and `postgres` binaries available in PATH
- `pg_isready` is not required (readiness verified via direct Unix socket connection)

## Limitations

- Requires PostgreSQL binaries installed on the host system (no embedded or bundled postgres)
- Dev-mode defaults disable durability guarantees; data may be lost on unclean shutdown (acceptable for local development)
- Unix socket path length is limited by the OS (typically 104-108 bytes); deeply nested amp dir paths may exceed this limit
- Single postgres instance per amp dir; concurrent `ampd solo` invocations sharing the same amp dir are not supported (orphan detection handles stale processes from prior runs)

## Implementation

### Source Files

- **`crates/services/metadata-db-postgres/src/postgres.rs`** — `PostgresBuilder` API, `PostgresProcess` lifecycle management (binary discovery, orphan cleanup, initdb, server start, readiness probe, graceful shutdown)
- **`crates/services/metadata-db-postgres/src/service.rs`** — `Handle` abstraction, `service::new()` convenience constructor with app defaults
- **`crates/bin/ampd/src/solo_cmd.rs`** — Solo command integration, structured concurrency with other services

## References

- [app-ampd-solo](app-ampd-solo.md) - Solo mode overview and service architecture
- [app-ampd](app-ampd.md) - ampd daemon overview

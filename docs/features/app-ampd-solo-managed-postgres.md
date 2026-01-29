---
name: "app-ampd-solo-managed-postgres"
description: "Zero-config persistent PostgreSQL for ampd solo local development. Load when working with solo mode, metadata database, .amp directory, or local postgres management"
type: "meta"
status: "unstable"
components: "service:metadata-db-postgres,crate:config,app:ampd"
---

# Managed PostgreSQL for Solo Mode

## Summary

`ampd solo` manages a local PostgreSQL instance automatically, storing all database state in `<cwd>/.amp/metadb/`. No Docker, no manual setup, and no configuration file is required. The managed postgres uses Unix socket connections (no TCP port conflicts), persists data across restarts by detecting existing data directories, and shuts down gracefully on Ctrl+C or SIGTERM.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Architecture](#architecture)
3. [Configuration](#configuration)
4. [Prerequisites](#prerequisites)
5. [Limitations](#limitations)
6. [References](#references)

## Key Concepts

- **Amp dir**: The `.amp/` directory inside the base directory where `ampd solo` is invoked. Stores all local state including the metadata database, parquet data, provider configs, and dataset manifests. Controlled via `--amp-dir` or `AMP_DIR` environment variable, defaulting to `<cwd>/.amp`
- **Managed PostgreSQL**: System-installed PostgreSQL binaries (`initdb`, `postgres`) managed by ampd. The process lifecycle (startup, health check, graceful shutdown) is fully automated
- **Data persistence**: On first run, `initdb` initializes a new database cluster. On subsequent runs, the existing cluster is reused by detecting the `PG_VERSION` file in the data directory. No data is lost between restarts
- **Unix socket connection**: PostgreSQL listens on a Unix domain socket inside the data directory (`.amp/metadb/.s.PGSQL.5432`) instead of a TCP port. This eliminates port conflicts and provides filesystem-level access control
- **Zero-config**: `ampd solo` works out of the box with no config file. All paths default to subdirectories of `.amp/`, and the metadata database starts automatically with dev-mode performance defaults

## Architecture

### Directory Structure

When `ampd solo` starts, it creates the following directory structure inside the amp dir:

```
<amp_dir>/
└── .amp/
    ├── config.toml     # Optional, auto-discovered if present
    ├── data/           # Parquet file storage
    ├── providers/      # Provider configuration files
    ├── manifests/      # Dataset manifest files
    └── metadb/         # PostgreSQL data directory
        ├── PG_VERSION  # Cluster version marker (skip initdb if present)
        ├── postmaster.pid  # Running process PID (orphan detection)
        └── .s.PGSQL.5432  # Unix domain socket
```

Directory creation is idempotent via `ensure_amp_directories()` in the config crate.

### PostgreSQL Process Lifecycle

1. **Binary discovery**: Locates `initdb` and `postgres` binaries in `PATH` using the `which` crate
2. **Orphan cleanup**: Reads `postmaster.pid` to detect and terminate leftover postgres processes from prior crashes
3. **Data directory setup**: Creates `.amp/metadb/` with `0700` permissions if it does not exist
4. **Initialization**: If no `PG_VERSION` file exists, runs `initdb` with locale `C`, encoding `UTF8`, and trust authentication
5. **Server start**: Launches `postgres` with Unix socket in the data directory and dev-mode defaults (fsync off, synchronous_commit off, full_page_writes off, autovacuum off)
6. **Readiness probe**: Attempts Unix socket connection with exponential backoff (50ms to 2s, up to 60 retries, 30s total timeout) using the `backon` crate
7. **Subprocess output**: Postgres stdout and stderr are piped to structured `tracing` logs (not inherited stdio)

### Structured Concurrency

The postgres background future participates in the solo command's `tokio::select!` alongside controller, server, and worker futures. This provides:

- **Unexpected exit detection**: If postgres crashes, the solo command reacts immediately
- **Signal handling**: The postgres service future listens for SIGINT and SIGTERM internally, matching the pattern used by controller, server, and worker services
- **Graceful shutdown**: On signal or stop request, sends SIGTERM to postgres, waits for exit, and falls back to SIGKILL if needed
- **Drop safety**: The `PostgresProcess` Drop impl sends SIGTERM synchronously before `kill_on_drop(true)` sends SIGKILL as a safety net

### PostgresBuilder API

The `metadata-db-postgres` crate exposes a builder API for configuring and starting postgres:

```rust
// Minimal (app defaults via service::new)
let (handle, fut) = service::new(data_dir).await?;

// Full control via builder
let (handle, fut) = PostgresBuilder::new(data_dir)
    .locale("C")
    .encoding("UTF8")
    .config_param("max_connections", "50")
    .config_param("shared_buffers", "128MB")
    .initdb_arg("--data-checksums", "")
    .bin_path("/usr/lib/postgresql/18/bin")
    .start()
    .await?;
```

Builder methods include `.locale()`, `.encoding()`, `.config_param()`, `.initdb_arg()`, and `.bin_path()`. Dev-mode server defaults (fsync off, synchronous_commit off, full_page_writes off, autovacuum off) are applied automatically and can be overridden via `.config_param()`.

## Configuration

### CLI and Environment

| Setting | Method | Example |
|---------|--------|---------|
| Base directory | `--amp-dir` flag | `ampd solo --amp-dir /path/to/project` |
| Base directory | `AMP_DIR` env var | `AMP_DIR=/path/to/project ampd solo` |
| Default | Current working directory | `ampd solo` uses `<cwd>/.amp` |

**Precedence**: CLI flag > environment variable > default (`<cwd>`)

### Config File

If `<amp_dir>/.amp/config.toml` exists, it is auto-discovered and loaded without needing a `--config` flag. When no config file is found, `Config::default_for_solo()` provides zero-config defaults for all paths and settings.

### Dev-Mode PostgreSQL Defaults

These non-durability defaults are applied automatically for local development performance:

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| `fsync` | `off` | Skip WAL flush to disk for faster writes |
| `synchronous_commit` | `off` | Do not wait for WAL write confirmation |
| `full_page_writes` | `off` | Skip full-page images after checkpoint |
| `autovacuum` | `off` | Disable background vacuum for short-lived workloads |

These defaults match the [PostgreSQL non-durability recommendations](https://www.postgresql.org/docs/16/non-durability.html) and are appropriate for local development databases that can be recreated from scratch.

### Connection URL Format

The managed postgres uses Unix socket connections:

```
postgresql:///postgres?host=/path/to/.amp/metadb
```

No TCP port is allocated. The socket file `.s.PGSQL.5432` is created inside the data directory by postgres.

## Prerequisites

- **PostgreSQL 16+** installed on the system
- `initdb` and `postgres` binaries available in `PATH`
- `pg_isready` is not required (readiness is verified via direct Unix socket connection)

## Limitations

- Requires PostgreSQL binaries installed on the host system (no embedded or bundled postgres)
- Dev-mode defaults disable durability guarantees; data may be lost on unclean shutdown (acceptable for local development)
- Unix socket path length is limited by the OS (typically 104-108 bytes); deeply nested amp dir paths may exceed this limit
- Single postgres instance per amp dir; concurrent `ampd solo` invocations sharing the same amp dir are not supported (orphan detection handles stale processes from prior runs)

## References

- [app-ampd-solo](app-ampd-solo.md) - Base: Solo mode overview and service architecture
- [app-ampd](app-ampd.md) - Base: ampd daemon overview

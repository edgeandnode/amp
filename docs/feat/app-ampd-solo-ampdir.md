---
name: "app-ampd-solo-ampdir"
description: ".amp directory structure and configuration for solo mode. Load when working with solo mode directory layout, config file discovery, or local state management"
type: "component"
status: "unstable"
components: "crate:config,app:ampd"
---

# .amp Directory Structure for Solo Mode

## Summary

`ampd solo` uses the `.amp/` directory to store local state: configuration, parquet data, provider configs, dataset manifests, and (when managed PostgreSQL is enabled) the metadata database. The location is controlled via `--amp-dir`/`AMP_DIR` environment variable, defaulting to `<cwd>/.amp`. Zero-config defaults are used when no config file is present; `config.toml` is auto-discovered if it exists inside the amp dir.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Architecture](#architecture)
3. [Configuration](#configuration)
4. [Limitations](#limitations)
5. [Implementation](#implementation)
6. [References](#references)

## Key Concepts

- **Amp dir**: The directory where all local state is stored — controlled via `--amp-dir`/`AMP_DIR`, defaulting to `<cwd>/.amp`
- **Zero-config defaults**: `ampd solo` works out of the box with no config file; all paths default to subdirectories of `.amp/`
- **Config auto-discovery**: If `<amp_dir>/config.toml` exists, it is loaded automatically without requiring a `--config` flag
- **Idempotent directory creation**: Required subdirectories are created if they do not exist; if they already exist, no changes are made
- **Managed metadb directory**: `<amp_dir>/metadb` is created only when managed PostgreSQL is used; if an external `metadata_db.url` is configured, managed PostgreSQL is skipped and the directory is not created

## Architecture

### Directory Structure

When `ampd solo` starts, it creates the following directory structure inside the amp dir. The `metadb/` directory is created only when managed PostgreSQL is enabled:

```
<amp_dir>/
├── config.toml     # Optional, auto-discovered if present
├── data/           # Parquet file storage
├── providers/      # Provider configuration files
├── manifests/      # Dataset manifest files
└── metadb/         # PostgreSQL data directory
    ├── PG_VERSION  # Cluster version marker (skip initdb if present)
    ├── postmaster.pid  # Running process PID (orphan detection)
    └── .s.PGSQL.5432  # Unix domain socket
```

Directory creation is idempotent. The metadb directory is created only when managed PostgreSQL is enabled.

## Configuration

### CLI and Environment

| Setting | Method | Example | Precedence |
|---------|--------|---------|------------|
| Amp directory | `--amp-dir` flag | `ampd solo --amp-dir /path/to/.amp` | 1 (highest) |
| Amp directory | `AMP_DIR` env var | `AMP_DIR=/path/to/.amp ampd solo` | 2 |
| Default | Current working directory | `ampd solo` uses `<cwd>/.amp` | 3 (lowest) |

**Precedence**: CLI flag > environment variable > default (`<cwd>`)

### Amp Dir and Config Precedence (Requirements)

The solo command follows these rules in order:

1. **Amp dir resolution**
   - If `--amp-dir` (or `AMP_DIR`) is provided, use that path as the amp dir.
   - Otherwise, use `<cwd>/.amp`.

2. **Config path resolution**
   - If `--config` is provided, load that file regardless of its location.
   - If no `--config` is provided, look for `<amp_dir>/config.toml`.
   - If no config file is found, run with zero-config defaults.

### Config File Behavior

- **Explicit `--config` flag**: Uses the specified config file (highest precedence), regardless of location
- **Auto-discovery**: If no `--config` flag is provided, looks for `<amp_dir>/config.toml`
- **Zero-config defaults**: If no config file is found, `ampd solo` runs with default settings for all paths and options

### Version Control Recommendation

It is recommended to add `.amp/` to `.gitignore` to avoid committing local state. Only check in `.amp/config.toml` if it contains non-default values that should be shared across developers.

Example `.gitignore`:

```gitignore
# Ignore all .amp/ local state
.amp/

# Optionally track config if it has non-default values
!.amp/config.toml
```

## Limitations

- Single-user local development only; not designed for multi-user or production use
- Concurrent `ampd solo` invocations on the same `.amp/` directory are not supported

## Implementation

### Source Files

- **`crates/config/src/lib.rs`** — `ensure_amp_dir_root()`, `ensure_amp_base_directories()`, and `ensure_amp_metadb_directory()` for idempotent directory creation, `load_config()` for config loading with `ConfigDefaultsOverride` defaults, `DEFAULT_AMP_DIR_NAME` constant
- **`crates/config/src/config_file.rs`** — Directory name constants (`DEFAULT_DATA_DIRNAME`, `DEFAULT_PROVIDERS_DIRNAME`, `DEFAULT_MANIFESTS_DIRNAME`)
- **`crates/config/src/metadb.rs`** — `DEFAULT_METADB_DIRNAME` constant
- **`crates/bin/ampd/src/main.rs`** — `--amp-dir` CLI argument parsing, `AMP_DIR` environment variable handling, config auto-discovery logic
- **`crates/bin/ampd/src/solo_cmd.rs`** — Receives resolved `amp_dir` and optional `config_path`, ensures directories before starting services

## References

- [app-ampd-solo](app-ampd-solo.md) - Solo mode overview and service architecture
- [app-ampd](app-ampd.md) - ampd daemon overview

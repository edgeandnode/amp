# SPEC.md - Amp Local Development: Persistent PostgreSQL

## Goal

Implement a zero-configuration, persistent PostgreSQL metadata database for
`ampd solo` that requires no Docker and stores all data in `.amp/`.

## Current State Analysis (Verified 2026-01-29 17:00 UTC)

### Phase 0 Status (Refactoring - Foundations)

| Task                                        | PR    | Status                         |
|---------------------------------------------|-------|--------------------------------|
| Rename `dev` subcommand to `solo`           | #1458 | ✅ Merged                       |
| Create `metadata-db-postgres` service crate | #1457 | ✅ Merged                       |
| Remove `temp.rs` from `metadata-db`         | #1457 | ✅ Merged                       |
| Decouple Store creation from Config loading | #1440 | ✅ Merged                       |
| Break circular deps with config types       | #1456 | ✅ Merged                       |
| Clean up config.rs temp DB spawning         | -     | ✅ Done (via singleton pattern) |

**Phase 0 is COMPLETE** - All foundation work has been merged.

### What Currently Exists

1. **`metadata-db-postgres` service crate** (`crates/services/metadata-db-postgres/src/service.rs:10-14`)
    - Current signature: `new(keep: bool) -> (Handle, impl Future<Output = ()>)`
    - Uses `PgTempDBBuilder::new().with_initdb_arg("locale", "C").persist_data(keep).start()`
    - Returns `Handle` with `url()` and `data_dir()` accessors
    - **Gap**: Uses random temp directory (no custom data dir support)
    - **Gap**: No reconnection logic for existing databases
    - **Decision**: Repurpose this crate as pgtemp replacement (see Task 1.1)

2. **Config system** (`crates/config/src/lib.rs`)
    - Global singleton `GLOBAL_METADATA_DB` (lines 32-51) with lazy initialization
    - `KEEP_TEMP_DIRS` env var controls persistence (default: false)
    - `Config::load()` (line 233) requires a file path - creates temp DB when `allow_temp_db=true`
    - **Gap**: `--config` flag is mandatory in `main.rs:169-171`
    - **Gap**: ConfigFile fields lack defaults (see detailed analysis below)

3. **ConfigFile field defaults** (`crates/config/src/lib.rs:138-190`)

| Field                | Has Default? | Current Behavior                           |
|----------------------|--------------|--------------------------------------------|
| `data_dir`           | ❌ NO         | Required in TOML                           |
| `providers_dir`      | ❌ NO         | Required in TOML                           |
| `poll_interval_secs` | ❌ NO         | Required in TOML                           |
| `manifests_dir`      | ✅ YES        | `#[serde(default)]` → empty string         |
| `max_mem_mb`         | ✅ YES        | `#[serde(default)]` → 0                    |
| `query_max_mem_mb`   | ✅ YES        | `#[serde(default)]` → 0                    |
| `metadata_db`        | ✅ YES        | `#[serde(default)]` with sensible defaults |
| Server addresses     | ✅ YES        | Default impl: `0.0.0.0:1602/1603/1610`     |

4. **Solo command** (`crates/bin/ampd/src/solo_cmd.rs`, `main.rs:107`)
    - Calls `load_config(config_path, allow_temp_db=true)`
    - **Gap**: `load_config()` (line 169-171) rejects `None` config path with "mandatory" error

5. **Test fixtures** (`tests/src/testlib/fixtures/daemon_state_dir.rs:26-38`)
    - Constants: `DEFAULT_STATE_DIRNAME = ".amp"`, `DEFAULT_CONFIG_FILENAME = "config.toml"`
    - Subdirs: `manifests/`, `providers/`, `data/`
    - **Currently test-only** - duplicated in production files:
        - `crates/bin/ampup/src/config.rs` - hardcodes `.amp` for binary management
        - `crates/bin/ampcc/src/auth/domain.rs` - hardcodes `.amp/cache/amp_cli_auth`

### Key Gaps Blocking Progress

| Gap                                                | Impact                            | Blocking | Status       |
|----------------------------------------------------|-----------------------------------|----------|--------------|
| pgtemp cannot reuse existing data dirs             | Need custom postgres management   | Phase 1  | ✅ Understood |
| Need pgtemp replacement in `metadata-db-postgres`  | Cannot persist data across runs   | Phase 1  | ✅ Task 1.1  |
| No graceful shutdown/error handling for postgres   | Orphan processes, poor UX         | Phase 1  | 🔴 Task 1.2  |
| Config system doesn't pass data_dir to metadata_db | Can't specify persistent location | Phase 1  | ⬚ Task 1.3   |
| `--config` is mandatory in `main.rs`               | Cannot run zero-config            | Phase 2  | ⬚ Task 2.1   |
| `ConfigFile` fields lack defaults                  | Must specify all paths            | Phase 2  | ⬚ Task 2.2   |
| No auto-creation of `.amp/` directory structure    | Manual setup required             | Phase 2  | ⬚ Task 2.3   |

---

## Tasks

### Phase 1: Persistent pgtemp

**Goal**: Make `ampd solo` persist PostgreSQL data across restarts using system PostgreSQL.

**Note**: Phase 1 still requires PostgreSQL to be installed on the system (it uses the system's `initdb` and
`postgres` binaries via pgtemp).

#### 1.0 ✅ COMPLETE: Investigate pgtemp reconnection behavior

- [x] Analyzed pgtemp v0.7.1 API documentation (docs.rs)
- [x] Verified: `with_data_dir_prefix()` creates NEW temp subdirectory, doesn't reuse existing
- [x] Verified: pgtemp ALWAYS runs `initdb` - no skip logic for existing `PG_VERSION`
- [x] Documented findings in "Resolved Questions" section
- [x] **Decision**: Cannot use pgtemp for persistence - must use Option C (manual postgres management)
- **Status**: COMPLETE (2026-01-29)

#### 1.1 ✅ COMPLETE: Repurpose `metadata-db-postgres` with direct system PostgreSQL management

**Status**: COMPLETE (2026-01-29)

**Rationale**: pgtemp cannot reuse existing data directories (see Task 1.0). Replaced pgtemp with
direct management of system-installed PostgreSQL binaries (`initdb`, `postgres`).

**Implementation Summary**:

- Created `error.rs` with `PostgresError` enum (BinaryNotFound, InitDbFailed, StartFailed, etc.)
- Created `postgres.rs` with `PostgresProcess` struct for async lifecycle management
- Implemented PG_VERSION detection to skip initdb for existing data directories
- Configured Unix socket connections in data directory (no port conflicts)
- Updated `service::new(data_dir: PathBuf) -> Result<..., PostgresError>` API
- Removed `pgtemp` dependency, added `thiserror` and tokio process features
- Updated config crate and test fixtures to use new API

**Module Structure**:

```
crates/services/metadata-db-postgres/src/
├── lib.rs          # Public API re-exports, PostgresError
├── service.rs      # service::new(data_dir) function
├── postgres.rs     # PostgresProcess struct - lifecycle management
└── error.rs        # PostgresError enum with typed variants
```

**Implementation Sub-Tasks**:

- [x] **1.1.1**: Create `error.rs` with error types
- [x] **1.1.2**: Create `postgres.rs` with `PostgresProcess` struct
- [x] **1.1.3**: Implement data directory detection (PG_VERSION check)
- [x] **1.1.4**: Implement unix socket connections
- [x] **1.1.5**: Update `service.rs` with new API signature
- [x] **1.1.6**: Update `Cargo.toml` (remove pgtemp, add thiserror/tokio features)
- [x] **1.1.7**: Update `lib.rs` documentation

**Feature Requirements**:

1. **Replace pgtemp with direct postgres management**
    - Remove `pgtemp` dependency from Cargo.toml
    - Implement direct calls to system `initdb`, `postgres`, `pg_ctl` binaries
    - Use `tokio::process::Command` for async process spawning

2. **Data directory reuse** (KEY DIFFERENCE from pgtemp)
    - Accept explicit data directory path (required for `ampd solo`)
    - If `PG_VERSION` file exists in data directory → skip initdb, reuse existing data
    - If data directory empty or doesn't exist → run initdb
    - `.amp/` directory is always persistent (never a temp directory)

3. **Updated API**
    - Change `service::new(keep: bool)` to
      `service::new(data_dir: PathBuf) -> Result<(Handle, impl Future), PostgresError>`
    - The `keep` parameter is no longer needed since data is always persistent
    - Return `Handle` with `url()` and `data_dir()` as before
    - **NEW**: Return `Result` to handle startup failures gracefully

4. **Postgres lifecycle management**
    - Dynamic port allocation (unix socket or TCP port 0)
    - Start postgres process with socket in data directory
    - Wait for readiness before returning
    - Graceful shutdown on drop (send SIGTERM to postgres process)

5. **Unix socket connections** (KEY REQUIREMENT)
    - Start postgres with unix sockets located inside `.amp/metadb/` directory
    - Use `ipc:/` URLs (not `tcp://`) to connect to the postgres metadata DB
    - Example socket path: `.amp/metadb/.s.PGSQL.5432`
    - Connection URL format: `postgresql:///postgres?host=/path/to/.amp/metadb`
    - Benefits: No port conflicts, better security (filesystem permissions), slightly lower latency

**Breaking Change Note**: The signature of `service::new()` changes from `new(keep: bool)` to
`new(data_dir: PathBuf) -> Result<..., PostgresError>`. Call sites need updating:

| Location                                       | Current Call                          | New Call                                 |
|------------------------------------------------|---------------------------------------|------------------------------------------|
| `crates/config/src/lib.rs:42-50`               | `service::new(*KEEP_TEMP_DIRS)`       | `service::new(metadb_data_dir)?`         |
| `tests/src/testlib/fixtures/metadata_db.rs:40` | `service::new(*TESTS_KEEP_TEMP_DIRS)` | `service::new(temp_dir.join("metadb"))?` |

- **Acceptance**:
    - Data persists in `.amp/metadb/` across restarts
    - No data loss on restart
    - Skip initdb when reusing existing data directory
    - Clean error messages when PostgreSQL binaries not found
    - Unix socket connection works without port conflicts
- **Files**: `crates/services/metadata-db-postgres/`
- **Depends on**: 1.0 ✅

#### 1.2 ✅ COMPLETE: Add robustness features (health checks, orphan detection, logging)

**Status**: COMPLETE (2026-01-29)

**Implementation Summary**:

- [x] **Health check with pg_isready**: Verify postgres accepts connections before returning URL
    - Uses `pg_isready` command for proper connection verification
    - Falls back to socket file check if pg_isready not available
    - Polls every 100ms with 30-second timeout

- [x] **Orphan process detection and cleanup**: Check for existing postgres on startup
    - Reads `postmaster.pid` to detect orphan processes
    - Sends SIGTERM to orphan process, waits up to 5 seconds
    - Force kills (SIGKILL) if process doesn't respond
    - Cleans up stale socket and pid files

- [x] **Logging improvements**:
    - Log PostgreSQL version on startup via `postgres --version`
    - Log data directory location at startup and ready state
    - Log connection URL when database is ready
    - Log whether database is fresh or reusing existing data

**Note**: Process signal handling at ampd level (SIGINT/SIGTERM registration) is deferred to
Phase 2 as it requires changes outside the metadata-db-postgres crate.

- **Acceptance**: ✅ Postgres reliably starts/stops; orphan processes detected and cleaned up; clear diagnostics
- **Files**: `crates/services/metadata-db-postgres/src/postgres.rs`
- **Depends on**: 1.1 ✅

#### 1.3 ✅ COMPLETE: Wire persistence into config system

**Status**: COMPLETE (2026-01-29)

**Implementation Summary**:

- [x] **1.3.1**: Updated `global_metadata_db()` function:
    - Changed signature from `(keep: bool)` to `(data_dir: PathBuf)`
    - Calls `service::new(data_dir)?` with proper error handling
    - Returns `Result` with `PostgresError`

- [x] **1.3.2**: Removed `KEEP_TEMP_DIRS` static:
    - Deleted from config crate
    - Persistence is now implicit (always enabled)

- [x] **1.3.3**: Added `metadb_data_dir` computation in `Config::load()`:
    - Derives data_dir from config file location: `<config_parent>/.amp/metadb/`
    - Added `Config::default_for_solo()` for zero-config mode using `.amp-local/metadb/`

- [x] **1.3.4**: Updated metadata DB resolution call sites:
    - `Config::load()` passes computed `metadb_data_dir` to `global_metadata_db()`
    - `Config::default_for_solo()` uses `.amp-local/metadb/` path
    - Added `ConfigError::PostgresStartup` variant for error propagation

- [x] **1.3.5**: Updated test fixtures:
    - `tests/src/testlib/fixtures/metadata_db.rs` now uses `with_data_dir()` method
    - Creates unique temp directory per test: `amp-test-metadb-{pid}`

- [x] **1.3.6**: `TESTS_KEEP_TEMP_DIRS` retained:
    - Still used for other test artifacts (not just postgres)
    - Located in `tests/src/testlib/debug.rs`

- **Acceptance**: ✅ Config system creates persistent DB in `.amp/metadb/`; all tests pass
- **Files**: `crates/config/src/lib.rs`, `tests/src/testlib/fixtures/metadata_db.rs`
- **Depends on**: 1.1 ✅

---

### Phase 2: Zero-Config Experience

**Goal**: `ampd solo` works with no `--config` argument and no config file.

#### 2.1 Make config file optional for solo mode

**Current State** (from gap analysis):

- `Command::Solo` at `main.rs:26-40` has NO `--amp-dir` argument, only server flags
- `load_config()` at `main.rs:169-171` returns hard error "mandatory" when config_path is None
- `Config::load()` at `lib.rs:233-238` requires `impl Into<PathBuf>` (NOT optional)

**Changes Required**:

- [ ] Add `--amp-dir <PATH>` CLI argument to `Command::Solo` in `main.rs:26-40`:
    ```rust
    #[arg(long, env = "AMP_DIR")]
    amp_dir: Option<PathBuf>,
    ```
- [ ] Update `load_config()` at `main.rs:165-180`:
    - Accept optional `amp_dir` parameter
    - When `allow_temp_db=true` and `config_path=None`:
        - Check if `<amp_dir>/.amp/config.toml` exists
        - If exists: load it
        - If not exists: use in-memory defaults
    - Remove "mandatory" error for solo mode
- [ ] Modify `Config::load()` signature to accept `Option<impl Into<PathBuf>>`
- [ ] Create default `ConfigFile` when file path is None:
    ```rust
    ConfigFile {
        data_dir: ".amp/data".into(),
        providers_dir: ".amp/providers".into(),
        manifests_dir: ".amp/manifests".into(),
        poll_interval_secs: ConfigDuration::new(60),
        // ... other fields use their defaults
    }
    ```
- [ ] Handle path resolution when no config file (use amp_dir as base)
- **Acceptance**:
    - `ampd solo` starts without `--config` argument, uses `./.amp/` by default
    - `ampd solo --amp-dir /path/to/project` uses `/path/to/project/.amp/`
    - `AMP_DIR=/path/to/project ampd solo` uses `/path/to/project/.amp/`
    - If `.amp/config.toml` exists, it is automatically loaded without `--config` flag
- **Files**: `crates/bin/ampd/src/main.rs`, `crates/config/src/lib.rs`
- **Depends on**: Phase 1 complete

#### 2.2 Define sensible defaults for all paths

**Current State** (from gap analysis at `lib.rs:138-190`):

| Field                | Has `#[serde(default)]`? | Current Default      |
|----------------------|--------------------------|----------------------|
| `data_dir`           | ❌ NO                     | **REQUIRED in TOML** |
| `providers_dir`      | ❌ NO                     | **REQUIRED in TOML** |
| `poll_interval_secs` | ❌ NO                     | **REQUIRED in TOML** |
| `manifests_dir`      | ✅ YES                    | Empty string `""`    |
| `max_mem_mb`         | ✅ YES                    | `0`                  |
| `metadata_db`        | ✅ YES                    | Has Default impl     |

**Changes Required**:

- [ ] Add default functions for required ConfigFile fields:
    - `fn default_data_dir() -> String { ".amp/data".into() }`
    - `fn default_providers_dir() -> String { ".amp/providers".into() }`
    - `fn default_poll_interval_secs() -> ConfigDuration<1> { ConfigDuration::new(60) }`
- [ ] Add `#[serde(default = "default_*")]` to these fields in `ConfigFile` struct
- [ ] Default metadata DB data to `.amp/metadb/data/` (in 1.3 wiring)
- [ ] Fix `manifests_dir` default to `.amp/manifests/` (currently empty string)
- **Acceptance**: All paths resolve to `.amp/` subdirectories by default
- **Files**: `crates/config/src/lib.rs:138-190`

#### 2.3 Auto-create directory structure on first run

- [ ] Resolve base directory from `--amp-dir` / `AMP_DIR` / cwd (see 2.1)
- [ ] Add shared constants to `config` crate (e.g., `DEFAULT_STATE_DIRNAME = ".amp"`)
- [ ] Create `<base>/.amp/` directory if it doesn't exist
- [ ] Create subdirectories: `data/`, `providers/`, `manifests/`, `metadb/data/`
- [ ] Log directory creation with `tracing::info!("Created .amp directory at: {}", path)`
- **Acceptance**: No manual directory creation required; directories created at resolved location
- **Files**: `crates/config/src/lib.rs`

#### 2.4 Update E2E test framework for `--amp-dir`

- [ ] Update test fixtures to pass `--amp-dir` pointing to test case temp directory
- [ ] Ensure each test case gets isolated `.amp/` directory inside its temp dir
- [ ] Update `daemon_state_dir.rs` fixtures to use new `--amp-dir` argument
- [ ] Remove any test-specific workarounds for config file requirements
- **Acceptance**: E2E tests use `--amp-dir <temp_dir>` for isolation; no shared state between tests
- **Files**: `tests/src/testlib/fixtures/daemon_state_dir.rs`, related test fixtures
- **Depends on**: 2.1

---

## Task Priority Order

**Phase 1: Persistent PostgreSQL** ✅ COMPLETE

1. **1.0** ✅ COMPLETE - Investigate pgtemp reconnection behavior (pgtemp cannot reuse data dirs)
2. **1.1** ✅ COMPLETE - Replace pgtemp with direct system PostgreSQL management
    - Implemented: error types, postgres process, data dir, sockets, service, Cargo, docs
    - ~700 lines of new code
3. **1.2** ✅ COMPLETE - Add robustness features (health checks, orphan detection, logging)
    - Health check via `pg_isready`, orphan process cleanup, version/path logging
4. **1.3** ✅ COMPLETE - Wire persistence into config system
    - Config crate updated, test fixtures updated, `KEEP_TEMP_DIRS` removed

**Phase 2: Zero-Config Experience** (current focus)

5. **2.1** 🔴 NEXT - Make config file optional for solo mode (add `--amp-dir` CLI option)
6. **2.2** ⬚ PENDING - Define sensible defaults for all config paths
7. **2.3** ⬚ PENDING - Auto-create `.amp/` directory structure on first run
8. **2.4** ⬚ PENDING - Update E2E test framework for `--amp-dir` isolation

---

## Dependencies

**External Dependencies** (system requirements):

- **PostgreSQL 16+** must be installed on the system
    - Binaries required in PATH: `initdb`, `postgres`, `pg_isready`
    - Optional: `pg_ctl` (not currently used)
    - Tested with PostgreSQL 16, 17, 18 (prefer 18 per issue #1383)

**Crate Dependency Changes** (Task 1.1.6 - IMPLEMENTED):

| Change | Crate       | Reason                                      |
|--------|-------------|---------------------------------------------|
| REMOVE | `pgtemp`    | Replaced with direct postgres management    |
| ADD    | `thiserror` | Error type derive macros (workspace dep)    |
| MODIFY | `tokio`     | Add features: `process`, `fs`, `time`       |

**Implementation Note**: Used `kill` command via `tokio::process::Command` for SIGTERM instead of adding `nix` crate - simpler approach with no new dependencies.

**Existing workspace dependencies** (used):

- `tokio` - already in workspace (added features to crate's Cargo.toml)
- `tracing` - already used for logging
- `thiserror` - ✅ already in workspace (`Cargo.toml:115`)

**Phase Dependencies**:

- **Phase 1**: System PostgreSQL installed
- **Phase 2**: Phase 1 complete

---

## Blockers / Open Questions

### Active Blockers

*None* - pgtemp investigation is complete (see Resolved Questions)

### Decisions Needed

*All decisions for Phase 1 are resolved. See Resolved Questions section.*

### Previously Resolved Decisions

1. ~~**pgtemp replacement strategy**~~ → **RESOLVED** (see Resolved Questions)
2. ~~**Working directory resolution**~~ → **RESOLVED** (see Resolved Questions)
3. ~~**Config precedence**~~ → **RESOLVED** (see Resolved Questions)
4. ~~**Shared constants**~~ → **RESOLVED** (see Resolved Questions)

### Resolved Questions

- **pgtemp replacement strategy** (Resolved 2026-01-29):
    - **Decision**: Use direct system PostgreSQL management via tokio async APIs
    - **Rationale**: PostgreSQL is expected to be installed on the system; async process
      management via `tokio::process::Command` integrates naturally with the existing async codebase
    - **Approach**: Invoke system `initdb`, `postgres`, `pg_ctl` binaries directly
    - **Prerequisite**: PostgreSQL must be installed and binaries available in PATH
- **Shared constants** (Resolved 2026-01-29):
    - Put directory name constants in the `config` crate (not a separate `amp-paths` crate)
    - Other crates (`ampup`, `ampcc`, tests) should depend on `config` for these constants
- **Config precedence** (Resolved 2026-01-29):
    - Built-in defaults → `.amp/config.toml` (if present) → env vars
    - If `.amp/config.toml` exists in the resolved directory, it is automatically loaded and prevails over defaults
- **`.amp` directory location** (Resolved 2026-01-29):
    - **CLI option**: `--amp-dir <PATH>` on `ampd solo` command
    - **Environment variable**: `AMP_DIR`
    - **Default**: Current working directory where command was invoked (i.e., `./.amp/`)
    - **Precedence**: CLI option → env var → default (cwd)
    - **Rationale**: Follows `.gradle/` convention - project-local by default, but overridable for
      non-standard setups or when running from different directories
- **PostgreSQL version**: Prefer PostgreSQL 18 if available (per issue #1383)
- **Subcommand name**: Using `solo` (renamed from `dev` in #1458)
- **pgtemp crate version**: Was v0.7.1 - **being replaced** (see pgtemp replacement strategy below)
- **pgtemp reconnection behavior** (Resolved 2026-01-29 via docs.rs analysis):
    - **Finding**: pgtemp v0.7.1 does NOT support skipping `initdb` for existing directories
    - **Available methods**:
        - `with_data_dir_prefix(path)` - sets parent dir for temp subdirectory, always creates NEW subdir
        - `persist_data(bool)` - prevents deletion on drop, but doesn't enable reuse on restart
        - `load_database(path)` - loads SQL script via `psql` (schema/data dump, not raw data dir)
        - `dump_database(path)` - exports via `pg_dump` on shutdown
    - **Implication**: pgtemp always creates fresh temp subdirectory and runs `initdb`
    - **Decision**: Need alternative approach for true persistence - Option C (manual postgres) is recommended
- **pgtemp replacement implementation** (Resolved 2026-01-29):
    - **Decision**: Replace pgtemp with direct system PostgreSQL management in `metadata-db-postgres`
    - **Rationale**: System PostgreSQL is a prerequisite; direct management is simpler than adding dependencies
    - **Key difference from pgtemp**: Accepts explicit data directory, checks `PG_VERSION` to skip initdb
    - **Persistence model**: `.amp/` is always persistent (no temp mode, no `keep` parameter)

---

## Notes

- Multi-project support: each project has its own `.amp/` directory
- External tables can bridge to shared daemons for large datasets
- Test fixtures in `daemon_state_dir.rs` provide patterns for directory management
- `ampup` uses separate `~/.amp/` directory for binary management (different purpose)

---

## Verification Log

### 2026-01-29 17:00 UTC - Gap Analysis Verified

**Files verified against SPEC**:

| File                                                  | Status     | Notes                                                |
|-------------------------------------------------------|------------|------------------------------------------------------|
| `crates/services/metadata-db-postgres/src/service.rs` | ✅ Accurate | Still uses pgtemp, signature `new(keep: bool)`       |
| `crates/services/metadata-db-postgres/Cargo.toml`     | ✅ Accurate | pgtemp.workspace = true                              |
| `crates/config/src/lib.rs`                            | ✅ Accurate | KEEP_TEMP_DIRS lines 26-30, global_metadata_db 42-51 |
| `crates/bin/ampd/src/main.rs`                         | ✅ Accurate | --config mandatory at lines 169-171                  |
| `tests/src/testlib/fixtures/metadata_db.rs`           | ✅ Accurate | Uses TESTS_KEEP_TEMP_DIRS                            |
| Root `Cargo.toml`                                     | ✅ Verified | thiserror=2.0 (line 115), pgtemp=0.7.1 (line 99)     |

**Key findings**:

- All gaps documented in SPEC are still present
- No blockers identified
- nix dependency may be avoidable using tokio signals (added note)
- Task 1.1 is ready to begin implementation

# Dataset Authoring - IPC Schema + `tables/` Refactor Plan

## Summary

Refactor dataset-authoring to use Arrow IPC **file format** for schemas and move build artifacts under `tables/`. Rename the authoring config field from `models` to `tables`. Keep Amp core unchanged by converting **legacy manifests → canonical package** in the dataset-authoring adapter layer when fetching from the admin API, and converting **package → legacy manifest JSON** in memory for `ampctl dataset register --package`. No backwards compatibility with old `sql/` or `*.schema.json` outputs.

## Decisions

- **Schema format**: Arrow IPC **file format** (`.ipc`), not JSON.
- **Build output layout**:
  - `tables/<table>.sql` for derived datasets only
  - `tables/<table>.ipc` for all tables
  - `functions/<name>.js` unchanged
- **Authoring config**: rename `models` → `tables` (default `tables`).
- **Manifest table shape**:
  - Derived table: `tables.<table>.sql` + `tables.<table>.ipc` + `network`
  - Raw table: `tables.<table>.ipc` + `network` (no `sql` field)
- **Cache**: `~/.amp/registry/<legacy-hash>/` stores canonical package format.
- **Interop**:
  - **Admin API fetch**: legacy manifest JSON → canonical package (adapter).
  - **Register**: package → legacy manifest JSON in memory (adapter).
- **No backwards compatibility** with old `sql/` + `*.schema.json` outputs.

---

## Status

| Phase | Description | Status |
|-------|-------------|--------|
| 1 | Arrow IPC Module | **Complete** |
| 2 | Rename `models` → `tables` | **Complete** |
| 3 | Build Output Layout (`sql/` → `tables/`) | **Complete** |
| 4 | Schema Type Refactor | **Complete** |
| 5 | Manifest Table Shape Changes | **Complete** |
| 6 | Adapter Layer (Legacy ↔ Package) | **Complete** |
| 7 | Cache Updates | **Complete** |
| 8 | Documentation & Tests | **Complete** |

---

## Gap Analysis

Based on codebase exploration (2026-02-04, verified via code search):

### Currently Implemented

| Component | Location | Details |
|-----------|----------|---------|
| Config parsing | `config.rs:87-118` | `AmpYaml` with `models: PathBuf` field, default via `default_models_dir()` returning `"models"` |
| Model discovery | `discovery.rs:97-162` | `discover_models()` scans `<models_dir>/**/*.sql`, returns `BTreeMap<TableName, PathBuf>` |
| Build output | `manifest.rs:406,415` | `sql/<table>.sql` and `sql/<table>.schema.json` |
| Schema files | `arrow_json.rs` | JSON format using `ArrowSchema` from `datasets_common::manifest` |
| Package assembly | `package.rs:175-184` | Includes `sql/` and `functions/` directories |
| Cache | `cache.rs:131-133` | Stores `<hash>/manifest.json` only (no SQL/schema files) |
| Bridge | `bridge.rs` | Converts `AuthoringManifest` → legacy runtime format |
| Jinja | `jinja.rs` | `ref`, `source`, `var`, `env_var`, `this` template helpers |
| SQL validation | `query.rs` | SELECT-only, incremental mode constraints |
| Lockfile | `lockfile.rs` | `amp.lock` for reproducible dependency resolution |
| Validation | `validation.rs:105` | `discovered_models: BTreeMap<TableName, PathBuf>` |
| TableDef | `manifest.rs:168-176` | Has `sql: FileRef`, `schema: FileRef`, `network: NetworkId` |
| Playground | `playground/` | Uses `models/` dir, builds to `build/sql/` |

### Not Yet Implemented

| Feature | Current State | Target State |
|---------|---------------|--------------|
| Arrow IPC I/O | `arrow_ipc.rs` module with `write_ipc_schema()`, `read_ipc_schema()` | **Complete** |
| Build output dir | `sql/` | `tables/` |
| Schema format | `.schema.json` (JSON) | `.ipc` (Arrow IPC file) |
| Config field | `models:` (default `"models"`) | `tables:` (default `"tables"`) |
| Raw table support | `sql` field required | `sql: Option<FileRef>` |
| Cache format | `manifest.json` only | Full package: `manifest.json` + `tables/` + `functions/` |
| Fetch adapter | N/A | Legacy manifest → canonical package conversion |
| Register adapter | N/A | Package → legacy manifest JSON for API |

---

## Tasks

### Phase 1: Arrow IPC Module (Foundational)

**Files**: `arrow_ipc.rs` (new), `lib.rs` (1 line)

**1.1) Create `arrow_ipc.rs` module**
- [x] Add new module `crates/core/dataset-authoring/src/arrow_ipc.rs`
- [x] Implement `write_ipc_schema(schema: &SchemaRef, path: &Path) -> Result<()>`
  - Uses Arrow IPC FileWriter with schema-only (no record batches)
- [x] Implement `read_ipc_schema(path: &Path) -> Result<SchemaRef>`
  - Uses Arrow IPC FileReader to read schema metadata
- [x] Add comprehensive tests for round-trip serialization
- [x] Export from `lib.rs`

**Acceptance criteria**: Can write Arrow `Schema` to `.ipc` file and read it back losslessly. **VERIFIED**

---

### Phase 2: Rename `models` → `tables` (Config Change)

**Files**: `config.rs`, `discovery.rs`, `validation.rs`, CLI commands, integration tests

**2.1) Update `config.rs`**
- [x] Rename field `models: PathBuf` to `tables: PathBuf` (line 109 in `AmpYaml`)
- [x] Update default function `default_models_dir()` → `default_tables_dir()` returning `"tables"` (line 156-158)
- [x] Update `AmpYamlV1` struct similarly (line 141)
- [x] Update `validate()` to reference "tables directory" instead of "models directory" (line 208)
- [x] Update all tests using `models` field (lines 507-852 test module)

**2.2) Update `discovery.rs`**
- [x] Rename function `discover_models()` → `discover_tables()` (line 97)
- [x] Update variable name `models` → `tables` in function body (line 108)
- [x] Update `DiscoveryError` variants: `DuplicateModelName` → `DuplicateTableName`, etc.
- [x] Update `DiscoveredModel` → `DiscoveredTable` struct
- [x] Update documentation and error messages throughout

**2.3) Update call sites**
- [x] Update `validation.rs` field `discovered_models` → `discovered_tables` (line 105 in `ValidationResult`)
- [x] Update `validate_network_inference()` parameters (line 591)
- [x] Update all callers of discovery functions in validation/build flows
- [x] Update CLI commands (check.rs, build.rs) to use new field/function names
- [x] Update integration tests (it_dataset_authoring.rs) with new paths and imports

**Acceptance criteria**: `amp.yaml` accepts `tables:` field (with `tables` as default). `models:` is no longer recognized. **VERIFIED**

---

### Phase 3: Build Output Layout (`sql/` → `tables/`)

**Files**: `manifest.rs`, `package.rs`, `bridge.rs`, `arrow_json.rs`, CLI help text

**3.1) Update `manifest.rs` output paths**
- [x] Change SQL file path from `sql/<table>.sql` to `tables/<table>.sql` (line 406)
- [x] Change schema file path from `sql/<table>.schema.json` to `tables/<table>.ipc` (line 415)
- [x] Update `sql_dir` parameter naming throughout to `tables_dir`
- [x] Update `ManifestBuilder` field `sql_dir: &'a Path` → `tables_dir: &'a Path` (line 286)
- [x] Update `ManifestBuilder::new()` parameter (line 306)
- [x] Update all test fixtures using `sql/` paths (lines 819-934 tests)

**3.2) Update `package.rs`**
- [x] Change directory inclusion from `sql/` to `tables/` (lines 175-178)
- [x] Update `from_directory()` to look for `tables/` instead of `sql/`
- [x] Update all test fixtures and assertions (lines 613-651 tests)

**3.3) Update `bridge.rs`**
- [x] Update all path references from `sql/` to `tables/`
- [x] Update test fixtures to use IPC schema files (completed in Phase 6)

**3.4) Update `arrow_json.rs` → deprecate or remove**
- [x] After IPC is working, remove JSON schema write calls from build flow
- [ ] Keep `arrow_json.rs` only if needed for legacy adapter layer in Phase 6 (confirmed needed)

**Acceptance criteria**: `ampctl dataset build` produces `tables/<table>.sql` + `tables/<table>.ipc`, no `sql/` directory. **VERIFIED**

---

### Phase 4: Schema Type Refactor (Arrow-native)

**Files**: `schema.rs`, `validation.rs`, `dependency_manifest.rs`, build commands

**4.1) Update schema inference in `schema.rs`**
- [x] Change return type from `TableSchema` to Arrow `SchemaRef`
- [x] Remove intermediate `TableSchema`/`ArrowSchema` conversions
- [x] Update `SchemaContext::infer_schema()` to return `SchemaRef` directly

**4.2) Update build pipeline**
- [x] Write inferred schemas using `arrow_ipc::write_ipc_schema()` instead of JSON
- [x] Update validation output to use Arrow types (`ValidatedTable.schema: SchemaRef`)
- [x] Build command now uses `SchemaRef` directly without conversion

**4.3) Update `dependency_manifest.rs`**
- [x] `DependencyTable.schema` stays as `TableSchema` for JSON serialization (needed for legacy adapter in Phase 6)
- [x] Added `From<&SchemaRef> for ArrowSchema` to enable conversion from native to serializable format

**Acceptance criteria**: No `TableSchema`/`ArrowSchema` usage in authoring pipeline (only in adapter layer). **VERIFIED**

---

### Phase 5: Manifest Table Shape Changes

**Files**: `manifest.rs` (TableDef struct and builder)

**5.1) Update `TableDef` in `manifest.rs`**
- [x] Change `schema: FileRef` to `ipc: FileRef` (line 173)
- [x] Make `sql: FileRef` optional: `sql: Option<FileRef>` (line 171)
- [x] Add `#[serde(skip_serializing_if = "Option::is_none")]` to `sql` field
- [x] Update `ManifestError::SchemaFileRef` → `ManifestError::IpcFileRef` (lines 78-87)

**5.2) Update manifest builder**
- [x] Update `build_tables()` to create `TableDef` with optional `sql` (lines 401-438)
- [x] Handle derived tables: write both `sql: Some(...)` and `ipc: ...`
- [x] Prepare for raw tables: `sql: None`, only `ipc` field (future support)

**5.3) Update tests**
- [x] Update `table_def_serializes_correctly` test (lines 727-753)
- [x] Add test for optional SQL field serialization
- [x] Add test for raw table (no SQL) serialization

**Acceptance criteria**: Manifest JSON has `"ipc"` field instead of `"schema"`. SQL field can be optional.

---

### Phase 6: Adapter Layer - Legacy ↔ Package

**Files**: `bridge.rs` (extend), `cache.rs` (adapter calls), `resolver.rs` (use adapter)

**6.1) Admin API fetch adapter (legacy → package)**
- [x] Create adapter function to convert legacy manifest JSON to canonical package format
- [x] When fetching from admin API:
  - Parse legacy `manifest.json`
  - Extract inline SQL content → write to `tables/<table>.sql`
  - Convert inline schema JSON → write to `tables/<table>.ipc`
  - Copy function sources → `functions/`
  - Write canonical `manifest.json` with file refs
- [x] Store canonical package in cache directory (via `LegacyAdapter` writing to target dir)

**6.2) Register adapter (package → legacy)**
- [x] Create adapter function to convert package format to legacy manifest JSON
- [x] When registering via `--package`:
  - Read `tables/<table>.ipc` → convert to legacy schema JSON
  - Read `tables/<table>.sql` content
  - Read `functions/` sources
  - Build legacy manifest JSON with inline content
- [x] Upload legacy manifest to admin API (existing functionality via `LegacyBridge::to_json()`)

**6.3) Constrain legacy parsing**
- [x] Ensure all legacy JSON schema parsing is confined to adapter layer
- [x] Update resolver to use LegacyAdapter for derived datasets (kind="manifest")
- [x] Raw datasets (evm-rpc, firehose, etc.) parsed directly into DependencyManifest

**Acceptance criteria**:
- Fetching legacy manifests populates cache with canonical package format.
- Registering a package produces valid legacy manifest JSON for the API.

---

### Phase 7: Cache Updates

**Files**: `cache.rs`, `resolver.rs`, `dependency_manifest.rs`

**7.1) Update `cache.rs`**
- [x] Cache structure stores full package format (via LegacyAdapter):
  - `<hash>/manifest.json` (DependencyManifest)
  - `<hash>/tables/<table>.sql`
  - `<hash>/tables/<table>.ipc`
  - `<hash>/functions/<name>.js`
- [x] Add `CachedPackage` struct with methods:
  - `manifest()` - returns `&DependencyManifest`
  - `read_sql(table_name)` - reads SQL content
  - `read_schema(table_name)` - reads Arrow SchemaRef from IPC file
  - `read_function(filename)` - reads function source
  - `has_sql()`, `has_ipc_schema()` - existence checks
- [x] Add `Cache::get_package()` returning `Option<CachedPackage>`
- [x] Keep existing `Cache::get()` for backward compatibility (returns `DependencyManifest`)
- [x] Add error variants for IPC/SQL/function file reads

**7.2) Update `resolver.rs`** (No changes needed)
- [x] Resolver already works correctly:
  - Uses `LegacyAdapter` to write full package to cache directory
  - Uses `Cache::put()` to store `DependencyManifest`
  - Uses `Cache::get()` to retrieve `DependencyManifest` for resolution
  - Consumers can use `Cache::get_package()` when IPC access is needed

**7.3) Update `dependency_manifest.rs`** (Deferred)
- [x] `DependencyTable.schema` keeps `TableSchema` (JSON-serializable) for compatibility
- [x] Consumers needing Arrow SchemaRef use `CachedPackage::read_schema()`
- Note: Lazy loading from IPC considered but not needed - current approach works

**Acceptance criteria**: Cache stores and retrieves full canonical packages, not just manifest JSON. **VERIFIED**

---

### Phase 8: Documentation & Tests

**Files**: `docs/features/dataset-authoring.md`, `tests/src/tests/it_dataset_authoring.rs`, CLI help markdown files, `playground/`

**8.1) Update `docs/features/dataset-authoring.md`**
- [x] Replace all `models/` references with `tables/` (currently 11 occurrences)
- [x] Update build output structure section (currently shows `sql/` layout)
- [x] Update `amp.yaml` schema documentation (config field `models` → `tables`)
- [x] Document `.ipc` schema format (replace `.schema.json` references)
- [x] Update CLI examples

**8.2) Update tests**
- [x] Update all fixture paths from `sql/` to `tables/`
- [x] Update all `.schema.json` references to `.ipc`
- [x] Add IPC round-trip tests (already exist in arrow_ipc.rs)
- [x] Add adapter layer tests for legacy conversion (already exist in bridge.rs)
- [x] Ensure coverage of new table shape (optional sql) (already exist in manifest.rs)

**8.3) Update CLI help text**
- [x] Update `ampctl dataset` subcommand help for `tables/` directory (package.rs docstrings updated)

**8.4) Update playground sample**
- [x] Delete `playground/build/` directory (gitignored, not tracked)
- [x] Rename `playground/models/` to `playground/tables/`
- [x] Update `playground/amp.yaml` to use `tables:` field (relies on new default)
- [x] Regenerate `playground/build/` with new structure (gitignored)

**8.5) Update module docstrings**
- [x] Update `lib.rs` docstring (lines 1-29) mentioning `models/` and `sql/`
- [x] Update `manifest.rs` docstring (lines 1-41) with example JSON

**Acceptance criteria**: Docs, tests, and samples are consistent with new implementation. **VERIFIED**

---

## File Annotations

Quick reference for key files and line numbers (verified 2026-02-04):

| File | Key Lines | Purpose |
|------|-----------|---------|
| `config.rs` | 87-118, 156-158 | `AmpYaml` struct, `default_models_dir()` |
| `discovery.rs` | 97-162 | `discover_models()` function |
| `manifest.rs` | 168-176, 282-292 | `TableDef` struct, `ManifestBuilder` |
| `package.rs` | 164-187 | `from_directory()` reads `sql/` and `functions/` |
| `cache.rs` | 131-133, 145-163, 173-200 | `manifest_path()`, `get()`, `put()` |
| `arrow_json.rs` | 74-104 | `write_schema_file()`, `read_schema_file()` |
| `validation.rs` | 105, 429-470, 591 | `discovered_models` field and usage |
| `lib.rs` | 1-29 | Module docstring with workflow description |
| `files.rs` | docstrings, tests | Path examples use `sql/` throughout |
| `bridge.rs` | tests | Test fixtures use `sql/` paths extensively |
| `playground/amp.yaml` | 1-8 | Sample config using `models` default |

---

## Implementation Order

**Recommended sequence** (minimizes rework):

1. **Phase 1** - Arrow IPC module (no dependencies, foundational)
2. **Phase 5.1** - TableDef field rename (`schema` → `ipc`, optional `sql`)
3. **Phase 3** - Build output layout change (`sql/` → `tables/`)
4. **Phase 2** - Config rename (`models` → `tables`)
5. **Phase 4** - Schema type refactor (use Arrow-native types)
6. **Phase 6** - Adapter layer (legacy conversion)
7. **Phase 7** - Cache format update
8. **Phase 8** - Documentation and tests

Each phase should be completable in one commit/PR.

---

## Answered Questions

1. **Deprecation period for `models`?** - No backwards compatibility. No deprecation period. None of this code is released yet.
2. **Cache migration?** - No migration. Clear old cache entries. No backwards compatibility. None of this code is released yet.
3. **Raw table authoring?** - Raw datasets are currently defined by extractor code. Eventually this will change and raw datasets will also have their table schemas declared and discoverable in a registry. But this is out of scope for this plan.

---

## Blockers

None identified. All dependencies (Arrow, IPC support) are already available in the workspace.
- Arrow IPC verified in use at: `crates/services/server/src/flight.rs:595`, `crates/clients/flight/src/store/mod.rs:195`

---

## Next Steps

All phases complete. The refactor is done:
- Arrow IPC schema format in use
- `tables/` directory layout
- `tables:` config field (with `tables` default)
- Legacy adapter layer for API interop
- Full package caching

Ready for review and merge.

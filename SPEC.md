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
| 1 | Arrow IPC Module | Not started |
| 2 | Rename `models` → `tables` | Not started |
| 3 | Build Output Layout (`sql/` → `tables/`) | Not started |
| 4 | Schema Type Refactor | Not started |
| 5 | Manifest Table Shape Changes | Not started |
| 6 | Adapter Layer (Legacy ↔ Package) | Not started |
| 7 | Cache Updates | Not started |
| 8 | Documentation & Tests | Not started |

---

## Gap Analysis

Based on codebase exploration (2026-02-04):

### Currently Implemented
- `amp.yaml` parsing with `models` field (default `"models"`) in `config.rs:108-109`
- Model discovery from `models/**/*.sql` in `discovery.rs:97-162`
- Build output to `sql/<table>.sql` and `sql/<table>.schema.json` in `manifest.rs:405-415`
- Arrow JSON schema read/write in `arrow_json.rs`
- Package assembly including `sql/` directory in `package.rs:175-177`
- Cache storing `manifest.json` only in `cache.rs`
- Legacy bridge converting authoring manifest to runtime format in `bridge.rs`
- Jinja templating with `ref`, `source`, `var`, `env_var`, `this` helpers in `jinja.rs`
- SQL validation (SELECT-only, incremental constraints) in `query.rs`
- Lockfile (`amp.lock`) for reproducible builds in `lockfile.rs`
- CLI commands: `build`, `check`, `package`, `register` in `ampctl`

### Not Yet Implemented
- Arrow IPC file I/O (no `arrow_ipc.rs` module exists)
- `tables/` output directory (currently `sql/`)
- `.ipc` schema files (currently `.schema.json`)
- `tables` config field (currently `models`)
- Support for raw tables (tables without SQL) in authoring manifest
- Canonical package format in cache (currently legacy manifest JSON)
- Admin API fetch → canonical package conversion (adapter layer)
- IPC → legacy schema JSON conversion for register (adapter layer)

---

## Tasks

### Phase 1: Arrow IPC Module (Foundational)

**Files**: `arrow_ipc.rs` (new), `lib.rs` (1 line)

**1.1) Create `arrow_ipc.rs` module**
- [ ] Add new module `crates/core/dataset-authoring/src/arrow_ipc.rs`
- [ ] Implement `write_ipc_schema(schema: &Schema, path: &Path) -> Result<()>`
  - Use Arrow IPC FileWriter with schema-only (no record batches)
- [ ] Implement `read_ipc_schema(path: &Path) -> Result<Schema>`
  - Use Arrow IPC FileReader to read schema metadata
- [ ] Add comprehensive tests for round-trip serialization
- [ ] Export from `lib.rs`

**Acceptance criteria**: Can write Arrow `Schema` to `.ipc` file and read it back losslessly.

---

### Phase 2: Rename `models` → `tables` (Config Change)

**Files**: `config.rs`, `discovery.rs`, `validation.rs`, CLI commands, integration tests

**2.1) Update `config.rs`**
- [ ] Rename field `models: PathBuf` to `tables: PathBuf` (line 109)
- [ ] Update default function `default_models_dir()` → `default_tables_dir()` returning `"tables"`
- [ ] Update `AmpYamlV1` struct similarly (line 141)
- [ ] Update all validation messages referencing "models directory"
- [ ] Update all tests using `models` field

**2.2) Update `discovery.rs`**
- [ ] Rename function `discover_models()` → `discover_tables()`
- [ ] Update variable name `models` → `tables` in function body (line 108)
- [ ] Update documentation and error messages

**2.3) Update call sites**
- [ ] Update `validation.rs` field `discovered_models` → `discovered_tables` (line 105)
- [ ] Update all callers of discovery functions

**Acceptance criteria**: `amp.yaml` accepts `tables:` field (with `tables` as default). `models:` is no longer recognized.

---

### Phase 3: Build Output Layout (`sql/` → `tables/`)

**Files**: `manifest.rs`, `package.rs`, `bridge.rs`, `arrow_json.rs`, CLI help text

**3.1) Update `manifest.rs` output paths**
- [ ] Change SQL file path from `sql/<table>.sql` to `tables/<table>.sql` (line 406)
- [ ] Change schema file path from `sql/<table>.schema.json` to `tables/<table>.ipc` (line 415)
- [ ] Update `sql_dir` parameter naming throughout to `tables_dir`
- [ ] Update `ManifestBuilder` field and parameter names

**3.2) Update `package.rs`**
- [ ] Change directory inclusion from `sql/` to `tables/` (lines 174-177)
- [ ] Update all test fixtures and assertions

**3.3) Update `bridge.rs`**
- [ ] Update all path references from `sql/` to `tables/`
- [ ] Update test fixtures

**3.4) Update `arrow_json.rs` → deprecate or remove**
- [ ] After IPC is working, remove JSON schema write calls from build flow
- [ ] Keep `arrow_json.rs` only if needed for legacy adapter layer

**Acceptance criteria**: `ampctl dataset build` produces `tables/<table>.sql` + `tables/<table>.ipc`, no `sql/` directory.

---

### Phase 4: Schema Type Refactor (Arrow-native)

**Files**: `schema.rs`, `validation.rs`, `dependency_manifest.rs`, build commands

**4.1) Update schema inference in `schema.rs`**
- [ ] Change return type from `TableSchema` to Arrow `SchemaRef`
- [ ] Remove intermediate `TableSchema`/`ArrowSchema` conversions
- [ ] Update `SchemaContext::infer_schema()` to return `SchemaRef` directly

**4.2) Update build pipeline**
- [ ] Write inferred schemas using `arrow_ipc::write_ipc_schema()` instead of JSON
- [ ] Update validation output to use Arrow types
- [ ] Update dependency schema handling to read IPC schemas

**4.3) Update `dependency_manifest.rs`**
- [ ] Consider if `DependencyTable.schema` field needs to change (for cached dependencies)
- [ ] May need intermediate representation for cache format

**Acceptance criteria**: No `TableSchema`/`ArrowSchema` usage in authoring pipeline (only in adapter layer).

---

### Phase 5: Manifest Table Shape Changes

**Files**: `manifest.rs` (TableDef struct and builder)

**5.1) Update `TableDef` in `manifest.rs`**
- [ ] Change `schema: FileRef` to `ipc: FileRef` (path to `.ipc` file)
- [ ] Make `sql: FileRef` optional (`Option<FileRef>`) for raw table support
- [ ] Update serialization to omit `sql` field when `None`

**5.2) Update manifest builder**
- [ ] Handle derived tables: write both `sql` and `ipc`
- [ ] Prepare for raw tables: only `ipc` field (future support)

**Acceptance criteria**: Manifest JSON has `"ipc"` field instead of `"schema"`. SQL field can be optional.

---

### Phase 6: Adapter Layer - Legacy ↔ Package

**Files**: `bridge.rs` (extend), `cache.rs` (adapter calls), `resolver.rs` (use adapter)

**6.1) Admin API fetch adapter (legacy → package)**
- [ ] Create adapter function to convert legacy manifest JSON to canonical package format
- [ ] When fetching from admin API:
  - Parse legacy `manifest.json`
  - Extract inline SQL content → write to `tables/<table>.sql`
  - Convert inline schema JSON → write to `tables/<table>.ipc`
  - Copy function sources → `functions/`
  - Write canonical `manifest.json` with file refs
- [ ] Store canonical package in cache directory

**6.2) Register adapter (package → legacy)**
- [ ] Create adapter function to convert package format to legacy manifest JSON
- [ ] When registering via `--package`:
  - Read `tables/<table>.ipc` → convert to legacy schema JSON
  - Read `tables/<table>.sql` content
  - Read `functions/` sources
  - Build legacy manifest JSON with inline content
- [ ] Upload legacy manifest to admin API

**6.3) Constrain legacy parsing**
- [ ] Ensure all legacy JSON schema parsing is confined to adapter layer
- [ ] Remove direct `TableSchema` parsing from non-adapter code

**Acceptance criteria**:
- Fetching legacy manifests populates cache with canonical package format.
- Registering a package produces valid legacy manifest JSON for the API.

---

### Phase 7: Cache Updates

**Files**: `cache.rs`, `resolver.rs`

**7.1) Update `cache.rs`**
- [ ] Change cache structure from `manifest.json` only to full package format:
  - `<hash>/manifest.json` (canonical format)
  - `<hash>/tables/<table>.sql`
  - `<hash>/tables/<table>.ipc`
  - `<hash>/functions/`
- [ ] Update `Cache::get()` to read canonical package
- [ ] Update `Cache::put()` to write canonical package

**7.2) Update `resolver.rs`**
- [ ] Update dependency resolution to read IPC schemas from cache
- [ ] Update `DependencyManifest` parsing to handle new format

**Acceptance criteria**: Cache stores and retrieves full canonical packages, not just manifest JSON.

---

### Phase 8: Documentation & Tests

**Files**: `docs/features/dataset-authoring.md`, `tests/src/tests/it_dataset_authoring.rs`, CLI help markdown files, `playground/`

**8.1) Update `docs/features/dataset-authoring.md`**
- [ ] Replace all `models/` references with `tables/`
- [ ] Update build output structure section
- [ ] Update `amp.yaml` schema documentation
- [ ] Document `.ipc` schema format
- [ ] Update CLI examples

**8.2) Update tests**
- [ ] Update all fixture paths from `sql/` to `tables/`
- [ ] Update all `.schema.json` references to `.ipc`
- [ ] Add IPC round-trip tests
- [ ] Add adapter layer tests for legacy conversion
- [ ] Ensure coverage of new table shape (optional sql)

**8.3) Update CLI help text**
- [ ] Update `ampctl dataset` subcommand help for `tables/` directory

**8.4) Update playground sample**
- [ ] Rename `playground/models/` to `playground/tables/`
- [ ] Update `playground/amp.yaml` to use `tables:` field (or rely on new default)

**Acceptance criteria**: Docs, tests, and samples are consistent with new implementation.

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

## Open Questions

1. **Deprecation period for `models`?** - Current plan: no backwards compatibility. Confirm this is acceptable.
2. **Cache migration?** - Should we clear old cache entries or support both formats during transition?
3. **Raw table authoring?** - When will authoring support raw tables (no SQL)? This plan prepares the manifest shape but doesn't implement discovery.

---

## Blockers

None identified. All dependencies (Arrow, IPC support) are already available in the workspace.

---

## Next Steps

1. Begin with **Phase 1** (Arrow IPC module) - self-contained, no breaking changes
2. Implementation should follow the recommended sequence in "Implementation Order"
3. Each phase should be one atomic commit/PR

# SPEC.md - Amp: Static CSV Provider for External Tables

## Goal

Implement a new `amp-providers-static` crate (based on `amp-object-store`) that provides
external CSV-backed tables to the query engine catalog via the providers registry.
This provider must support schema inference with column name sanitization and optional
in-memory caching for small CSV files. The providers registry remains the single
entity that supplies catalogs to the query engine.

**MANDATORY**: CSV-only for the MVP. No schema overrides beyond per-column type overrides and column mapping. No file watching. No remote HTTP URLs.

## Examples

### Example 1: Minimal ERC-20 Metadata Augmentation (Defaults Only)

**CSV file** (`datasets/table_group_1/erc20-token-info.csv`):

```csv
contract_address,token_symbol,decimals
0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48,USDC,6
0xdac17f958d2ee523a2206206994597c13d831ec7,USDT,6
```

**Provider config file**: `provider_a.toml`

This provider TOML omits `has_header` (auto-detects header; see IC-4) and uses defaults for `in_memory_max_bytes` and `schema_inference_max_rows`:

```toml
kind = "static"
object_store_root = "s3://bucket/prefix"

[tables.table_group_1.table_name_1]
path = "datasets/table_group_1/erc20-token-info.csv"
```

### Example 2: Column Mapping (Provider TOML + Query)

**CSV file** (`datasets/table_group_1/erc20-token-info.csv`):

```csv
Contract Address,Token Symbol,Decimals
0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48,USDC,6
0xdac17f958d2ee523a2206206994597c13d831ec7,USDT,6
```

**Provider config file**: `provider_a.toml`

This provider TOML shows a partial `columns` mapping from SQL name to CSV name:

```toml
kind = "static"
object_store_root = "s3://bucket/prefix"

[tables.table_group_1.table_name_1]
path = "datasets/table_group_1/erc20-token-info.csv"
columns = {
  contract_address = "Contract Address",
  token_symbol = "Token Symbol",
  decimals = "Decimals",
}
```

**Query** (catalog = provider ID, schema = dataset grouping, table = mapping name):

```sql
SELECT contract_address, token_symbol, decimals
FROM provider_a.table_group_1.table_name_1;
```

### Example 3: No Headers (ERC-20 Metadata)

**CSV file** (`datasets/table_group_1/erc20-token-info.csv`) with no header row:

```csv
0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48,USDC,6
0xdac17f958d2ee523a2206206994597c13d831ec7,USDT,6
```

**Provider config file**: `provider_a.toml`

This provider TOML sets `has_header = false` and maps SQL names to placeholder CSV columns:

```toml
kind = "static"
object_store_root = "s3://bucket/prefix"

[tables.table_group_1.table_name_1]
path = "datasets/table_group_1/erc20-token-info.csv"
has_header = false # Optional: if omitted, header is auto-detected (see IC-4).
columns = {
  contract_address = "col0",
  token_symbol = "col1",
  decimals = "col2",
}
```

**Query**:

```sql
SELECT contract_address, token_symbol, decimals
FROM provider_a.table_group_1.table_name_1;
```

### Example 4: Column Type Override (ERC-20 Metadata)

Schema type inference is automatic, but a manual override is possible. In this example,
`contract_address` would be inferred as bytes, and we override it to string.

**CSV file** (`datasets/table_group_1/erc20-token-info.csv`):

```csv
contract_address,token_symbol,decimals
0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48,USDC,6
0xdac17f958d2ee523a2206206994597c13d831ec7,USDT,6
```

**Provider config file**: `provider_a.toml`

This provider TOML overrides the inferred type for `contract_address` (e.g., bytes -> string):

```toml
kind = "static"
object_store_root = "s3://bucket/prefix"

[tables.table_group_1.table_name_1]
path = "datasets/table_group_1/erc20-token-info.csv"

[[tables.table_group_1.table_name_1.columns]]
name = "contract_address"
header-name = "contract_address"
type = "string"
```

**Query**:

```sql
SELECT contract_address, token_symbol, decimals
FROM provider_a.table_group_1.table_name_1;
```

## Proposed Provider Configuration

**Example provider TOML showing all fields:**

```toml
# Provider type. CSV-only for the MVP.
kind = "static"

# Root location for table paths. All table paths are relative to this root.
# Any scheme supported by amp-object-store is allowed (HTTP(S) remains disallowed for MVP).
object_store_root = "<placeholder-object_store_root-value>"

# Maximum file size (bytes) to cache in memory. Defaults to 10 MB if omitted.
# Set to 0 to disable caching.
in_memory_max_bytes = "<placeholder-in_memory_max_bytes-value>"

# Max rows to scan for schema inference when file size exceeds in_memory_max_bytes.
# Must be non-zero. Defaults to 256 if omitted.
schema_inference_max_rows = "<placeholder-schema_inference_max_rows-value>"

# Tables are nested by group and table name: tables.<group-name>.<table-name>
# The group name maps to the schema in SQL, and the table name is the SQL table.
# Two tables under the same group, each using a different columns flavor.

## Flavor A (columns map)
[tables.<group-name>.table-name-1]
# Path to the CSV file, relative to object_store_root.
path = "<placeholder-path-1-value>"

# Optional per-table header flag. If omitted, header is auto-detected (see IC-4).
has_header = "<placeholder-has_header-1-value>"

# Optional file type override (CSV-only, case-insensitive). If present, skips extension inference.
# Allowed values: "csv" or "text/csv" (case-insensitive); "text/csv" preferred.
file_type = "<placeholder-file_type-1-value>"

# Mapped SQL names are validated (not sanitized).
columns = {
  contract_address = "<placeholder-contract_address-csv-name-value>",
  token_symbol = "<placeholder-token_symbol-csv-name-value>",
  decimals = "<placeholder-decimals-csv-name-value>",
}

## Flavor B (columns entries)
[tables.<group-name>.table-name-2]
# Path to the CSV file, relative to object_store_root.
path = "<placeholder-path-2-value>"

# Optional per-table header flag. If omitted, header is auto-detected (see IC-4).
has_header = "<placeholder-has_header-2-value>"

[[tables.<group-name>.table-name-2.columns]]
# SQL column name exposed to queries.
name = "<placeholder-sql-column-name-1-value>"
# CSV header name (or col#) mapped to this SQL column.
header-name = "<placeholder-csv-column-name-or-col-1-value>"
# Optional DataFusion type override for this SQL column.
type = "<placeholder-column-type-1-value>" # Optional

[[tables.<group-name>.table-name-2.columns]]
# SQL column name exposed to queries.
name = "<placeholder-sql-column-name-2-value>"
# CSV header name (or col#) mapped to this SQL column.
header-name = "<placeholder-csv-column-name-or-col-2-value>"
# type = "<placeholder-column-type-2-value>" # Optional
```

## Design: Lifecycle Flows

### Provider Registration Time (What Happens)

- Provider config is submitted for registration.
- Provider config file is registered in the system's providers object store.
- The registry performs static analysis of the config only.
- The config is validated for structure and contradictions (for example, a table cannot specify both the `columns` map and `[[tables.<group>.<table>.columns]]` entries).
- Registration does not touch the data object store.
- No file existence or content validation occurs at registration time.

### Query Run Time (What Happens)

- A query references a provider-backed table.
- The registry resolves the table and begins data access.
- The CSV file is loaded, parsed, validated, and served.
- If the file size is at or below `in_memory_max_bytes`, the file is cached in memory for fast access.
- Schema inference, column mapping, sanitization, and type overrides are applied.
- Derived metadata (schema, mappings, and other table info) is cached in memory within the providers registry for fast access on subsequent queries.

## Current State Analysis (Verified 2026-02-04 00:00 UTC)

### Phase 0 Status (Documentation and Config Baseline)

| Task                                   | PR | Status |
|----------------------------------------|----|--------|
| Provider config documented as TOML     | -- | Exists |
| Providers directory configured in TOML | -- | Exists |

**Phase 0 is COMPLETE** - Provider configuration format and location are documented.

### What Currently Exists (Verified 2026-02-04)

1. **Config sample** (`docs/config.sample.toml`)
    - Defines `providers_dir` as a directory containing one TOML file per provider
    - Indicates relative paths are resolved from the config file directory
    - Note: this refers to provider config file lookup; static provider table `path` values remain relative to `object_store_root`
    - No external-table provider configuration is documented
    - **IC violations**: None identified

2. **Provider samples** (`docs/providers/evm-rpc.sample.toml`, `docs/providers/firehose.sample.toml`)
    - Provider configs are TOML files
    - File name represents the provider identity
    - No static CSV provider exists
    - **IC violations**: None identified

### Remaining Gaps (Phase 1 -- Core Implementation)

| Gap                                                  | IC   | Impact                                   | Task |
|------------------------------------------------------|------|------------------------------------------|------|
| Provider config definitions are tied to registry crate | IC-1 | Cannot reuse config parsing across crates | 1.1  |
| No `amp-providers-static` crate exists               | IC-2 | No external tables via CSV               | 2.1  |
| Providers registry does not expose lazy catalog      | IC-3 | Tables must be pre-registered at startup | 3.1  |
| No schema inference or name sanitization for CSV     | IC-4 | Schema cannot be resolved safely         | 2.2  |
| No small-file in-memory cache path                   | IC-5 | Small CSVs pay object-store latency      | 2.3  |

---

## Implementation Constraints

### IC-1: Provider Config in `amp-providers-common`

Provider configuration parsing and shared types MUST live in a new `amp-providers-common`
crate. Existing provider config parsing is moved from `amp-providers-registry` to this
crate, and other crates depend on `amp-providers-common` for config IO.

**Applies to**: Task 1.1, 1.2

### IC-2: CSV-Only Static Provider (MVP)

`amp-providers-static` MUST support CSV files only for this MVP. Multi-format support
is out of scope. Schema overrides are limited to per-column type overrides.

**Applies to**: Task 2.1, 2.2

### IC-3: Lazy Catalog Resolution

The providers registry MUST supply catalogs lazily to the query engine. Tables are
resolved on demand via `CatalogProvider` and `SchemaProvider` interfaces rather than
pre-registering all tables at startup.

**Applies to**: Task 3.1

### IC-4: Schema Inference and Column Name Sanitization

CSV schemas MUST be inferred from the file content. Column names MUST be sanitized by:

1. Lowercasing
2. Replacing non-alphanumeric characters with `_`
3. Collapsing repeated `_`
4. Trimming leading and trailing `_`
5. Prefixing `_` if the name starts with a digit

Optional column mapping is allowed per table in one of two mutually exclusive forms.

Form A (map): `columns` is a partial mapping of `sql_column_name -> csv_column_name`. Mapping values match raw CSV header values (or placeholder names like `col0`) before sanitization.

Form B (columns entries): use `[[tables.<group>.<table>.columns]]` entries with `name` (SQL column name), `header-name` (CSV header name or `col#`), and optional `type` (DataFusion type override).

Rules:
- If `has_header` is omitted, auto-detect by reading the first two rows. If the first row is all strings
  and the second row contains heterogeneous inferred types, treat the first row as a header. Otherwise
  treat the file as having no header. If the file has only one row, treat it as no header. Use the same
  type inference rules as schema inference. **Note**: all-string datasets with headers must set
  `has_header = true`.
- If `has_header = false` (or no header is detected), generate zero-based placeholder source names:
  `col0`, `col1`, `col2`, ...
- If both `columns` map and columns entries are provided, return an error
- SQL column names MUST already satisfy the sanitization rules above (validate, do not sanitize)
- Unmapped columns use sanitized versions of their source names
- SQL column names after mapping + sanitization MUST be unique (case-sensitive). Enforce at
  registration time when determinable from config alone, and again at load time after inference and
  sanitization, just before processing the query.
- If a mapped CSV column name is not found, skip that mapping, omit it from the resolved schema, and
  emit a warning log. Queries that reference a missing column MUST error.
- If a `type` override refers to a missing SQL column, skip and emit a warning log
- Any collision after applying mappings, validating mapped names, and sanitizing unmapped names MUST return an error
- If no mapping exists and a collision occurs, return an error

Full schema overrides beyond per-column `type` are not allowed in this MVP.

**Applies to**: Task 2.2

### IC-5: Small-File In-Memory Cache

If a CSV file is at or below a configurable byte threshold, it MUST be loaded into
memory to avoid object-store latency. The default threshold is 10 MB and must be
configurable in the provider config. Setting `in_memory_max_bytes = 0` disables caching.
Cache bounds and eviction policy are TBD (intended to use foyer; specific algorithm to be decided).
Until an eviction policy is selected, the cache is unbounded aside from the per-file threshold.
The cache MUST be centralized in `amp-providers-registry` and shared across file providers.
When a provider is removed, its cached data MUST be cleared.

**Applies to**: Task 2.3

### IC-5b: Schema Inference Sampling

If a CSV file is at or below `in_memory_max_bytes`, schema inference MUST scan the full
file. If the file size exceeds `in_memory_max_bytes`, schema inference MUST cap scanning
to a configurable max row limit (default: 256). `schema_inference_max_rows` must be non-zero.

**Applies to**: Task 2.2

### IC-6: No File Watching

Provider updates occur only when provider configs are updated through the Admin API.
No file watchers or background polling are allowed in this MVP.

**Applies to**: Task 3.1

### IC-7: Object Store Root and Relative Paths

Static CSV table paths MUST be relative to the configured `object_store_root` in the
provider config. Any scheme supported by `amp-object-store` is allowed (HTTP(S) remains
disallowed for the MVP). Path resolution MUST follow `amp-object-store` / DataFusion
object store join semantics. Remote HTTP URLs are not allowed. Absolute paths and any
path containing `..` segments MUST be rejected.

**Applies to**: Task 2.1

---

## Tasks

### Phase 1: Provider Config Refactor

**Goal**: Centralize provider config parsing and types in `amp-providers-common`.

#### 1.1 Create `amp-providers-common` crate

**Current state**: Provider config parsing is tied to `amp-providers-registry`.

**Required change**: Introduce `amp-providers-common` and move config model and parsing
there. Consumers depend on `amp-providers-common` instead of `amp-providers-registry`.

**Implementation**:

- Create new crate `crates/core/providers-common` with public config types
- Move provider config parsing and validation from `amp-providers-registry`
- Keep TOML format and per-file provider identity

**Acceptance**:

- Existing provider configs still parse from TOML
- Crates that need provider configs depend on `amp-providers-common`
- `amp-providers-registry` no longer owns config parsing

**Depends on**: None

#### 1.2 Update consumers to use `amp-providers-common`

**Current state**: `amp-providers-registry` provides config parsing to consumers.

**Required change**: Update consumers to import config types from `amp-providers-common`.

**Implementation**:

- Update crate dependencies to use `amp-providers-common`
- Ensure Admin API and dataset store use the new config types

**Acceptance**:

- Provider configs load successfully through the new common crate
- No compile errors related to moved config types

**Depends on**: 1.1

---

### Phase 2: Static CSV Provider

**Goal**: Provide CSV-backed external tables through a new provider crate.

#### 2.1 Define static provider config schema

**Current state**: No static provider configuration exists.

**Required change**: Add a new TOML schema for `kind = "static"` providers. The file
name is the provider ID. Paths are relative to `object_store_root`.
Tables are configured as nested tables: `tables.<group>.<table>`.

**Implementation**:

- Add config structs in `amp-providers-static`
- Support grouped tables under `tables.<group>.<table>` with optional per-table settings
- Table names are keys (TableName type), not fields inside the table config
- Optional `has_header` per table; if omitted, auto-detect header per IC-4
- Allow `file_type` override when extension is ambiguous (CSV-only, case-insensitive). If present, skip file type inference from the file extension.
  Allowed values: `csv` or `text/csv` (case-insensitive); `text/csv` preferred.
- Allow optional `columns` per table in one of two forms (mutually exclusive):
- `columns = { <sql> = <csv> }` map
- `[[tables.<group>.<table>.columns]]` entries with `name`, `header-name`, and optional `type`
- Add `in_memory_max_bytes` with default 10 MB; `0` disables caching
- Add `schema_inference_max_rows` with default 256 (used only when file size exceeds `in_memory_max_bytes`); must be non-zero

**Target config shape (TOML)**:

```toml
kind = "static"
object_store_root = "s3://bucket/prefix"
in_memory_max_bytes = 10485760
schema_inference_max_rows = 256

# tables.<group>.<table> (group maps to schema; table maps to SQL table name)
[tables.table_group_1.table_name_1]
path = "datasets/table_group_1/erc20-token-info.csv"
# has_header = true
# file_type = "text/csv" # Allowed: "csv" or "text/csv" (case-insensitive); "text/csv" preferred
# Columns map SQL column name -> CSV column name (partial mapping)
# columns = {
#   contract_address = "Contract Address",
#   token_symbol = "Token Symbol",
#   decimals = "Decimals",
# }
# Alternative column entries (use instead of `columns` map)
# [[tables.table_group_1.table_name_1.columns]]
# name = "contract_address"
# header-name = "Contract Address"
# type = "string"
```

**Acceptance**:

- Static provider config parses from TOML
- Tables map to CSV paths under `tables.<group>.<table>`, relative to `object_store_root`
- Default values applied when optional fields are absent

**Depends on**: 1.1

#### 2.2 Implement CSV schema inference with sanitization

**Current state**: No static provider exists.

**Required change**: Infer CSV schema when resolving a table and apply sanitization.

**Implementation**:

- Use `amp-object-store` to read CSV content
- Determine source column names from the CSV header if `has_header = true`.
- If `has_header = false`, generate `col0`, `col1`, `col2`, ... as source names.
- If `has_header` is omitted, read the first two rows and apply the IC-4 detection rule. If no header
  is detected (or the file has only one row), generate `col0`, `col1`, `col2`, ...
- Apply `columns` using either the map or columns entries form (not both)
- For map form, match mapping values against source names; if a mapped CSV column name is missing, skip,
  omit from the resolved schema, and emit a warning log
- For columns entries form, match `header-name` against source names; if missing, skip, omit from the
  resolved schema, and emit a warning log
- Sanitize unmapped source names per IC-4 and validate mapped SQL names (do not sanitize mapped names)
- Enforce case-sensitive SQL name uniqueness after mapping + sanitization (IC-4), at registration when
  determinable and again at load time after inference
- If any collision remains after remapping/validation/sanitization, return an error
- If file size <= `in_memory_max_bytes`, infer schema using the full file
- If file size > `in_memory_max_bytes`, infer schema using at most `schema_inference_max_rows`
- Apply `type` overrides from columns entries to inferred columns; if a referenced SQL column is missing, skip and emit a warning log
- Return DataFusion `Schema` for the table

**Acceptance**:

- Table schema resolves without explicit schema in config
- Unmapped column names are sanitized consistently; mapped column names are validated as-is
- Invalid or empty headers are handled by sanitization rules or placeholder names
- Missing mapped CSV columns are omitted from the resolved schema and emit a warning; queries that reference them error
- Collisions after sanitization or remapping return an error
- Schema inference uses full scan for small files and row-limited scan for large files
- Column type overrides (from columns entries) are applied to matching SQL columns

**Depends on**: 2.1

#### 2.3 Implement small-file in-memory cache

**Current state**: No static provider exists.

**Required change**: Load small CSVs into memory and reuse cached data.

**Implementation**:

- Determine file size via object store metadata
- If `in_memory_max_bytes = 0`, disable caching and always stream from the object store
- If size <= `in_memory_max_bytes`, load data into memory
- Use in-memory representation for table scans
- If size > threshold, stream from object store
- Store cached data in a centralized registry cache shared across file providers
- Clear cached data when a provider is removed
- Apply foyer eviction policy once selected (TBD)

**Acceptance**:

- Small CSV files avoid object-store latency on repeated access
- Threshold is configurable and defaults to 10 MB
- `in_memory_max_bytes = 0` disables caching
- Eviction policy is documented once selected (foyer algorithm TBD)
- Cache is centralized in `amp-providers-registry` and is cleared when providers are removed

**Depends on**: 2.2

---

### Phase 3: Registry and Query Engine Integration

**Goal**: Expose static providers as lazy catalogs in the query server path.

#### 3.1 Providers registry supplies lazy catalogs

**Current state**: Providers registry does not supply external-table catalogs.

**Required change**: Implement a registry-backed `CatalogProvider` and `SchemaProvider`
that resolve tables on demand. The query server registers the registry catalog during
startup. Provider updates occur via Admin API only.

**Implementation**:

- Add a registry-backed catalog that delegates to provider implementations
- Map provider ID to a DataFusion catalog (provider is the external catalog)
- Use schema as a dataset grouping namespace; table name is the configured mapping
- Resolve `TableProvider` lazily using `amp-providers-static` for `kind = "static"`
- Update query server startup to register the catalog
- Ensure Admin API updates refresh registry state without file watchers
- Ensure registry-level cache is cleared when providers are removed

**Acceptance**:

- Query server can query static CSV tables without pre-registration
- Catalogs are provider IDs; schemas represent dataset groupings; tables are mapping names
- Default provider catalogs (`amp`, and DataFusion's `datafusion`) represent the current engine
- Provider updates via Admin API are visible on subsequent catalog lookups
- No file watching or background polling exists

**Depends on**: 2.1

---

## Task Priority Order

**Phase 1: Provider Config Refactor**

1. 1.1 Create `amp-providers-common` crate
2. 1.2 Update consumers to use `amp-providers-common`

**Phase 2: Static CSV Provider**

1. 2.1 Define static provider config schema
2. 2.2 Implement CSV schema inference with sanitization
3. 2.3 Implement small-file in-memory cache

**Phase 3: Registry and Query Engine Integration**

1. 3.1 Providers registry supplies lazy catalogs

**Dependency graph**:

```
Independent (no deps): 1.1
1.1 -> 1.2 -> 2.1 -> 2.2 -> 2.3 -> 3.1
```

**Recommended execution order**:

| Order | Task | IC   | Summary                               | Can Parallelize With |
|-------|------|------|---------------------------------------|----------------------|
| 1     | 1.1  | IC-1 | Create common provider config crate   | --                   |
| 2     | 1.2  | IC-1 | Update consumers to use common crate  | --                   |
| 3     | 2.1  | IC-2 | Define static provider config schema  | --                   |
| 4     | 2.2  | IC-4 | CSV schema inference + sanitization   | --                   |
| 5     | 2.3  | IC-5 | Small-file in-memory cache            | --                   |
| 6     | 3.1  | IC-3 | Lazy catalog integration              | --                   |

---

## Dependencies

**External Dependencies**:

- None

**Crate Dependency Changes**:

| Change | Crate                  | Reason                                         | Status   |
|--------|------------------------|------------------------------------------------|----------|
| ADD    | `amp-providers-common` | Shared provider config parsing and types       | Not done |
| ADD    | `amp-providers-static` | Static CSV provider implementation             | Not done |
| USE    | `amp-object-store`     | Object store access for CSV files              | Not done |
| MODIFY | `amp-providers-registry` | Use `amp-providers-common` and lazy catalogs | Not done |

**Phase Dependencies**:

- **Phase 2**: Phase 1 complete
- **Phase 3**: Phase 2 complete

---

## Blockers / Open Questions

### Active Blockers

*None*

### Decisions Needed

*None*

### Resolved Questions

- **CSV-only MVP scope** (Resolved 2026-02-04):
  - **Decision**: CSV only, no schema overrides beyond per-column type overrides and column mapping, no file watching, no remote HTTP URLs.
  - **Rationale**: Keep the POC focused and deterministic while proving the integration path.

- **Small-file in-memory cache** (Resolved 2026-02-04):
  - **Decision**: Cache CSV files <= 10 MB in memory with configurable threshold. `in_memory_max_bytes = 0`
    disables caching. Until an eviction policy is selected, the cache is unbounded aside from the per-file
    threshold.
  - **Rationale**: Avoid object-store latency for small files while keeping configuration flexible.

- **Schema inference** (Resolved 2026-02-04):
  - **Decision**: Infer schema from CSV and sanitize column names using the specified rule set.
  - **Rationale**: Reduce config burden while allowing targeted type overrides where needed.

- **Header auto-detection** (Resolved 2026-02-05):
  - **Decision**: If `has_header` is omitted, read the first two rows. If the first row is all strings and the
    second row contains heterogeneous inferred types, treat the first row as a header; otherwise treat as
    no header. If the file has only one row, treat as no header. Use the same inference rules as schema
    inference. All-string datasets with headers must set `has_header = true`.
  - **Rationale**: Provides deterministic behavior without forcing explicit configuration.

- **Column mapping and collision handling** (Resolved 2026-02-05):
  - **Decision**: Support optional per-table column mapping in one of two mutually exclusive forms:
    a `columns` map (SQL column name -> CSV column name) or `[[tables.<group>.<table>.columns]]` entries.
    The mapping can be partial. If no CSV header is present, generate placeholder names `col0`, `col1`, `col2`, ...
    Missing mapped CSV columns emit a warning, are omitted from the resolved schema, and queries that reference them error.
    SQL names after mapping + sanitization must be unique (case-sensitive). Any collision after remapping/validation/
    sanitization results in an error. Mapped names are validated (not sanitized).
    Configuration must be validated at provider registration time; if both mapping forms appear on the same table, return an error.
  - **Rationale**: Allows deterministic SQL naming while preventing ambiguous or lossy collisions.

- **Column type overrides** (Resolved 2026-02-05):
  - **Decision**: Support optional per-column `type` in `[[tables.<group>.<table>.columns]]` entries.
  - **Rationale**: Allows targeted type corrections (e.g., bytes to string) without full schema overrides.

- **Path traversal protection** (Resolved 2026-02-05):
  - **Decision**: Table paths must be relative to `object_store_root` and must not be absolute or contain `..` segments.
  - **Rationale**: Prevents path traversal and keeps resolution deterministic across object stores.

- **Schema inference sampling** (Resolved 2026-02-05):
  - **Decision**: If file size <= `in_memory_max_bytes`, infer schema using a full scan. Otherwise
    cap inference to `schema_inference_max_rows` (default 256).
  - **Rationale**: Keeps inference accurate for small files and bounded for large files.

- **`file_type` override scope** (Resolved 2026-02-05):
  - **Decision**: Accept `file_type` but restrict it to `csv` or `text/csv` (case-insensitive), prefer `text/csv`.
    Model as an enum.
  - **Rationale**: Keeps the MVP CSV-only while allowing explicit overrides for ambiguous extensions.

- **Object store root semantics** (Resolved 2026-02-05):
  - **Decision**: Allow any scheme supported by `amp-object-store` (HTTP(S) remains disallowed for the MVP).
    Path resolution uses `amp-object-store` / DataFusion object store join semantics.
  - **Rationale**: Keeps configuration flexible while relying on the object store's canonical path handling.

- **Crate path vs name** (Resolved 2026-02-05):
  - **Decision**: Keep crate name `amp-providers-common` while locating it at `crates/core/providers-common`.
  - **Rationale**: Preserve current workspace layout while documenting the naming mismatch explicitly.

- **Dependency table accuracy** (Resolved 2026-02-05):
  - **Decision**: `amp-object-store` already exists in `crates/core/object-store`, so mark as USE.
  - **Rationale**: Avoids implying new work when reuse is intended.

- **Catalog/schema mapping** (Resolved 2026-02-05):
  - **Decision**: Catalogs always correspond to provider IDs. Providers are external catalogs.
    The default provider catalogs (`amp`, and DataFusion's `datafusion`) represent the current
    engine. Use the schema namespace as a logical dataset grouping for static files, and the
    table name is the mapping name.
  - **Rationale**: Keeps providers aligned with external catalog semantics while reserving
    schema for dataset grouping in the MVP and preserving a clear default engine catalog.

---

## Notes

- This MVP targets the query server path only.
- Provider config files remain TOML with one provider per file.
- Provider updates are surfaced through Admin API updates to the registry.

---

## Verification Log

### 2026-02-04 (rev 1) - Documentation Baseline

**Re-verified**: Provider config format and location are documented as TOML files under
`providers_dir`, with one provider per file.

**Verification method**: Read sample config and provider documentation files.

**Findings**:

1. `docs/config.sample.toml` defines `providers_dir` and describes per-file provider configs.
2. `docs/providers/evm-rpc.sample.toml` and `docs/providers/firehose.sample.toml` show TOML provider files.

**Files verified**:

| File                                  | Status | Notes                                  |
|---------------------------------------|--------|----------------------------------------|
| `docs/config.sample.toml`             | OK     | `providers_dir` documented             |
| `docs/providers/evm-rpc.sample.toml`  | OK     | TOML provider format example           |
| `docs/providers/firehose.sample.toml` | OK     | TOML provider format example           |

**Status**: Ready for Phase 1 implementation.

---
name: "dataset-authoring"
description: "Build and package derived datasets from amp.yaml/amp.yml configuration with SQL/Jinja templating, dependency resolution, and schema inference. Load when working with dataset build, package, or authoring workflow"
type: "feature"
status: "unstable"
components: "crate:dataset-authoring,app:ampctl"
---

# Dataset Authoring

## Summary

Dataset Authoring provides a filesystem-first workflow for defining derived datasets using YAML configuration and SQL files. The authoring pipeline parses `amp.yaml` or `amp.yml` (including the required `amp` contract version), discovers table SQL files from the `tables/` directory (default when omitted), renders Jinja templates, validates SELECT-only statements, infers Arrow schemas via DataFusion, and produces deterministic packages for deployment. The workflow integrates with the existing registry and admin API for dependency resolution and manifest registration.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Usage](#usage)
3. [Configuration](#configuration)
4. [Limitations](#limitations)
5. [References](#references)

## Key Concepts

- **amp.yaml / amp.yml**: The YAML configuration file that defines a dataset's metadata, dependencies, tables, functions, and variables. It is the entrypoint for the authoring workflow and must include an `amp` contract version.

- **Tables**: SQL files in the `tables/` directory (default), each containing a single SELECT query. The table name is derived from the filename (e.g., `tables/transfers.sql` creates the `transfers` table).

- **Jinja templating**: SQL files support Jinja templates rendered at build time. Available helpers include `ref()`, `source()`, `var()`, `env_var()`, and `this`. No database access is permitted during rendering.

- **Lockfile (amp.lock)**: Records the full transitive dependency graph with manifest hashes. Ensures reproducible builds by pinning exact versions.

- **Package (dataset.tgz)**: A deterministic archive containing the canonical manifest, rendered SQL files, inferred schemas, and function sources. Used for distribution and deployment.

- **Legacy bridge**: Converts the authoring manifest format to the runtime manifest format expected by existing Amp servers, enabling incremental adoption.

## Usage

### Package Layout

A dataset project has this structure (use `amp.yaml` or `amp.yml`, not both):

```
my_dataset/
  amp.yaml              # Required: dataset configuration (or amp.yml)
  tables/
    transfers.sql       # One SQL file per table
    balances.sql
  functions/            # Optional: JavaScript UDFs
    my_func.js
  amp.lock              # Generated: dependency lockfile
```

### Build a Dataset

Build a dataset from the current directory:

```bash
ampctl dataset build --output build/
```

Build from a specific directory with custom output:

```bash
ampctl dataset build --dir my_dataset/ --output dist/
```

Override Jinja variables at build time:

```bash
ampctl dataset build --output build/ --var network=mainnet --var chain_id=1
```

Build with locked dependencies (fail if `amp.lock` is missing or stale):

```bash
ampctl dataset build --output build/ --locked
```

Build with offline cache only (requires amp.lock when version refs are present):

```bash
ampctl dataset build --output build/ --offline
```

Build with frozen dependencies (equivalent to `--locked` + `--offline`):

```bash
ampctl dataset build --output build/ --frozen
```

### Validate a Dataset

Validate authoring inputs without writing build artifacts:

```bash
ampctl dataset check
```

Validate a specific directory:

```bash
ampctl dataset check --dir my_dataset/
```

Validate with offline cache only:

```bash
ampctl dataset check --offline
```

Emit structured JSON output:

```bash
ampctl dataset check --json
```

### Package a Dataset

Create a distributable archive from build output:

```bash
ampctl dataset package
```

Specify custom input directory and output filename:

```bash
ampctl dataset package --dir dist/ --output my_dataset.tgz
```

### Register a Dataset

Register using the legacy bridge (from package directory):

```bash
ampctl dataset register my_namespace/my_dataset --package build
```

Register with a version tag:

```bash
ampctl dataset register my_namespace/my_dataset --package build --tag 1.0.0
```

### Full Workflow Example

```bash
# Build the dataset
ampctl dataset build --dir my_dataset/ --output build/

# Package for distribution
ampctl dataset package --dir build --output my_dataset.tgz

# Register with the admin API
ampctl dataset register my_namespace/my_dataset --package build --tag 1.0.0
```

### Writing SQL Files

Each SQL file must contain exactly one SELECT statement:

```sql
-- tables/transfers.sql
SELECT
    block_num,
    tx_hash,
    arrow_cast(log_index, 'Int32') AS log_index,
    "from",
    "to",
    amount
FROM {{ ref('eth', 'logs') }}
WHERE topic0 = '0xddf252ad...'
```

### Table Validation Constraints

Authoring validation matches legacy `manifest.json` rules. For a table to be valid:

- The SQL file must contain exactly one SELECT statement (no DDL/DML).
- Table references must be dependency-qualified `alias.table` and exist in the dependency manifest. Unqualified (`table`) or catalog-qualified (`catalog.schema.table`) references are rejected.
- Function references must be either built-in or qualified: `alias.func` for dependency UDFs or `self.func` for local UDFs. `eth_call` is only available for `evm-rpc` dependencies.
- Incremental constraints apply: no aggregates, `DISTINCT`, window functions, `ORDER BY`, `LIMIT/OFFSET`, outer/anti joins, or recursive queries. Inner/semi joins and `UNION ALL` are allowed (plain `UNION` implies `DISTINCT`).
- Aliases cannot start with `_` (for example, `AS _tmp`).

### Jinja Helpers

Available helpers in SQL templates:

| Helper | Description |
|--------|-------------|
| `ref(alias, table)` | Reference a dependency table, returns `<alias>.<table>` |
| `source(alias, table)` | Alias for `ref()` (dbt compatibility) |
| `var(name, default=None)` | Get build-time variable (CLI > config > default) |
| `env_var(name, default=None)` | Get environment variable (env > default) |
| `this` | Current table name being processed |

Variable precedence:
- `var()`: CLI `--var` > config `vars` > function default (error if missing without default)
- `env_var()`: environment > function default (error if missing without default)

CLI `--var` keys must be declared in config `vars`; unknown keys fail build/check.

## Configuration

### amp.yaml / amp.yml Schema

`amp` is the authoring contract version (semver) and is separate from the dataset `version`.

| Field | Required | Description |
|-------|----------|-------------|
| `amp` | Yes | Authoring contract version (semver string, e.g., `1.0.0`) |
| `namespace` | Yes | Dataset namespace (lowercase, alphanumeric, underscores) |
| `name` | Yes | Dataset name (lowercase, alphanumeric, underscores) |
| `version` | Yes | Semantic version (e.g., `1.0.0`) |
| `dependencies` | No | Map of alias to `namespace/name@version` or `@hash` |
| `tables` | No | Path to directory containing table SQL files (defaults to `tables`) |
| `functions` | No | Map of function name to function definition |
| `vars` | No | Map of variable name to default value |

### Example amp.yaml

```yaml
amp: 1.0.0
namespace: my_namespace
name: my_dataset
version: 1.0.0
dependencies:
  eth: my_namespace/eth_mainnet@1.0.0
  other: other_ns/other@sha256:b94d27b9...
 # tables: tables  # optional; defaults to "tables"
functions:
  addSuffix:
    input_types: [Utf8]
    output_type: Utf8
    source: functions/add_suffix.js
vars:
  network: mainnet
```

### Function Definition

| Field | Required | Description |
|-------|----------|-------------|
| `input_types` | Yes | Array of Arrow type names (e.g., `[Int64, Utf8]`) |
| `output_type` | Yes | Arrow type name for return value |
| `source` | Yes | Path to JavaScript source file |

### Build Output Structure

The build command produces:

```
<output>/
  manifest.json           # Canonical manifest with file references
  tables/
    <table>.sql           # Rendered SQL (post-Jinja)
    <table>.ipc           # Inferred Arrow schema (IPC file format)
  functions/
    <func>.js             # Copied function sources
```

### CLI Options

All dataset authoring commands honor the global `--json` flag for structured output.

#### `ampctl dataset build`

| Flag | Default | Description |
|------|---------|-------------|
| `--dir`, `-d` | `.` | Directory containing amp.yaml or amp.yml |
| `--output`, `-o` | (required) | Output directory for build artifacts |
| `--var`, `-V` | - | Variable override (NAME=VALUE), repeatable |
| `--locked` | false | Fail if amp.lock missing or stale |
| `--offline` | false | Disable network fetches; cache-only resolution |
| `--frozen` | false | Equivalent to `--locked` + `--offline` |
| `--admin-url` | `http://localhost:1610` | Admin API URL for dependency resolution |
| `--json` | false | Emit structured JSON output |

Notes:
- `--offline` requires `amp.lock` when version refs are present; cache misses are errors.

#### `ampctl dataset check`

| Flag | Default | Description |
|------|---------|-------------|
| `--dir`, `-d` | `.` | Directory containing amp.yaml or amp.yml |
| `--var`, `-V` | - | Variable override (NAME=VALUE), repeatable |
| `--locked` | false | Fail if amp.lock missing or stale |
| `--offline` | false | Disable network fetches; cache-only resolution |
| `--frozen` | false | Equivalent to `--locked` + `--offline` |
| `--admin-url` | `http://localhost:1610` | Admin API URL for dependency resolution |
| `--json` | false | Emit structured JSON output |

#### `ampctl dataset package`

| Flag | Default | Description |
|------|---------|-------------|
| `--dir`, `-d` | CWD (if `manifest.json` exists) | Directory containing build artifacts (required if no manifest in CWD) |
| `--output`, `-o` | `dataset.tgz` | Output archive path |
| `--json` | false | Emit structured JSON output |

#### `ampctl dataset register`

| Argument | Required | Description |
|----------|----------|-------------|
| `FQN` | Yes | Fully qualified name (`namespace/name`) |
| `FILE` | No | Path or URL to manifest file |
| `--package`, `-p` | No | Directory with built package (converts via bridge) |
| `--tag`, `-t` | No | Semantic version tag (e.g., `1.0.0`) |

## Limitations

- Dependencies must be explicitly versioned or hashed; symbolic references like `latest` or `dev` are not allowed.
- Dependency-free datasets cannot infer network configuration; include at least one dependency.
- Table SQL must satisfy the validation constraints listed above.
- Jinja is deterministic-only: no database access or filesystem/network operations during rendering.
- Using `env_var()` makes builds environment-dependent; avoid for reproducible builds.
- The legacy bridge is required until servers support the new package format directly.

## References

- [dataset-registry](dataset-registry.md) - Dependency: Manifest storage and version tags
- [data](data.md) - Related: Data layer overview

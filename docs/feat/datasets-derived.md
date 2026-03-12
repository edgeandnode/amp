---
name: "datasets-derived"
description: "Derived dataset definition with SQL transformations, dependency management, topological sorting, and user-defined functions. Load when asking about derived datasets, SQL views, manifest kind, dataset dependencies, or custom functions"
type: feature
status: stable
components: "crate:datasets-derived"
---

# Derived Datasets

## Summary

Derived datasets transform and combine data from existing datasets using SQL views. They declare dependencies on other datasets by alias, define tables as SQL queries over those dependencies, and optionally include user-defined functions. Tables are topologically sorted by their inter-table dependencies to ensure correct evaluation order. The kind string for derived datasets is `"manifest"` (not `"derived"`).

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Configuration](#configuration)
3. [Usage](#usage)
4. [Implementation](#implementation)
5. [Limitations](#limitations)
6. [References](#references)

## Key Concepts

- **Kind String**: Derived datasets use `"manifest"` as their kind identifier, not `"derived"`. This is the value used in the `kind` field of the JSON manifest
- **Dependencies**: Named references to other datasets, aliased for use in SQL queries. Each dependency is identified by a `DepAlias` key and a `DepReference` value
- **SQL Views**: Each table is defined by a SQL query (`SELECT ...`) that references dependency tables using the alias prefix (e.g., `src.blocks` where `src` is the dependency alias)
- **Topological Sorting**: Tables within a derived dataset may reference each other. The system topologically sorts tables by their dependencies to determine evaluation order
- **User-Defined Functions (UDFs)**: Optional custom functions that can be referenced in SQL views, defined inline in the manifest

## Configuration

For the complete field reference, see the [manifest schema](../schemas/manifest/derived.spec.json).

### Example Manifest

```json
{
  "kind": "manifest",
  "dependencies": {
    "eth": "my-namespace/eth-mainnet"
  },
  "tables": {
    "daily_tx_count": {
      "input": {
        "sql": "SELECT DATE_TRUNC('day', timestamp) AS day, COUNT(*) AS tx_count FROM eth.transactions GROUP BY 1"
      },
      "schema": {
        "arrow": {
          "fields": [
            { "name": "day", "type": "Timestamp(Nanosecond, Some(\"UTC\"))", "nullable": false },
            { "name": "tx_count", "type": "Int64", "nullable": false }
          ]
        }
      }
    }
  },
  "functions": {}
}
```

### Manifest Fields

| Field | Type | Description |
|-------|------|-------------|
| `kind` | `"manifest"` | Dataset kind identifier (always `"manifest"` for derived datasets) |
| `dependencies` | object | Map of alias to dataset reference (e.g., `"eth": "namespace/dataset"`) |
| `tables` | object | Map of table name to table definition with SQL view and schema |
| `functions` | object | Optional map of function name to UDF definition |

### Table Definition

Each table requires an `input` with a SQL query and an explicit `schema`:

```json
{
  "input": {
    "sql": "SELECT column1, column2 FROM dep_alias.source_table WHERE condition"
  },
  "schema": {
    "arrow": {
      "fields": [
        { "name": "column1", "type": "Utf8", "nullable": false },
        { "name": "column2", "type": "UInt64", "nullable": true }
      ]
    }
  }
}
```

## Usage

### Referencing Dependencies

Dependencies are declared at the manifest level and referenced in SQL using dot notation:

```sql
-- Given dependency: "eth": "my-namespace/eth-mainnet"
SELECT hash, gas_used FROM eth.blocks WHERE number > 1000000
```

### User-Defined Functions

Custom functions can be declared in the `functions` field and used in SQL views. See the UDF feature docs for available function types and formats.

## Implementation

### Source Files

- `crates/core/datasets-derived/src/manifest.rs` — `Manifest`, `Table`, `TableInput`, `View` types
- `crates/core/datasets-derived/src/dataset.rs` — `dataset()` factory, `Dataset` impl, topological sorting
- `crates/core/datasets-derived/src/dataset_kind.rs` — `DerivedDatasetKind` (kind string: `"manifest"`)
- `crates/core/datasets-derived/src/deps.rs` — Dependency alias and reference types
- `crates/core/datasets-derived/src/sql.rs` — SQL parsing and table dependency extraction
- `crates/core/datasets-derived/src/sorting.rs` — Topological sort for table evaluation order
- `crates/core/datasets-derived/src/function.rs` — UDF types and sources

## Limitations

- Circular table dependencies within a single derived dataset cause a `SortTablesByDependenciesError`
- SQL parsing errors in view definitions are reported as `DatasetError::ParseSql`
- The kind string `"manifest"` is a historical naming choice; it does not mean the dataset is a manifest itself

## References

- [datasets](datasets.md) - Base: Dataset system overview
- [datasets-manifest](datasets-manifest.md) - Related: Manifest format
- [datasets-derived-inter-table-dependencies](datasets-derived-inter-table-dependencies.md) - Child: Inter-table dependency resolution

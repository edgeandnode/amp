---
name: "data-inter-table-dependencies"
description: "Inter-table dependencies within derived datasets: self-qualified table references, topological ordering, cycle detection. Load when asking about tables referencing other tables in the same dataset"
type: feature
status: unstable
components: "service:admin-api,service:server,crate:common,crate:datasets-derived,crate:worker-datasets-derived"
---

# Inter-Table Dependencies

## Summary

Derived datasets can contain tables that reference other tables within the same dataset. For example, `table_b` can `SELECT` from `table_a` using a `self.`-qualified table reference (e.g., `SELECT * FROM self.table_a`). This syntax is consistent with how UDFs reference same-dataset functions (e.g., `self.myFunction()`). The system validates that inter-table dependencies form a directed acyclic graph (DAG) and rejects cyclic references with a `CYCLIC_DEPENDENCY` error.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Usage](#usage)
3. [Architecture](#architecture)
4. [API Reference](#api-reference)
5. [Implementation](#implementation)
6. [Limitations](#limitations)

## Key Concepts

- **Inter-table dependency**: A table within a derived dataset that references another table in the same dataset via its SQL query
- **Self-qualified table reference**: A `self.`-prefixed SQL table name (e.g., `SELECT * FROM self.my_table`) that resolves to a sibling table in the same dataset, consistent with the `self.` convention used for UDFs
- **Topological ordering**: Processing tables in dependency order so that each table is validated after all tables it depends on have been validated
- **Cycle detection**: Verification that inter-table dependencies do not form circular references (e.g., table_a depends on table_b which depends on table_a)

## Usage

### Defining inter-table dependencies

Reference sibling tables using `self.`-qualified table names in SQL queries:

```typescript
export default defineDataset(() => ({
  name: "my_dataset",
  network: "mainnet",
  dependencies: {
    eth_firehose: "_/eth_firehose@0.0.0",
  },
  tables: {
    // Base table: references an external dependency (qualified by dep alias)
    blocks_base: {
      sql: "SELECT block_num, gas_limit, miner FROM eth_firehose.blocks",
      network: "mainnet",
    },
    // Derived table: references a sibling table (self-qualified)
    blocks_summary: {
      sql: "SELECT block_num, miner FROM self.blocks_base",
      network: "mainnet",
    },
  },
}))
```

External dependencies use dependency-alias-qualified references (`eth_firehose.blocks`), while inter-table references use the `self.` prefix (`self.blocks_base`). This is consistent with how UDFs reference same-dataset functions (`self.myFunction()`).

### Cycle detection errors

If tables form a circular dependency, both the schema endpoint and manifest registration return a `CYCLIC_DEPENDENCY` error:

```json
{
  "error_code": "CYCLIC_DEPENDENCY",
  "error_message": "Cyclic dependency detected among inter-table references: ..."
}
```

## Architecture

### Validation flow

1. **Parse SQL** for all tables in the dataset
2. **Extract `self.`-qualified table references** from each table's SQL that match sibling table names
3. **Build dependency graph** mapping each table to its intra-dataset dependencies
4. **Topological sort** the graph, detecting cycles (returns `CYCLIC_DEPENDENCY` on failure)
5. **Process tables in topological order**: validate each table's SQL, infer its schema, and register it with the self-schema provider so subsequent tables can reference it

### Self-schema resolution

The `SelfSchemaProvider` is registered under the `"self"` schema in the `AmpCatalogProvider`. When a SQL query contains `self.table_name`, DataFusion resolves it through this provider, which holds schemas for already-processed sibling tables. This is the same mechanism used for UDF resolution (`self.functionName()`).

## API Reference

Inter-table dependency validation occurs at two endpoints:

| Endpoint | Method | Validation |
|----------|--------|------------|
| `/schema` | POST | Schema inference with inter-table resolution |
| `/manifests` | POST | Manifest registration with inter-table validation |

### Error codes

| Code | Status | Description |
|------|--------|-------------|
| `SELF_REFERENCING_TABLE` | 400 | A table references itself via `self.<own_name>` |
| `SELF_REF_TABLE_NOT_FOUND` | 400 | A `self.`-qualified reference targets a table that does not exist in the dataset |
| `CYCLIC_DEPENDENCY` | 400 | Inter-table references form a cycle |
| `NO_TABLE_REFERENCES` | 400 | Table SQL references no source tables (e.g., `SELECT 1`) |
| `CATALOG_QUALIFIED_TABLE` | 400 | Table uses unsupported 3-part catalog-qualified reference (e.g., `catalog.schema.table`) |
| `INVALID_TABLE_NAME` | 400 | Table name in SQL reference does not conform to identifier rules |

## Implementation

### Source files

#### Validation (admin API)

- `crates/core/common/src/self_schema_provider.rs` - `SelfSchemaProvider::add_table()` for progressive schema registration
- `crates/services/admin-api/src/handlers/schema.rs` - Schema endpoint with topological ordering and cycle detection
- `crates/services/admin-api/src/handlers/common.rs` - Manifest validation with topological ordering and cycle detection
- `crates/core/datasets-derived/src/sorting.rs` - `topological_sort()` and `CyclicDepError` used by both handlers

#### Runtime (dump and query)

- `crates/core/worker-datasets-derived/src/job_impl.rs` - Passes sibling `PhysicalTable` map to each table's materialization
- `crates/core/worker-datasets-derived/src/job_impl/table.rs` - Splits `self.` refs from external deps, registers sibling tables in both planning and execution phases
- `crates/core/common/src/catalog/physical/for_dump.rs` - `resolve_external_deps()` + `build_catalog()` for composable catalog construction

## Limitations

- Inter-table references must use the `self.` prefix; dataset-name-qualified self-references (e.g., `my_dataset.my_table`) are not supported because the dataset name is not yet assigned at validation time
- All tables referenced via `self.` must exist in the same dataset definition; `self.` references to non-existent tables will cause planning errors

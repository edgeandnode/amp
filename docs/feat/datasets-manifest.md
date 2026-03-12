---
name: "datasets-manifest"
description: "Dataset manifest format, JSON structure, content-addressable storage, and schema definitions shared across all dataset kinds. Load when asking about manifest format, manifest fields, or manifest schemas"
type: feature
status: stable
components: "crate:datasets-common,crate:datasets-registry"
---

# Dataset Manifest

## Summary

A dataset manifest is a JSON document that fully describes a dataset — its kind, tables, Arrow schemas, and kind-specific configuration. Manifests are content-addressable: a SHA-256 hash of the canonical JSON produces the [hash reference](../glossary.md#hash-reference) used to identify each [revision](../glossary.md#revision). All dataset kinds share the same manifest envelope with a `kind` discriminator field.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Configuration](#configuration)
3. [Usage](#usage)
4. [Implementation](#implementation)
5. [References](#references)

## Key Concepts

- **Content-Addressable Storage**: Each manifest is identified by a SHA-256 hash of its canonical JSON, ensuring immutability and deduplication
- **Kind Discriminator**: The `kind` field determines which dataset implementation parses the manifest (e.g., `"evm-rpc"`, `"manifest"`, `"static"`)
- **Table Schema**: An ordered list of named Arrow fields per table, defining column names, data types, and nullability
- **JSON Schema**: Each kind has a generated JSON schema at `docs/schemas/manifest/{kind}.spec.json` for validation

## Configuration

### Common Fields

All manifests share these top-level fields:

| Field | Type | Description |
|-------|------|-------------|
| `kind` | string | Dataset kind identifier |
| `tables` | object | Map of table name to table definition |

Kind-specific fields (e.g., `network`, `start_block`, `dependencies`) are documented in each kind's feature doc.


## Usage

### Table Schema Definition

Each table requires an explicit Arrow schema. The schema is an ordered list of fields:

```json
{
  "schema": {
    "arrow": {
      "fields": [
        { "name": "block_number", "type": "UInt64", "nullable": false },
        { "name": "hash", "type": "Utf8", "nullable": false },
        { "name": "value", "type": "Decimal128(38, 0)", "nullable": true }
      ]
    }
  }
}
```

Supported data types follow Arrow conventions: `UInt64`, `Int64`, `Float64`, `Utf8`, `Boolean`, `Binary`, `Timestamp(Nanosecond, Some("UTC"))`, `Decimal128(precision, scale)`, and others defined in the `DataType` enum.

### Table Naming

Table names are validated SQL identifiers: 1-63 characters, starting with a letter or underscore, containing only `[a-zA-Z0-9_$]`.

## Implementation

### Source Files

- `crates/core/datasets-common/src/manifest.rs` — `Manifest`, `Schema`, `TableSchema`, `ArrowSchema`, `Field`, `DataType` types
- `crates/core/datasets-common/src/dataset.rs` — `Dataset` and `Table` traits
- `crates/core/datasets-common/src/dataset_kind_str.rs` — `DatasetKindStr` type-erased kind identifier
- `crates/core/datasets-common/src/table_name.rs` — `TableName` validated SQL identifier
- `crates/core/datasets-common/src/hash_reference.rs` — `HashReference` content-addressable identifier
- `crates/core/datasets-registry/` — Dataset registry for manifest storage and version management ([README](../../crates/core/datasets-registry/README.md))

## References

- [datasets](datasets.md) - Base: Dataset system overview

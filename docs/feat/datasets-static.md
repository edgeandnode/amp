---
name: "datasets-static"
description: "Static dataset definition with CSV-backed data files, explicit Arrow schemas, and file path validation. Load when asking about static datasets, CSV datasets, or static manifest format"
type: feature
status: development
components: "crate:amp-datasets-static"
---

# Static Datasets

## Summary

Static datasets define tables backed by static data files (currently CSV) served from an object store. Unlike raw datasets, static datasets have no blockchain network, no block concept, and no `_block_num` column. Each table specifies a validated relative file path, file format, explicit Arrow schema, and optional sort order. The data files are resolved relative to the static provider's object store root.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Manifest](#manifest)
3. [Schema](#schema)
4. [Implementation](#implementation)
5. [References](#references)

## Key Concepts

- **File Path Validation**: Table file paths are validated at parse time — must be relative, use forward slashes, have a file extension, and contain no path traversal (`..`) components
- **File Format**: Currently supports CSV with configurable header row detection. The format is specified inline in the table definition using the `format` and `has_header` fields
- **Sort Order**: Tables may declare `sorted_by` columns, indicating the physical ordering of rows in the data file
- **No Block Concept**: Static datasets do not have `start_block`, `_block_num`, or `finalized_blocks_only` — they represent non-temporal reference data

## Manifest

See the [static dataset manifest schema](../manifest-schemas/static.spec.json) for the complete field reference, types, defaults, and examples.

## Schema

Each table defines its own explicit Arrow schema inline in the manifest. There is no fixed set of tables — users declare tables with arbitrary names, each specifying its own fields, file path, and format.

## Implementation

### Source Files

- `crates/core/datasets-static/src/manifest.rs` — `Manifest`, `Table` types with validation
- `crates/core/datasets-static/src/dataset.rs` — `dataset()` factory, `Dataset` and `Table` implementations
- `crates/core/datasets-static/src/dataset_kind.rs` — `StaticDatasetKind` (kind string: `"static"`)
- `crates/core/datasets-static/src/file_path.rs` — `FilePath` validated newtype with parse-time validation
- `crates/core/datasets-static/src/file_format.rs` — `FileFormat` enum (currently CSV only)

## References

- [datasets](datasets.md) - Base: Dataset system overview
- [datasets-manifest](datasets-manifest.md) - Related: Manifest format
- [provider-static](provider-static.md) - Related: Static provider and object store configuration

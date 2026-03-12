---
name: "provider-static"
description: "Static provider for CSV-backed datasets via object store with in-memory caching and schema inference. Load when asking about static providers, CSV data sources, or object store providers"
type: feature
status: development
components: "crate:providers-static"
---

# Static Provider

## Summary

The static provider enables serving pre-computed CSV datasets from object stores. Unlike blockchain providers that extract data from live networks, static providers read CSV files from configurable object store roots (local filesystem, S3, GCS) and serve them as queryable datasets. The provider supports in-memory caching for repeated access and automatic schema inference from CSV content.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Configuration](#configuration)
3. [Architecture](#architecture)
4. [Usage](#usage)
5. [Limitations](#limitations)
6. [References](#references)

## Key Concepts

- **Object Store Root**: The base URL pointing to the directory containing CSV files, supporting `file://`, `s3://`, and `gs://` schemes via `amp-object-store`
- **Schema Inference**: Automatic detection of column types by sampling rows from CSV files, controlled by `schema_inference_max_rows`
- **In-Memory Cache**: An optional byte-budget cache that keeps frequently accessed objects in memory, controlled by `in_memory_max_bytes`
- **Static Provider Kind**: The `"static"` provider kind identifier, distinct from blockchain provider kinds (`evm-rpc`, `firehose`, `solana`)

## Configuration

For the complete field reference, see the [config schema](../schemas/providers/static.spec.json).

### Example Configuration

```toml
kind = "static"
object_store_root = "file:///data/static-datasets"

# Optional: increase cache to 50 MiB
in_memory_max_bytes = 52428800

# Optional: sample more rows for schema inference
schema_inference_max_rows = 512
```

### Object Store Schemes

```toml
# Local filesystem
object_store_root = "file:///data/static-datasets"

# Amazon S3
object_store_root = "s3://my-bucket/datasets"

# Google Cloud Storage
object_store_root = "gs://my-bucket/datasets"
```

## Architecture

### Provider Resolution

Unlike blockchain providers that match on `(kind, network)`, static providers do not require a `network` field. Resolution matches on `kind = "static"` alone.

```
Dataset Manifest → Provider Resolution → Static Provider Config → Object Store → CSV Files
    (kind)           (find match)        (object_store_root)     (amp-object-store)  (data)
```

### Namespace Model

Static datasets are addressed as `<provider>.<dataset-ref>.<file-entry>`, where:

- **provider**: The registered provider name
- **dataset-ref**: Identifies the dataset within the manifest
- **file-entry**: The specific CSV file within the object store

### Data Flow

1. **Registration**: Provider config (TOML) is stored with `kind = "static"` and an `object_store_root`
2. **Manifest Generation**: Schema is inferred from CSV headers and sampled rows
3. **Query Time**: CSV files are read from the object store, optionally served from the in-memory cache

## Usage

### Registering a Static Provider

Store a provider config as a TOML file:

```toml
# File: /etc/amp/providers/my-static-data.toml
kind = "static"
object_store_root = "file:///data/csv-datasets"
in_memory_max_bytes = 10485760
schema_inference_max_rows = 256
```

### Querying Static Data

Once registered and a dataset manifest references this provider, query the data via SQL:

```sql
SELECT * FROM my_dataset.prices WHERE date > '2024-01-01'
```

## Limitations

- **CSV only (MVP)**: Only CSV file format is supported; no Parquet, JSON, or other formats
- **No file watching**: Changes to CSV files require manual re-registration or restart
- **No HTTP URLs**: Object store root must use `file://`, `s3://`, or `gs://` schemes; HTTP/HTTPS URLs are not supported
- **No runtime schema overrides**: Schema is inferred automatically; manual schema definitions are not supported
- **No network field**: Static providers do not participate in network-based provider resolution

## References

- [provider](provider.md) - Base: Provider system overview
- [provider-config](provider-config.md) - Related: Provider configuration format and storage

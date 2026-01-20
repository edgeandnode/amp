---
name: "data"
description: "Data lake architecture with tables, revisions, and parquet storage in object stores. Load when working with data organization, storage hierarchy, or table management"
type: "meta"
components: "crate:data-store,crate:common,crate:metadata-db"
---

# Data

## Summary

Amp's data lake provides high-performance analytical query capabilities over blockchain data using the FDAP stack (Flight, DataFusion, Arrow, Parquet).
Data is stored as immutable parquet files in object stores (S3, GCS, local filesystem), organized into tables and revisions.
Each table represents a logical collection of blockchain data (e.g., logs, transactions), and revisions are immutable snapshots identified by UUIDv7 that enable point-in-time queries and safe concurrent access.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Architecture](#architecture)
   - [Technology Stack](#technology-stack)
   - [Storage Model](#storage-model)
   - [Immutability Design](#immutability-design)

## Key Concepts

- **Table**: A logical collection of blockchain data organized by type (logs, transactions, blocks, etc.). Tables are defined in dataset manifests with schema, sort order, and partitioning.
- **Revision**: An immutable snapshot of a table's data at a point in time, identified by a UUIDv7. Each revision lives at a unique path in object storage and contains zero or more parquet files. UUIDv7 provides temporal ordering—newer revisions have lexicographically greater identifiers.
- **Active Revision**: The single revision per table currently used for queries. Only one revision is active at a time; previous revisions are retained but not queried.
- **Parquet File**: A columnar storage file containing blockchain data organized by block ranges. Files are named `{block_num:09}-{suffix:016x}.parquet` where `block_num` is the starting block number (9 digits) and `suffix` is a random 16-character hex value for uniqueness.

## Architecture

### Technology Stack

Amp's data lake is built on the FDAP stack:

- **Flight**: Arrow Flight protocol for high-performance data transfer
- **DataFusion**: Query engine providing SQL execution and optimization
- **Arrow**: In-memory columnar format for efficient processing
- **Parquet**: On-disk columnar format for compressed storage

This stack enables analytical queries over blockchain data with minimal data movement and maximal parallelism.

### Storage Model

Data is stored in object stores (S3, GCS, or local filesystem) which provide:

- **Durability**: Multi-region replication and versioning
- **Scalability**: Unlimited storage capacity
- **Cost efficiency**: Tiered storage with infrequent access pricing
- **Immutability**: Write-once semantics align with revision model

**Hierarchy:**

```
<base>/
└── <dataset_namespace>/<dataset_name>/
    └── <table_name>/
        └── <revision_uuid>/              # UUIDv7, lexicographically ordered
            ├── 000000000-xxxx.parquet    # block_num=0
            ├── 000015000-yyyy.parquet    # block_num=15000
            └── ...
```

| Level    | Identifier                         | Cardinality                | Mutability           |
|----------|------------------------------------|----------------------------|----------------------|
| Base     | Object store URL                   | One per deployment         | Configuration        |
| Dataset  | `namespace/name`                   | Many per deployment        | Immutable identifier |
| Table    | `table_name`                       | Many per dataset           | Defined in manifest  |
| Revision | UUIDv7                             | Many per table, one active | Immutable snapshot   |
| File     | `{block:09}-{suffix:016x}.parquet` | Many per revision          | Write-once           |

**Example path:**

```
s3://amp-data-lake/ethereum/mainnet/logs/01930eaf-67e5-7b5e-80b0-8d3f2a5c4e1b/000000000-a1b2c3d4e5f67890.parquet
```

- Base: `s3://amp-data-lake`
- Dataset: `ethereum/mainnet`
- Table: `logs`
- Revision: `01930eaf-67e5-7b5e-80b0-8d3f2a5c4e1b`
- File: `000000000-a1b2c3d4e5f67890.parquet` (blocks starting at 0)

### Immutability Design

The data lake uses an immutable, append-only model:

**Revision semantics:**

- New data creates new revisions (never modifies existing)
- UUIDv7 provides temporal ordering (newer = lexicographically greater)
- Single active revision per table serves queries
- Retained revisions enable point-in-time recovery

**Concurrency model:**

- Writers create new revisions while readers query existing ones
- Active revision switch is atomic (single metadata operation)
- No read-write contention by design

---
name: "data-store"
description: "Object store abstraction for datasets with PhyTableRevision lifecycle, file management, and parquet metadata caching. Load when working with physical table storage, revision management, or object store operations"
type: "component"
components: "crate:data-store,crate:metadata-db,crate:object-store"
---

# Data Store

## Summary

The Data Store provides the storage abstraction layer for dataset tables, bridging domain logic and physical storage.
It manages the complete lifecycle of table revisions—immutable snapshots of table data stored in object stores (S3, GCS, local filesystem).
The store tracks all file metadata in a PostgreSQL database and caches parsed parquet footers in memory, enabling fast query planning without touching object storage on the hot path.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Usage](#usage)
3. [Architecture](#architecture)
4. [Responsibility Boundaries](#responsibility-boundaries)
5. [Implementation](#implementation)
6. [References](#references)

## Key Concepts

For foundational concepts (tables, revisions, active revisions, storage hierarchy), see [data](data.md).

Data Store-specific concepts:

- **File Registration**: The process of recording parquet file metadata (size, e_tag, version, parquet stats, raw footer bytes) in the metadata database after writing to object storage, enabling queries without scanning object storage.
- **Restoration**: Recovering table revision state from object storage when the metadata database is empty (e.g., after database reset or migration). The system scans object storage, identifies revisions, and registers the latest as active.
- **Metadata Caching**: In-memory caching of parsed parquet metadata with memory-weighted eviction. For caching architecture details, see [data-metadata-caching](data-metadata-caching.md).

## Usage

### Creating a New Revision

When a writer job starts ingesting data for a table, it creates a new revision:

1. Generate a new UUIDv7 identifier for temporal ordering
2. Construct the storage path: `<dataset>/<table>/<uuid>/`
3. Register the revision in the metadata database
4. Mark it as the active revision (previous active revision becomes retained)
5. Lock the revision to the writer job for exclusive access

The new revision becomes the query target immediately, while the writer populates it with files.

### Registering Files

After writing each parquet file to object storage, the writer registers its metadata:

**What gets registered:**
- File name and full URL in object storage
- Object metadata: size, etag, version
- Parquet footer bytes (including page indices)
- Computed statistics for query optimization

This metadata enables fast query planning without reading from object storage.

### Querying File Metadata

Query engines retrieve file metadata from the DataStore:

**For query planning:**
- List all files in the active revision
- Access cached parquet metadata (schema, statistics, page indices)
- Prune files based on predicate pushdown
- Generate optimized execution plans

**Caching behavior:**
- Cache hit: Immediate memory access
- Cache miss: Database fetch, parse, then cache
- No object storage access on hot path

### Restoring from Object Storage

When the metadata database is empty but data exists in object storage (e.g., after database reset or migration):

1. Scan the table directory in object storage
2. Identify all revision UUIDs from directory names
3. Select the latest revision by lexicographic ordering
4. Register it as the active revision in the database

Files within the revision must be re-registered separately by scanning the revision directory and extracting footer metadata from each parquet file.

## Architecture

The Data Store architecture centers on two core abstractions: table revisions for organizing data in object storage, and a metadata layer for fast access to file information.
Writers create revisions and register files; readers query the metadata database and cache to locate and prune files without scanning object storage.

For foundational concepts about tables, revisions, and storage hierarchy, see [data](data.md).

### Logical vs Physical Tables

DataStore bridges the gap between logical tables (schema definitions in dataset manifests) and their physical representation (parquet files in object stores).

**Logical tables** define:
- Column names and types
- Sort order for efficient querying
- Partitioning strategy
- Table-level metadata

**Physical tables** are the materialized storage that DataStore manages:
- Parquet files organized by revisions in object stores
- Metadata tracking in PostgreSQL (file locations, statistics, footers)
- Active revision tracking for queries
- Revision lifecycle (creation, locking, retention)

This separation enables schema evolution without rewriting data and allows the domain layer (`PhysicalTable` in `common` crate) to work with logical definitions while DataStore handles the physical storage details.

### Metadata Storage and Caching

The metadata database stores file locations, object metadata, parquet statistics, and raw footer bytes to avoid expensive object storage access during query planning.
For the full caching architecture (two-tier design, memory-weighted eviction, and DataFusion integration), see [data-metadata-caching](data-metadata-caching.md).

### Table Revision Management

The DataStore manages table revisions through three primary operations: creation, restoration, and locking.

**Creation** occurs when a writer job starts producing data for a table.
The system generates a new UUIDv7 (ensuring temporal ordering), constructs the storage path, registers the revision in the metadata database, and marks it as active.
The previous active revision (if any) is marked inactive but retained in storage for potential recovery.

**Restoration** is used when the metadata database is empty but data exists in object storage (e.g., after a database reset or when connecting to existing data).
The system scans the table's directory in object storage, identifies all revision UUIDs, selects the latest by lexicographic ordering, and registers it as active.
File metadata must be re-registered separately after restoration.

**Locking** establishes exclusive write access to a revision.
When a writer job claims a revision, it updates the `writer` column to reference its job ID.
This prevents concurrent writes and enables the system to track which job is responsible for a revision's contents.
If a job fails, the lock can be released by clearing the writer reference.

### Parquet File Management

DataStore manages the lifecycle of parquet files within revisions:

**File naming**: Files follow the `{block_num:09}-{suffix:016x}.parquet` format (see [data](data.md#key-concepts) for details).

**File properties**:
- Each file covers a contiguous range of blocks
- Footer contains schema, row group statistics, and page indices for query optimization
- Compression is configurable per-column (typically Snappy or ZSTD)

**Immutability**: Files are never modified once written. Updates occur by:
- Creating new files in new revisions
- Compaction creates new files with broader block ranges and marks originals for cleanup

## Responsibility Boundaries

**DataStore IS responsible for:**

- Table revision lifecycle: create, restore, lock
- File registration and metadata storage in the database
- Parquet metadata caching with memory-weighted eviction
- Object store operations: read, write, list, delete files
- Streaming file metadata from revisions

**DataStore is NOT responsible for:**

- Domain-specific table logic (handled by `PhysicalTable` in `common` crate)
- Dataset manifest management
- Query execution

## Implementation

### Component Interaction

The DataStore sits between domain logic and storage infrastructure.
Domain components like PhysicalTable delegate all storage operations to DataStore, which coordinates between the object store (for actual file I/O) and the metadata database (for tracking file locations, sizes, and parquet statistics).

```
┌─────────────────┐     ┌───────────────────┐     ┌─────────────────┐
│  PhysicalTable  │────▶│     DataStore     │────▶│   ObjectStore   │
│   (domain)      │     │    (storage)      │     │   (S3/GCS/FS)   │
└─────────────────┘     └───────────────────┘     └─────────────────┘
                                │
                                ▼
                        ┌───────────────────┐
                        │   MetadataDb      │
                        │  (file registry)  │
                        └───────────────────┘
```

### Database Schema

The metadata database uses two primary tables to track revisions and files:

**`physical_tables`** - Stores table revision records.
Each row represents a revision with its storage path and active status.
Key columns: `id` (LocationId), `manifest_hash`, `dataset_namespace`, `dataset_name`, `table_name`, `path`, `active`, `writer` (job reference).
The `active` boolean ensures only one revision per table is queryable.
The `path` column stores the relative revision path (`<dataset_name>/<table_name>/<uuid>`).

**`file_metadata`** - Stores parquet file records within revisions.
Each row represents a single parquet file with its object store metadata.
Key columns: `id` (FileId), `location_id` (foreign key to physical_tables), `file_name`, `url` (full file URL), `object_size`, `object_e_tag`, `object_version`, `metadata` (parquet stats as JSON), `footer` (raw footer bytes).
Files are linked to revisions via `location_id`, enabling cascade deletes when revisions are removed.

**`footer_cache`** - Stores raw parquet footer bytes for cache population.
Linked to `file_metadata` by `file_id`.
Footer bytes are inserted into both `file_metadata.footer` and `footer_cache` for redundancy.
The cache table enables efficient footer retrieval without loading full file records.

**Relationships:**

- **Revision → Files**: A revision (`physical_tables`) has many files (`file_metadata`) via `location_id`.
  Deleting a revision cascades to delete all its files.
- **Revision → Writer**: A revision can be locked by a writer job via the `writer` column (foreign key to `jobs`).
  This prevents concurrent writes by associating the revision with a specific job.
  Only the owning job should write files to that revision.
- **File → Footer Cache**: Each file has a corresponding entry in `footer_cache` via `file_id`.
  The footer is stored in both `file_metadata` and `footer_cache` to allow efficient streaming of file metadata without loading large footer blobs during bulk operations.

### Source Files

- `crates/core/data-store/src/lib.rs` - `DataStore` struct providing revision lifecycle (create, restore, lock, get active), file registration, streaming file metadata, and cached parquet metadata access. Also defines `PhyTableRevision` (the core revision handle) and `PhyTableRevisionFileMetadata`.
- `crates/core/data-store/src/physical_table.rs` - Path and URL types: `PhyTableRevisionPath` (relative path like `dataset/table/uuid`), `PhyTablePath` (table directory path without revision), `PhyTableUrl` (full object store URL).
- `crates/core/data-store/src/file_name.rs` - `FileName` type for validated parquet filenames with format `{block_num:09}-{suffix:016x}.parquet` (9-digit block number, 16-char hex suffix).
- `crates/core/metadata-db/src/physical_table.rs` - Database operations for `physical_tables`: register, get active, mark active/inactive, assign writer.
- `crates/core/metadata-db/src/files.rs` - Database operations for `file_metadata` and `footer_cache`: register files with footers, stream by location, get footer bytes.

## References

- [data](data.md) - Parent: Data lake architecture overview
- [data-metadata-caching](data-metadata-caching.md) - Related: Parquet metadata caching architecture
- [providers-registry](provider-registry.md) - Related: Another storage registry pattern

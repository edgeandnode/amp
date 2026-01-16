---
name: "dataset-registry"
description: "DatasetsRegistry for manifest storage, version tags (latest/dev/semantic), revision resolution. Load when working with dataset versioning or manifest management"
components: "crate:amp-datasets-registry,crate:metadata-db,crate:amp-object-store"
---

# Dataset Registry

## Summary

The Dataset Registry (`DatasetsRegistry`) is the authoritative local registry for managing dataset revisions and version tags. It stores and retrieves dataset manifests (content-addressed by hash), manages version tags (semantic versions, "latest", "dev"), tracks dataset-to-manifest relationships, and resolves revision references to concrete manifest hashes.

The registry is the complete local inventory: it stores manifests and tracks how they're named/tagged/linked, but has no knowledge of *what's inside* those manifests or *how* to use them.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Architecture](#architecture)
3. [Implementation](#implementation)

## Key Concepts

- **Manifest**: A JSON file describing a dataset's schema, source, and configuration. Stored content-addressed by its hash.

- **Version Tag**: A named pointer to a manifest hash. Three types exist:
  - **Semantic versions** (e.g., `1.2.3`): Explicit version numbers set by users
  - **"latest"**: Automatically updated to point to the highest semantic version
  - **"dev"**: Points to the most recently linked manifest (development head)

- **Dataset Link**: An association between a dataset (namespace/name) and a manifest hash. A dataset can have multiple linked manifests (different versions).

- **Revision**: A reference that can be resolved to a manifest hash. Can be a version tag, hash, "latest", or "dev".

- **Hash Reference**: A fully resolved reference containing namespace, name, and concrete manifest hash.

## Architecture

The registry uses a two-layer storage architecture:

### Storage Layers

1. **Metadata Database** (PostgreSQL): Stores manifest metadata, version tags, and dataset-manifest links. Provides transactional consistency and querying capabilities.

2. **Object Store** (S3/GCS/local): Stores actual manifest file content, addressed by hash. Provides durable storage for manifest JSON files.

### Data Flow

1. **Registration**: Manifest content is stored in object store first, then metadata is registered in database
2. **Resolution**: Database is queried for manifest path, then content is fetched from object store
3. **Deletion**: Uses transactions with row-level locking to ensure manifests aren't deleted while linked

## Implementation

### Database Tables

The registry uses these tables in the metadata database:

| Table | Purpose |
|-------|---------|
| `manifests` | Stores manifest hash and object store path |
| `dataset_manifests` | Links datasets (namespace/name) to manifest hashes |
| `dataset_tags` | Stores version tags pointing to manifest hashes |

### Source Files

- `crates/core/datasets-registry/src/lib.rs` - Main `DatasetsRegistry` struct and methods
- `crates/core/datasets-registry/src/error.rs` - Error types for registry operations
- `crates/core/datasets-registry/src/manifests.rs` - Object store operations for manifest files

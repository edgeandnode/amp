---
name: "dataset-registry"
description: "DatasetsRegistry for manifest storage, version tags (latest/dev/semantic), revision resolution. Load when working with dataset versioning or manifest management"
components: "crate:datasets-registry,crate:metadata-db"
---

# Dataset Registry

## Summary

The Dataset Registry (`DatasetsRegistry`) is the authoritative local registry for managing dataset revisions and version tags. It stores and retrieves dataset manifests (content-addressed by hash), manages version tags (semantic versions, "latest", "dev"), tracks dataset-to-manifest relationships, and resolves revision references to concrete manifest hashes.

The registry is the complete local inventory: it stores manifests and tracks how they're named/tagged/linked, but has no knowledge of *what's inside* those manifests or *how* to use them.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Architecture](#architecture)
3. [Usage](#usage)
4. [API Reference](#api-reference)
5. [Implementation](#implementation)
6. [References](#references)

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

## Usage

### Creating a Registry

```rust
use amp_datasets_registry::{DatasetsRegistry, manifests::DatasetManifestsStore};

let manifests_store = DatasetManifestsStore::new(object_store);
let registry = DatasetsRegistry::new(metadata_db, manifests_store);
```

### Registering a Manifest

```rust
// Store manifest without linking to any dataset
let hash = Hash::from_str("abc123...")?;
registry.register_manifest(&hash, manifest_json).await?;
```

### Linking and Versioning

```rust
// Link manifest to dataset (also sets "dev" tag)
registry.link_manifest(&namespace, &name, &hash).await?;

// Set a semantic version (also updates "latest" if higher)
let version = Version::new(1, 0, 0);
registry.set_dataset_version_tag(&namespace, &name, &version, &hash).await?;
```

### Resolving Revisions

```rust
// Resolve "latest" tag
let hash = registry.resolve_latest_version_hash(&namespace, &name).await?;

// Resolve any revision reference
let hash_ref = registry.resolve_revision(&reference).await?;
```

### Garbage Collection

```rust
// Find orphaned manifests (not linked to any dataset)
let orphaned = registry.list_orphaned_manifests().await?;

// Delete orphaned manifests
for hash in orphaned {
    registry.delete_manifest(&hash).await?;
}
```

## API Reference

### Manifest Management

| Method | Description |
|--------|-------------|
| `register_manifest(hash, content)` | Store manifest in object store and database |
| `get_manifest(hash)` | Retrieve manifest content by hash |
| `delete_manifest(hash)` | Delete unlinked manifest from both stores |
| `list_all_manifests()` | List all manifests with usage counts |
| `list_orphaned_manifests()` | List manifests with no dataset links |

### Version Tags

| Method | Description |
|--------|-------------|
| `set_dataset_version_tag(ns, name, version, hash)` | Set semantic version, auto-update "latest" |
| `delete_dataset_version_tag(ns, name, version)` | Remove a version tag |
| `list_dataset_version_tags(ns, name)` | List all versions for a dataset |

### Revision Resolution

| Method | Description |
|--------|-------------|
| `resolve_revision(reference)` | Resolve any revision to hash reference |
| `resolve_latest_version_hash(ns, name)` | Get hash for "latest" tag |
| `resolve_dev_version_hash(ns, name)` | Get hash for "dev" tag |
| `resolve_version_hash(ns, name, version)` | Get hash for specific version |

### Dataset Links

| Method | Description |
|--------|-------------|
| `link_manifest(ns, name, hash)` | Link manifest to dataset, set "dev" tag |
| `unlink_dataset_manifests(ns, name)` | Remove all links for a dataset |
| `is_manifest_linked(ns, name, hash)` | Check if manifest is linked |
| `list_manifest_linked_datasets(hash)` | List datasets using a manifest |

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

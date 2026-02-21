---
name: "app-ampctl-dataset"
description: "Dataset lifecycle commands: register, deploy, list, inspect, versions, manifest, restore. Load when asking about managing datasets via ampctl CLI"
type: feature
status: stable
components: "app:ampctl,crate:admin-client"
---

# Dataset Management

## Summary

Dataset commands provide full lifecycle management for blockchain datasets. Operators can register manifests as named datasets with version tags, deploy datasets to start extraction jobs, inspect registered datasets and their versions, retrieve raw manifests, and restore dataset metadata from object storage after recovery scenarios.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Usage](#usage)
3. [API Reference](#api-reference)
4. [Implementation](#implementation)
5. [References](#references)

## Key Concepts

- **Dataset Reference**: Identifies a dataset as `namespace/name@version` (e.g., `ethereum/mainnet@1.0.0`)
- **Version Tag**: Semantic version (e.g., `1.0.0`) or special tags `latest` and `dev` (system-managed)
- **Deployment**: Scheduling an extraction job that syncs blockchain data for a dataset
- **Restore**: Re-indexing dataset metadata from existing data in object storage

## Usage

**Register and tag a dataset:**

Store a manifest and associate it with a named dataset. Without `--tag`, only the `dev` tag is updated.

```bash
# Register a dataset (updates "dev" tag)
ampctl dataset register my_namespace/my_dataset ./manifest.json

# Register and tag with a semantic version
ampctl dataset register my_namespace/my_dataset ./manifest.json --tag 1.0.0

# Using the alias
ampctl dataset reg my_namespace/my_dataset ./manifest.json -t 1.0.0

# Manifest can be loaded from object storage
ampctl dataset register my_namespace/my_dataset s3://bucket/manifest.json --tag 2.0.0
```

**Deploy a dataset for extraction:**

Start an extraction job to sync blockchain data. By default, syncing runs continuously.

```bash
# Deploy with continuous syncing (default)
ampctl dataset deploy my_namespace/my_dataset@1.0.0

# Stop at the latest block at deployment time
ampctl dataset deploy my_namespace/my_dataset@1.0.0 --end-block latest

# Stop at a specific block number
ampctl dataset deploy my_namespace/my_dataset@1.0.0 --end-block 5000000

# Stay 100 blocks behind chain tip
ampctl dataset deploy my_namespace/my_dataset@1.0.0 --end-block -100

# Run with multiple parallel workers
ampctl dataset deploy my_namespace/my_dataset@1.0.0 --parallelism 4

# Assign to a specific worker
ampctl dataset deploy my_namespace/my_dataset@1.0.0 --worker-id my-worker
```

**List registered datasets:**

View all datasets with their latest version tags and available versions.

```bash
ampctl dataset list
ampctl dataset ls  # alias
```

**Inspect a dataset:**

Get dataset details for a specific version, including manifest content hash and dataset kind.

```bash
# Inspect latest version
ampctl dataset inspect my_namespace/my_dataset

# Inspect a specific version
ampctl dataset inspect my_namespace/my_dataset@1.2.0

# Inspect the dev version
ampctl dataset inspect my_namespace/my_dataset@dev

# Using the alias
ampctl dataset get my_namespace/my_dataset@latest
```

**List dataset versions:**

View all available versions with their manifest hashes and timestamps.

```bash
ampctl dataset versions my_namespace/my_dataset
```

**View dataset manifest:**

Retrieve the raw manifest JSON for a dataset version.

```bash
# Latest version manifest
ampctl dataset manifest my_namespace/my_dataset

# Specific version
ampctl dataset manifest my_namespace/my_dataset@1.2.0
```

**Restore from object storage:**

Re-index dataset metadata from existing data in object storage. Useful for recovery after metadata loss, setting up new systems with pre-existing data, or re-syncing after storage restoration.

```bash
# Restore all tables for a specific version
ampctl dataset restore my_namespace/my_dataset@1.0.0

# Restore latest version
ampctl dataset restore my_namespace/my_dataset@latest

# Restore a single table (discovers latest revision from storage)
ampctl dataset restore my_namespace/my_dataset@1.0.0 --table blocks

# Restore a single table with a specific location ID
ampctl dataset restore my_namespace/my_dataset@1.0.0 --table blocks --location-id 42
```

**JSON output for scripting:**

```bash
ampctl dataset list --json
ampctl dataset inspect my_namespace/my_dataset --json | jq '.kind'
ampctl dataset restore my_namespace/my_dataset@1.0.0 --json
```

## API Reference

For request/response schemas, see [Admin API OpenAPI spec](../openapi-specs/admin.spec.json):

```bash
jq '.paths | to_entries[] | select(.key | startswith("/datasets"))' docs/openapi-specs/admin.spec.json
```

## Implementation

### Source Files

- `crates/bin/ampctl/src/cmd/dataset/` - CLI command implementations
- `crates/clients/admin/src/datasets.rs` - Admin API client library
- `crates/services/admin-api/src/handlers/datasets/` - API endpoint handlers

## References

- [app-ampctl](app-ampctl.md) - Base: ampctl overview
- [admin](admin.md) - Related: Administration overview
- [dataset-registry](dataset-registry.md) - Related: Dataset registry component

---
name: "app-ampctl-manifest"
description: "Manifest storage commands: generate, register, list, inspect, remove, prune. Load when asking about manifest management, content-addressable storage, or manifest hashes via ampctl"
type: feature
status: stable
components: "app:ampctl,crate:admin-client"
---

# Manifest Management

## Summary

Manifest commands manage content-addressable manifest storage independently from datasets. Operators can generate manifests for supported dataset kinds, register manifests for deduplication and integrity verification, list and inspect stored manifests, remove individual manifests, and prune orphaned manifests to reclaim storage.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Usage](#usage)
3. [API Reference](#api-reference)
4. [Implementation](#implementation)
5. [References](#references)

## Key Concepts

- **Manifest**: A JSON document describing a dataset's schema, tables, and extraction configuration
- **Content-Addressable Storage**: Manifests are identified by their SHA-256 content hash, enabling deduplication
- **Orphaned Manifest**: A manifest with no dataset links (`dataset_count: 0`), eligible for pruning
- **Dataset Kind**: The type of blockchain data source (`evm-rpc`, `firehose`, `solana`)

## Usage

**Generate a manifest:**

Create a manifest JSON for a supported dataset kind. Derived datasets require manual creation.

```bash
# Generate an EVM RPC manifest for Ethereum mainnet
ampctl manifest generate --kind evm-rpc --network mainnet

# Generate a Firehose manifest, output to file
ampctl manifest generate --kind firehose --network mainnet --out ./manifest.json

# Generate with a custom start block
ampctl manifest generate --kind solana --network mainnet --start-block 100000

# Only finalized blocks
ampctl manifest generate --kind evm-rpc --network mainnet --finalized-blocks-only
```

**Register a manifest:**

Store a manifest in content-addressable storage. The same content always produces the same hash.

```bash
# Register from local file
ampctl manifest register ./manifests/eth_mainnet.json

# Register from S3
ampctl manifest register s3://my-bucket/manifests/eth.json

# Register from GCS
ampctl manifest register gs://manifests/polygon.json

```

**List registered manifests:**

View all manifests with their hashes and linked dataset counts. Manifests with `dataset_count: 0` are orphaned.

```bash
ampctl manifest list
ampctl manifest ls  # alias
```

**Inspect a manifest:**

Retrieve and display a manifest by its content hash.

```bash
# Inspect by hash
ampctl manifest inspect <hash>
ampctl manifest get <hash>  # alias

# Save to file
ampctl manifest inspect <hash> > manifest.json

# Extract specific fields
ampctl manifest inspect <hash> | jq '.name'
```

**Remove a manifest:**

Delete a manifest from storage. Manifests linked to datasets cannot be deleted (returns 409 Conflict). Deleting a non-existent manifest is treated as success.

```bash
ampctl manifest rm <hash>
ampctl manifest remove <hash>  # alias
```

**Prune orphaned manifests:**

Remove all manifests not linked to any datasets. Safe to run repeatedly.

```bash
ampctl manifest prune
# Output: "Pruned 15 orphaned manifest(s)"
```

**JSON output for scripting:**

```bash
ampctl manifest list --json
ampctl manifest list --json | jq '.manifests[] | select(.dataset_count == 0) | .hash'
```

## API Reference

For request/response schemas, see [Admin API OpenAPI spec](../openapi-specs/admin.spec.json):

```bash
jq '.paths | to_entries[] | select(.key | startswith("/manifests"))' docs/openapi-specs/admin.spec.json
```

## Implementation

### Source Files

- `crates/bin/ampctl/src/cmd/manifest/` - CLI command implementations
- `crates/bin/ampctl/src/cmd/manifest/generate.rs` - Manifest generation logic
- `crates/clients/admin/src/manifests.rs` - Admin API client library

## References

- [app-ampctl](app-ampctl.md) - Base: ampctl overview
- [dataset-registry](dataset-registry.md) - Related: Dataset registry and manifest storage

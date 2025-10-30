# Amp Dataset Versioning System

## Overview

The Amp platform uses a **content-addressable storage model** with **semantic versioning** for dataset manifests. This document describes the versioning architecture, storage model, and operational semantics.

## Core Concepts

### Content-Addressable Storage

Manifests are stored by their **SHA-256 content hash**, ensuring:
- **Deduplication**: Identical manifests are stored only once
- **Immutability**: Hash changes if content changes
- **Integrity**: Content can be verified against its hash
- **Cacheability**: Hash-based lookups are deterministic

### Three-Tier Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Object Store (S3/GCS/etc)                   │
│  Manifests stored by hash: manifests/<hash>.json                │
└─────────────────────────────────────────────────────────────────┘
                              ▲
                              │
┌─────────────────────────────────────────────────────────────────┐
│                    PostgreSQL Metadata DB                        │
│                                                                  │
│  ┌────────────────┐  ┌──────────────────┐  ┌────────────────┐  │
│  │ manifest_files │  │ dataset_manifests│  │     tags       │  │
│  │                │  │                  │  │                │  │
│  │ hash -> path   │  │ (ns, name, hash) │  │ (ns,name,ver)  │  │
│  └────────────────┘  └──────────────────┘  └────────────────┘  │
│         │                     │                     │           │
│         └─────────────────────┴─────────────────────┘           │
│          Content-addressable    Many-to-many    Version tags    │
│             storage               linking         (semver)      │
└─────────────────────────────────────────────────────────────────┘
```

**Layer 1: Object Store**
- Physical storage of manifest JSON files
- Path format: `manifests/<64-char-hex-hash>.json`
- Supports: S3, GCS, Azure Blob, local filesystem

**Layer 2: Manifest Registry** (`manifest_files` table)
- Maps manifest hash → object store path
- Enables existence checks without object store queries
- Tracks creation timestamps

**Layer 3a: Dataset Linking** (`dataset_manifests` table)
- Many-to-many relationship: datasets ↔ manifests
- Allows multiple datasets to share the same manifest
- Enables manifest lifecycle management (prevent deletion of in-use manifests)

**Layer 3b: Version Tags** (`tags` table)
- Human-readable names pointing to manifest hashes
- Supports semantic versions and special tags
- Enables versioned references and rollbacks

## Database Schema

### manifest_files

Stores manifest metadata and object store location.

```sql
CREATE TABLE manifest_files (
    hash TEXT PRIMARY KEY,                           -- SHA-256 hash (64 hex chars, lowercase)
    path TEXT NOT NULL,                              -- Object store path
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

**Columns**:
- `hash`: SHA-256 digest of canonical manifest JSON (64 lowercase hex chars, no `0x` prefix)
- `path`: Object store path (e.g., `manifests/abc123...def.json`)
- `created_at`: Registration timestamp (UTC)

**Properties**:
- Primary key on `hash` prevents duplicate registration
- No foreign keys (this is the root table)

### dataset_manifests

Many-to-many junction table linking datasets to manifests.

```sql
CREATE TABLE dataset_manifests (
    namespace TEXT NOT NULL,
    name TEXT NOT NULL,
    hash TEXT NOT NULL REFERENCES manifest_files(hash) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (namespace, name, hash)
);

CREATE INDEX idx_dataset_manifests_hash ON dataset_manifests(hash);
CREATE INDEX idx_dataset_manifests_dataset ON dataset_manifests(namespace, name);
```

**Columns**:
- `namespace`: Dataset namespace (e.g., `edgeandnode`, `_`)
- `name`: Dataset name (e.g., `eth_mainnet`)
- `hash`: Reference to manifest in `manifest_files`
- `created_at`: Link creation timestamp

**Relationships**:
- Foreign key to `manifest_files(hash)` with `CASCADE DELETE`
- Composite primary key prevents duplicate links

**Properties**:
- One manifest can be linked to multiple datasets (deduplication)
- One dataset can have multiple manifests (version history)
- Deleting a manifest cascades to remove all links

### tags

Version identifiers and symbolic names pointing to manifest hashes.

```sql
CREATE TABLE tags (
    namespace TEXT NOT NULL,
    name TEXT NOT NULL,
    version TEXT NOT NULL,                           -- Semantic version or special tag
    hash TEXT NOT NULL,                              -- Points to specific manifest
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (namespace, name, version),
    FOREIGN KEY (namespace, name, hash)
        REFERENCES dataset_manifests(namespace, name, hash)
        ON DELETE CASCADE
);

CREATE INDEX idx_tags_dataset ON tags(namespace, name);
```

**Columns**:
- `namespace`: Dataset namespace
- `name`: Dataset name
- `version`: Version identifier (semantic version or special tag)
- `hash`: Manifest hash this version points to
- `created_at`: Tag creation timestamp
- `updated_at`: Last update timestamp (for tag moves)

**Relationships**:
- Foreign key to `dataset_manifests(namespace, name, hash)` with `CASCADE DELETE`
- Composite primary key on `(namespace, name, version)` prevents duplicate tags

**Tag Types**:
1. **Semantic versions**: `1.0.0`, `2.1.3`, `0.0.1-alpha`
2. **Special tags**: `latest`, `dev`

## Revision Types

The system supports four types of revision identifiers:

### 1. Semantic Version

**Format**: `MAJOR.MINOR.PATCH[-PRERELEASE][+BUILD]`

**Examples**:
- `1.0.0` - Production release
- `2.1.3` - Minor update
- `1.0.0-alpha` - Pre-release
- `1.0.0+20241120` - Build metadata

**Properties**:
- Follows [Semantic Versioning 2.0.0](https://semver.org/)
- User-created during registration
- Immutable once created (updating requires new registration)
- Used for production deployments

**Resolution**: Direct lookup in `tags` table

### 2. Manifest Hash

**Format**: 64-character lowercase hexadecimal string (SHA-256)

**Example**: `b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9`

**Properties**:
- Computed from canonical JSON serialization
- Immutable and globally unique
- Content-addressable
- Used for exact manifest references

**Resolution**: Direct lookup in `manifest_files` table

### 3. Special Tag: "latest"

**Meaning**: The highest semantic version (by semver ordering)

**Properties**:
- Automatically updated when a higher version is registered
- Transactional: uses `SELECT FOR UPDATE` to prevent race conditions
- Only considers semantic versions (excludes pre-releases by default)
- Used for production "stable" deployments

**Resolution**: Query `tags` table for highest semver, return its hash

### 4. Special Tag: "dev"

**Meaning**: The most recently registered manifest

**Properties**:
- Automatically updated on every `POST /datasets` call
- Used for development and testing
- May point to any manifest (no version requirements)
- Mutable and frequently changing

**Resolution**: Direct lookup in `tags` table

## Registration Flow

### Two-Step Atomic Operation

The `POST /datasets` endpoint performs two atomic operations:

#### Step 1: Register Manifest and Link to Dataset

```
POST /datasets
Body: { namespace, name, version, manifest }

↓ Parse and validate manifest structure
↓ Canonicalize manifest (deterministic JSON serialization)
↓ Compute SHA-256 hash from canonical JSON
↓
↓ Call dataset_store.register_manifest_and_link():
  │
  ├─ Store manifest in object store
  │  └─ PUT manifests/<hash>.json
  │  └─ Idempotent: content-addressable storage
  │
  ├─ Register in manifest_files table
  │  └─ INSERT INTO manifest_files (hash, path)
  │  └─ ON CONFLICT DO NOTHING (idempotent)
  │
  └─ BEGIN TRANSACTION
      │
      ├─ Link manifest to dataset
      │  └─ INSERT INTO dataset_manifests (namespace, name, hash)
      │  └─ ON CONFLICT DO NOTHING (idempotent)
      │
      └─ Update "dev" tag
         └─ UPSERT INTO tags (namespace, name, 'dev', hash)
         └─ Always points to most recently registered manifest

     COMMIT TRANSACTION
```

**Properties**:
- **Idempotent**: Safe to retry if transaction fails
- **Atomic**: "dev" tag always points to a valid, linked manifest
- **Automatic**: No manual "dev" tag management required

#### Step 2: Create/Update Version Tag

```
↓ Call dataset_store.set_dataset_version_tag(namespace, name, version, hash):
  │
  └─ BEGIN TRANSACTION
      │
      ├─ Upsert version tag
      │  └─ UPSERT INTO tags (namespace, name, version, hash)
      │  └─ Updates updated_at on conflict
      │
      ├─ Lock "latest" tag row
      │  └─ SELECT * FROM tags WHERE version = 'latest' FOR UPDATE
      │  └─ Prevents concurrent modifications
      │
      ├─ Compare versions
      │  └─ IF version > current_latest OR current_latest IS NULL:
      │
      └─ Update "latest" tag (if needed)
         └─ UPSERT INTO tags (namespace, name, 'latest', hash)
         └─ Only if new version is higher

     COMMIT TRANSACTION
```

**Properties**:
- **Idempotent**: Re-registering same version with same manifest succeeds
- **Atomic**: "latest" tag always points to highest version
- **Transactional**: Row-level lock prevents race conditions
- **Automatic**: No manual "latest" tag management required

### Registration Example

```
POST /datasets
{
  "namespace": "_",
  "name": "eth_mainnet",
  "version": "1.2.0",
  "manifest": "{ ... }"
}

↓ Canonicalize and hash manifest → abc123...def

Step 1: register_manifest_and_link()
  ├─ Store manifests/abc123...def.json in S3
  ├─ INSERT INTO manifest_files (hash='abc123...def', path='manifests/abc123...def.json')
  ├─ INSERT INTO dataset_manifests (namespace='_', name='eth_mainnet', hash='abc123...def')
  └─ UPSERT tags (namespace='_', name='eth_mainnet', version='dev', hash='abc123...def')

Step 2: set_dataset_version_tag()
  ├─ UPSERT tags (namespace='_', name='eth_mainnet', version='1.2.0', hash='abc123...def')
  └─ IF 1.2.0 > current_latest:
      └─ UPSERT tags (namespace='_', name='eth_mainnet', version='latest', hash='abc123...def')

Result:
  tags table now contains:
    (_, eth_mainnet, '1.2.0', abc123...def)
    (_, eth_mainnet, 'dev',   abc123...def)
    (_, eth_mainnet, 'latest', abc123...def)  ← Only if 1.2.0 was highest
```

## Revision Resolution

### Resolution Algorithm

The `resolve_dataset_revision()` method converts a revision reference into a concrete manifest hash:

```rust
pub async fn resolve_dataset_revision(
    namespace: &Namespace,
    name: &Name,
    revision: &Revision,
) -> Result<Option<Hash>> {
    match revision {
        // Hash is already concrete, just verify it exists
        Revision::Hash(hash) => {
            let exists = manifest_exists(hash).await?;
            Ok(exists.then_some(hash.clone()))
        }

        // Semantic version: direct lookup in tags table
        Revision::Version(version) => {
            SELECT hash FROM tags
            WHERE namespace = $1 AND name = $2 AND version = $3
        }

        // Latest: find highest semantic version
        Revision::Latest => {
            SELECT hash FROM tags
            WHERE namespace = $1 AND name = $2
              AND version ~ '^[0-9]+\.[0-9]+\.[0-9]+.*'  -- Semantic versions only
            ORDER BY version DESC  -- Semver ordering
            LIMIT 1
        }

        // Dev: direct lookup in tags table
        Revision::Dev => {
            SELECT hash FROM tags
            WHERE namespace = $1 AND name = $2 AND version = 'dev'
        }
    }
}
```

### Resolution Examples

**Example 1: Version → Hash**
```
Input:  namespace="_", name="eth_mainnet", revision="1.2.0"
Query:  SELECT hash FROM tags WHERE namespace='_' AND name='eth_mainnet' AND version='1.2.0'
Output: Some("abc123...def")
```

**Example 2: Latest → Hash**
```
Input:  namespace="_", name="eth_mainnet", revision="latest"
Query:  SELECT hash FROM tags WHERE namespace='_' AND name='eth_mainnet'
        AND version LIKE '[0-9]%' ORDER BY version DESC LIMIT 1
Output: Some("abc123...def")  -- Hash of version 1.2.0 (highest)
```

**Example 3: Dev → Hash**
```
Input:  namespace="_", name="eth_mainnet", revision="dev"
Query:  SELECT hash FROM tags WHERE namespace='_' AND name='eth_mainnet' AND version='dev'
Output: Some("xyz789...abc")  -- Most recently registered manifest
```

**Example 4: Hash → Hash**
```
Input:  namespace="_", name="eth_mainnet", revision="abc123...def"
Query:  SELECT path FROM manifest_files WHERE hash='abc123...def'
Output: Some("abc123...def")  -- Verified to exist
```

## Deployment Flow

When deploying a dataset via `POST /datasets/{namespace}/{name}/versions/{revision}/deploy`:

```
1. Resolve revision to manifest hash
   ↓
   revision="latest" → hash="abc123...def"

2. Find which version tag points to this hash
   ↓
   Query: SELECT version FROM tags
          WHERE namespace=$1 AND name=$2 AND hash=$3
          AND version ~ '^[0-9]'  -- Semantic versions only
   ↓
   Result: version="1.2.0"

3. Load full dataset using resolved version
   ↓
   dataset_store.get_dataset(name="eth_mainnet", version="1.2.0")

4. Schedule extraction job
   ↓
   scheduler.schedule_dataset_dump(dataset, end_block, parallelism)
```

**Why resolve to version, not use hash directly?**
- The scheduler needs a version identifier for job tracking
- Historical jobs are recorded with version tags, not hashes
- Allows correlation between deployed jobs and registered versions

## Lifecycle Management

### Manifest Deletion

Manifests can only be deleted if **no datasets link to them**:

```sql
-- Check for links
SELECT COUNT(*) FROM dataset_manifests WHERE hash = $1;

-- If count = 0, safe to delete
BEGIN TRANSACTION;
  DELETE FROM manifest_files WHERE hash = $1;  -- Cascades to tags
  DELETE FROM object store: manifests/<hash>.json;
COMMIT;
```

**Garbage Collection**:
- Query `list_orphaned_manifests()` to find unlinked manifests
- Batch delete orphaned manifests periodically
- Prevents object store bloat

### Tag Lifecycle

**Semantic Version Tags**:
- Created explicitly during registration
- Never automatically deleted
- Can be manually deleted (leaves manifest intact)

**"latest" Tag**:
- Created/updated automatically when higher version registered
- Never deleted (unless dataset deleted)
- Always points to highest semver

**"dev" Tag**:
- Created/updated automatically on every registration
- Never deleted (unless dataset deleted)
- Always points to most recent registration

### Dataset Deletion

Deleting a dataset cascades through the schema:

```sql
-- Delete dataset link
DELETE FROM dataset_manifests WHERE namespace = $1 AND name = $2;

-- Cascades to:
--   1. All tags for this dataset (via FK constraint)
--   2. If manifest has no other links, eligible for GC
```

## Concurrency and Consistency

### Race Condition Prevention

**Problem**: Two concurrent registrations of different versions could cause "latest" tag inconsistency.

**Solution**: Row-level locking with `SELECT FOR UPDATE`

```sql
BEGIN TRANSACTION;
  -- Lock the "latest" row to prevent concurrent modifications
  SELECT * FROM tags
  WHERE namespace = $1 AND name = $2 AND version = 'latest'
  FOR UPDATE;

  -- Now we have exclusive access to decide if we should update latest
  -- Other transactions block here until we commit

  IF new_version > current_latest THEN
    UPDATE tags SET hash = $3 WHERE ... version = 'latest';
  END IF;
COMMIT;
```

### Idempotency

All operations are idempotent:

**Manifest Storage**:
- Content-addressable: same content → same hash → same path
- Object store: `PUT` is idempotent
- Database: `ON CONFLICT DO NOTHING`

**Dataset Linking**:
- `INSERT ... ON CONFLICT DO NOTHING`
- Safe to retry on transaction failure

**Version Tagging**:
- `INSERT ... ON CONFLICT DO UPDATE`
- Re-tagging same version with same hash succeeds (no-op)
- Re-tagging same version with different hash updates (version change)

## Best Practices

### For Development

- Use `dev` tag for active development
- Register manifests without semantic versions (auto-updates `dev`)
- Deploy using `--reference namespace/name@dev`

### For Production

- Use semantic versions: `1.0.0`, `1.1.0`, etc.
- Deploy using `--reference namespace/name@latest` (stable)
- Pin critical deployments to specific versions: `--reference namespace/name@1.0.0`
- Never manually modify `latest` tag (auto-managed)

### For Reproducibility

- Reference by manifest hash when exact reproducibility required
- Hash never changes, guaranteed immutable
- Use for audits, compliance, research

### Version Numbering

Follow semantic versioning:
- **MAJOR**: Breaking changes to schema or queries
- **MINOR**: New tables or backward-compatible features
- **PATCH**: Bug fixes, optimizations

## Related Files

**Database**:
- `crates/core/metadata-db/migrations/20251016093912_add_manifests_and_tags_tables.sql` - Schema definition

**Rust Implementation**:
- `crates/core/dataset-store/src/lib.rs` - Core versioning API
- `crates/services/admin-api/src/handlers/datasets/register.rs` - Registration handler
- `crates/services/admin-api/src/handlers/datasets/deploy.rs` - Deployment handler

**Type Definitions**:
- `crates/common/datasets-common/src/revision.rs` - Revision enum (Rust)
- `crates/common/datasets-common/src/version.rs` - Semantic version (Rust)
- `crates/common/datasets-common/src/hash.rs` - Manifest hash (Rust)
- `typescript/amp/src/Model.ts` - Revision types (TypeScript)

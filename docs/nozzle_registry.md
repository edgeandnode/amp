# Nozzle Registry API

A data layer and api for registry and discovery services for nozzle datasets within The Graph ecosystem. This platform serves as a centralized database and API layer to host, maintain, and make discoverable deployed nozzle datasets.

## Database Schema

The nozzle registry uses [GEL Database](https://docs.geldata.com/) with the following core models:

### Dataset Model

The `Dataset` type represents a deployed nozzle dataset and extends both `DatasetDiscovery` and `DatasetMetadata` abstract types.

**Core Properties:**

- `dataset_id` (required): Globally unique, content-addressable ID
- `name` (required): Slugified name (e.g., "ethereum-transfers")
- `namespace` (required): Grouping mechanism (org/team/wallet address)
- `status` (required): Current status (Draft, Published, Deprecated, Archived)
- `created_at` (required): Timestamp when dataset was created

**Discovery Properties (from DatasetDiscovery):**

- `description`: Dataset description (max 1024 chars)
- `keywords`: Array of usage keywords (e.g., "ERC20", "transfers", "NFT")
- `indexing_chains` (required): Array of CAIP-2 chain IDs (e.g., "eip155:1")
- `source` (required): Array of data sources (contracts, substreams, etc.)
- `visibility` (required): Private or Public (default: Public)

**Metadata Properties (from DatasetMetadata):**

- `owner` (required): ID of dataset owner
- `repository_url`: Optional GitHub repository URL
- `runtime_config`: Optional dynamic runtime configuration (JSON)
- `license`: Optional license information
- `mutability`: Dataset mutability (Mutable, Snapshot, AppendOnly)
- `ancestors`: Multi-link to other datasets this extends from

**Computed Properties:**

- `dataset_name`: Full name as "{namespace}/{name}"
- `latest_version`: Latest published version ordered by creation date
- `descendants`: Datasets that have this dataset as an ancestor

**Constraints:**

- `dataset_id` must be exclusive (unique)
- Combination of `namespace` and `name` must be exclusive
- `name` must match regex `^[a-z][a-z0-9_-]*$`

### DatasetVersion Model

The `DatasetVersion` type represents a deployed version of a Dataset. Each Dataset can have multiple versions.

**Properties:**

- `label` (required): Version label (e.g., "v0.0.1", "367e521")
- `dataset_version_id` (required): Content-addressable reference to this version
- `status` (required): Version status (default: Draft)
- `changelog`: Optional description of changes in this version
- `breaking`: Boolean indicating if version has breaking changes (default: false)
- `manifest` (required): Location of the dataset manifest
- `created_at` (required): Timestamp when version was created

**Computed Properties:**

- `dataset`: Backlink to the Dataset that owns this version

**Relationships:**

- Each DatasetVersion belongs to exactly one Dataset
- When a Dataset is deleted, orphaned DatasetVersions are also deleted

**Indexes:**

- Index on `dataset_version_id`
- Index on `label`

### Enums

**DatasetStatus:**

- `Draft`: Work in progress
- `Published`: Available for use
- `Deprecated`: No longer recommended
- `Archived`: Preserved but inactive

**DatasetVisibility:**

- `Private`: Only owner(s) can view/find
- `Public`: Anyone can discover

**DatasetMutability:**

- `Mutable`: Data can be modified
- `Snapshot`: Immutable point-in-time data
- `AppendOnly`: Data can only be added to

## Database Management Commands

Use these `just` commands for managing the GEL database migrations:

### Creating Migrations

```bash
# Create a new migration for schema changes
just nozzle_registry__migration_create
```

### Running Migrations

```bash
# Apply all unapplied migrations
# Note: Docker image runs all migrations on startup automatically
just nozzle_registry__migrate

# View log of applied migrations
just nozzle_registry__migrate_log
```

### Database Access

```bash
# Connect to the GEL CLI for direct database interaction
just nozzle_registry__gel_cli
```

## Full-Text Search and Indexing

The schema includes sophisticated indexing for dataset discovery:

- **Trigram Index**: Enables partial/fuzzy matching on dataset names
- **Full-Text Search**: Weight-ranked FTS index with priorities:
  1. **Weight A**: Keywords (highest priority)
  2. **Weight B**: Source information
  3. **Weight C**: Indexing chains
  4. **Weight D**: Description (lowest priority)

This allows for efficient searching across dataset metadata with relevance ranking based on the field where matches are found.

## References

- [gel database](https://docs.geldata.com/).
- [nozzle dataset metadata spec](https://www.notion.so/edgeandnode/Dataset-Metadata-Spec-22d8686fc7c280048266ce8f20c00246)
- [nozzle dataset info model](https://whimsical.com/dataset-information-model-Y8fbrdb3dvJnH1BLTG84Wk)
- [nozzle dataset registry design doc](https://www.notion.so/edgeandnode/Nozzle-Data-Registry-API-V1-0-24e8686fc7c280ae9790f3a3aada24b4)

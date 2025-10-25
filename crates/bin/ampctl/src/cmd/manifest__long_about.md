The manifest command provides content-addressable storage management for dataset manifests.

Manifests are stored by their content hash, which is computed from the canonical JSON
representation. This approach enables:

- Deduplication: Identical manifest content produces the same hash
- Integrity verification: Hash guarantees content hasn't changed
- Version-independent storage: Multiple dataset versions can reference the same manifest
- Pre-registration: Manifests can be uploaded before linking to datasets
- Safe deletion: Only unlinked manifests can be removed

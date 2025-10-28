Deletes all manifests that are not linked to any datasets. This operation identifies
manifests in content-addressable storage that have no references from any dataset and
removes them to free up storage space.

Operation Behavior:
    The prune operation performs the following steps:
    1. Queries the metadata database to identify all orphaned manifests
    2. Concurrently deletes each orphaned manifest from storage
    3. Returns the count of successfully deleted manifests

    Individual deletion failures are logged but don't fail the entire operation,
    allowing partial cleanup even if some manifests cannot be removed.

When to Use:
    - After removing multiple datasets to reclaim storage space
    - As part of scheduled maintenance to clean up unused manifests
    - When investigating storage usage and identifying orphaned content
    - Before storage migration or archival operations

Idempotent Operation:
    Pruning is safe to run repeatedly. It only deletes manifests that are confirmed
    to have no dataset links. Already-deleted manifests are skipped without error.

Concurrent Deletion:
    Each orphaned manifest is deleted concurrently for performance. Individual
    deletion failures are logged but don't fail the entire operation.

Examples:
    # Prune all orphaned manifests
    ampctl manifest prune
    # Output: "Pruned 15 orphaned manifest(s)"

    # Use with custom admin URL
    ampctl manifest prune --admin-url http://production:1610

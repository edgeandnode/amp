## Examples

Prune all non-canonical segments:
$ ampctl table prune 42

Prune segments ending before block 1000000:
$ ampctl table prune 42 --before-block 1000000

Custom GC delay (2 hours = 7200 seconds):
$ ampctl table prune 42 --gc-delay 7200

Skip confirmation:
$ ampctl table prune 42 -f

With JSON output:
$ ampctl table prune 42 --json

## Notes

Schedules non-canonical segments for garbage collection. These are segments
not in the canonical chain (from reorgs, failed compaction, or orphaned data).

The `--gc-delay` option sets how long (in seconds) before files are eligible
for deletion (default: 3600, i.e., 1 hour).

The `--before-block` option limits pruning to segments ending before the
specified block number.

Writer job must be in a terminal state before pruning. Use `ampctl job stop`
if needed.

## Warning

Pruning segments may prevent streaming query clients from resuming queries
that were reading from fork data in a pruned segment. Use `--before-block`
to preserve recent non-canonical data if clients may need to recover from
reorgs.

## See Also

- `ampctl table truncate` - Delete all files from a revision
- `ampctl table delete` - Delete only the revision record
- `ampctl job stop` - Stop a writer job

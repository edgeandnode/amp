## Examples

Prune all terminal jobs (completed, stopped, error):
  $ ampctl jobs prune

Prune only completed jobs:
  $ ampctl jobs prune --status completed

Prune only error jobs:
  $ ampctl jobs prune --status error

Prune without confirmation prompt (dangerous!):
  $ ampctl jobs prune --force

Prune completed jobs without confirmation:
  $ ampctl jobs prune --status completed -f

## Notes

Prune performs bulk cleanup of terminal jobs. By default, it removes all jobs
in terminal states (completed, stopped, error). Use status filters to target
specific job types.

Pruned jobs cannot be recovered. This operation is designed for maintenance
and cleanup - use `ampctl jobs delete <id>` to remove specific jobs.

Status filters:
  - terminal: All finished jobs (complete, stopped, error) [default]
  - completed: Only successfully completed jobs
  - stopped: Only manually stopped jobs
  - error: Only failed jobs

When to use prune vs delete:
  - Use prune for bulk cleanup of old/finished jobs
  - Use delete for removing specific jobs by ID

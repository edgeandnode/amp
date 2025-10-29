## Examples

Remove a specific job by ID (with confirmation):
  $ ampctl job rm 123

Remove a specific job without confirmation (dangerous!):
  $ ampctl job rm 123 --force

Using the 'remove' alias:
  $ ampctl job remove 456

Skip confirmation prompt:
  $ ampctl job remove 456 -f

## Notes

Removed jobs cannot be recovered. Only jobs in terminal states
(completed, stopped, error) can be removed. Running or pending jobs
must be stopped first.

Use 'ampctl job rm' to remove specific jobs by ID. For bulk deletion
with status filters (e.g., all completed jobs), use 'ampctl job prune'.

The confirmation prompt can be skipped with --force/-f, but use this
option carefully as deletions are permanent.

## Examples

Remove a specific job by ID:
$ ampctl job rm 123

Using the 'remove' alias:
$ ampctl job remove 456

## Notes

Removed jobs cannot be recovered. Only jobs in terminal states
(completed, stopped, error) can be removed. Running or pending jobs
must be stopped first.

Use 'ampctl job rm' to remove specific jobs by ID. For bulk deletion
with status filters (e.g., all completed jobs), use 'ampctl job prune'.

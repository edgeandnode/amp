## Examples

Truncate a revision by location ID (with confirmation prompt):
  $ ampctl table truncate 42

Skip confirmation prompt:
  $ ampctl table truncate 42 --force
  $ ampctl table truncate 42 -f

With JSON output:
  $ ampctl table truncate 42 --force --json

With custom admin URL:
  $ ampctl table truncate 42 --force --admin-url http://prod-server:1610

## Notes

Truncates a table revision by deleting all associated files from object
storage and their metadata from the database, then removes the revision
record itself. The revision must be inactive before it can be truncated.
If the revision is currently active, deactivate it first using
`ampctl table deactivate`.

Unlike `ampctl table delete`, which only removes database records,
truncate also cleans up all files in object storage.

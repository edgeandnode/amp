## Examples

Delete a revision by location ID (with confirmation prompt):
  $ ampctl table delete 42

Skip confirmation prompt:
  $ ampctl table delete 42 --force
  $ ampctl table delete 42 -f

With JSON output:
  $ ampctl table delete 42 --force --json

With custom admin URL:
  $ ampctl table delete 42 --force --admin-url http://prod-server:1610

## Notes

Deletes a table revision and all associated file metadata from the database.
The revision must be inactive before it can be deleted. If the revision is
currently active, deactivate it first using `ampctl table deactivate`.

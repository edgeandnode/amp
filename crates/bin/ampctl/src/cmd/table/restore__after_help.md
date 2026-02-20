## Examples

Restore a revision by location ID:
  $ ampctl table restore 1

With JSON output:
  $ ampctl table restore 1 --json

With custom admin URL:
  $ ampctl table restore 1 --admin-url http://prod-server:1610

## Notes

Restores a revision by re-reading its Parquet files from object storage and
registering their metadata in the database. Use this after files have been
added to or replaced in a revision's storage path.

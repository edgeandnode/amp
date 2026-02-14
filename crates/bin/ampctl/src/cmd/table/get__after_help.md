## Examples

Get a revision by location ID:
  $ ampctl table get 1

With JSON output:
  $ ampctl table get 1 --json

With custom admin URL:
  $ ampctl table get 1 --admin-url http://prod-server:1610

## Notes

Returns revision details including path, active status, writer, and metadata
such as dataset namespace, dataset name, manifest hash, and table name.

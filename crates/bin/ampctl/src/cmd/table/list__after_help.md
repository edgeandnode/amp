## Examples

List all table revisions:
  $ ampctl table list

Filter by active status:
  $ ampctl table list --active true

Limit the number of results:
  $ ampctl table list --limit 10

Combine filters:
  $ ampctl table list --active true --limit 5

With JSON output:
  $ ampctl table list --json

With custom admin URL:
  $ ampctl table list --admin-url http://prod-server:1610

## Notes

Returns revision details including ID, dataset namespace/name, table name, and
active status. Use `ampctl table get <id>` to see full details for a specific
revision.

## Examples

List all workers:
  $ ampctl worker list

Using the `ls` alias:
  $ ampctl worker ls

With custom admin URL:
  $ ampctl worker list --admin-url http://prod-server:1610

Use jq to filter workers by recent heartbeat:
  $ ampctl worker list | jq '.workers[] | select(.heartbeat_at > "2025-01-01")'

Count total workers:
  $ ampctl worker list | jq '.workers | length'

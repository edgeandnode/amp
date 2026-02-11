## Examples

Activate a table revision:
  $ ampctl table activate shiyasmohd/mainnet@0.0.1 blocks --location-id 1

With custom admin URL:
  $ ampctl table activate shiyasmohd/mainnet@0.0.1 blocks --location-id 1 --admin-url http://prod-server:1610

## Notes

Activating a revision atomically deactivates all existing revisions for the
table and marks the specified revision as active. Only one revision per table
can be active at a time.

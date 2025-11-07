## Examples

Inspect worker details:
  $ ampctl worker inspect worker-01h2xcejqtf2nbrexx3vqjhp41

Using the `get` alias:
  $ ampctl worker get indexer-node-1

With custom admin URL:
  $ ampctl worker inspect worker-01 --admin-url http://prod-server:1610

Use jq to extract version information:
  $ ampctl worker inspect worker-01 | jq '.info.version'

Check if worker version matches expected:
  $ ampctl worker inspect worker-01 | jq -e '.info.commit_sha == "8b065bde9c1a2f3e4d5c6b7a8e9f0a1b2c3d4e5f"'

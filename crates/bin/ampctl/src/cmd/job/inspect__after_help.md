## Examples

Inspect job details:
  $ ampctl job inspect 12345

Using the `get` alias:
  $ ampctl job get 12345

With custom admin URL:
  $ ampctl job inspect 12345 --admin-url http://prod-server:1610

Use jq to extract specific fields:
  $ ampctl job inspect 12345 | jq '.status'

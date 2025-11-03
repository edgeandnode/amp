## Examples

Get the manifest for the latest version:
  $ ampctl dataset manifest my_namespace/my_dataset

Get the manifest for a specific version:
  $ ampctl dataset manifest my_namespace/my_dataset@1.2.0

Get the manifest for the dev version:
  $ ampctl dataset manifest my_namespace/my_dataset@dev

Using a specific manifest hash:
  $ ampctl dataset manifest my_namespace/my_dataset@bafybei...

## Output

Returns the raw manifest JSON content with pretty formatting.
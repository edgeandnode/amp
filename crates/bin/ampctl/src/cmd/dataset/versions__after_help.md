## Examples

List all versions of a dataset:
  $ ampctl dataset versions my_namespace/my_dataset

Using the plural alias:
  $ ampctl datasets versions my_namespace/my_dataset

## Output

Returns JSON with version information including:
- All semantic versions with their manifest hashes and timestamps
- Special tags:
  - latest: The current latest version
  - dev: The development version (manifest hash)
## Examples

Inspect the latest version of a dataset:
$ ampctl dataset inspect my_namespace/my_dataset

Inspect a specific version:
$ ampctl dataset inspect my_namespace/my_dataset@1.2.0

Inspect the dev version:
$ ampctl dataset inspect my_namespace/my_dataset@dev

Using the `get` alias:
$ ampctl datasets get my_namespace/my_dataset@latest

## Output

Returns JSON with dataset information including:

- Namespace and name
- Revision (version/tag/hash)
- Manifest content hash
- Dataset kind (manifest/evm-rpc/firehose)

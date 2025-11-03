## What This Command Does

The `register` command performs three operations:

1. **Registers the manifest**: Stores the manifest content in content-addressable storage
   - The manifest is identified by its content hash (immutable)

2. **Links to dataset**: Associates the manifest hash with your dataset (namespace/name)
   - Creates or updates the dataset FQN entry

3. **Tags the revision**: Applies a version tag to the dataset revision
   - Uses the `--tag` value if provided (e.g., 1.0.0)
   - If no tag is specified, updates the "dev" tag to point to this revision

## Examples

Register a dataset (updates "dev" tag):
  $ ampctl dataset register my_namespace/my_dataset ./manifest.json

Register and tag with a specific semantic version:
  $ ampctl dataset register my_namespace/my_dataset ./manifest.json --tag 1.0.0

Using the short flag:
  $ ampctl dataset register my_namespace/my_dataset ./manifest.json -t 2.1.3

Using the `reg` alias:
  $ ampctl dataset reg my_namespace/my_dataset ./manifest.json -t 1.0.0

## Manifest Storage

Manifest files can be loaded from various storage backends:
- Local filesystem: `./manifest.json`, `/path/to/manifest.json`
- File URL: `file:///path/to/manifest.json`
- S3: `s3://bucket-name/path/manifest.json`
- Google Cloud Storage: `gs://bucket-name/path/manifest.json`
- Azure Blob Storage: `az://container/path/manifest.json`

## Tag Requirements

- Must be a valid semantic version (e.g., 1.0.0, 2.1.3)
- Cannot use special tags "latest" or "dev" - these are managed by the system
- If no tag is provided, only the "dev" tag is updated

## Notes

- Registration is idempotent - registering the same manifest content again will succeed
- The manifest is stored once and referenced by its content hash
- Each registration creates a new dataset revision linked to the manifest
- Dependencies declared in the manifest must already exist in the system

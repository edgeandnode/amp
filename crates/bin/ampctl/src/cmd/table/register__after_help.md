## Examples

Register a table revision:
  $ ampctl table register edgeandnode/ethereum_mainnet@0.0.1 blocks custom/blocks

With custom admin URL:
  $ ampctl table register edgeandnode/ethereum_mainnet@0.0.1 blocks custom/blocks --admin-url http://prod-server:1610

## Notes

Registering a revision creates an inactive, unlinked physical table revision
record in the metadata database. The revision is not activated and no
physical_tables entry is created. Use `ampctl table activate` to activate
the revision after registration.

## Examples

Deactivate all revisions for a table:
  $ ampctl table deactivate edgeandnode/ethereum_mainnet@0.0.1 blocks

With custom admin URL:
  $ ampctl table deactivate edgeandnode/ethereum_mainnet@0.0.1 blocks --admin-url http://prod-server:1610

## Notes

Deactivating marks all revisions for the specified table as inactive. This
means no revision will be active for the table until one is explicitly activated.

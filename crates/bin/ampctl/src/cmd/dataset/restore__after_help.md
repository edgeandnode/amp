## Examples

Restore dataset physical tables from storage:

```
ampctl dataset restore my_namespace/my_dataset@1.0.0
```

Restore latest version of a dataset:

```
ampctl dataset restore my_namespace/my_dataset@latest
```

Use custom admin URL:

```
ampctl dataset restore my_namespace/my_dataset@1.0.0 --admin-url http://production:1610
```

Output in JSON format:

```
ampctl dataset restore my_namespace/my_dataset@1.0.0 --json
```

## Use Cases

**Recovery from metadata database loss:**

```
# After restoring object storage from backup, re-index metadata
ampctl dataset restore production/eth_mainnet@2.1.0
```

**Setting up a new system with pre-existing data:**

```
# New controller, but object storage already has data
ampctl dataset restore analytics/uniswap_v3@1.0.0
```

**Re-syncing metadata after storage restoration:**

```
# Storage was restored from another region/backup
ampctl dataset restore research/defi_metrics@3.0.0
```

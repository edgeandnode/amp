## Examples

Deploy dataset with continuous syncing (default):
```
ampctl dep-dataset my_namespace/my_dataset@1.0.0
```

Deploy dataset, stopping at latest block:
```
ampctl dep-dataset my_namespace/my_dataset@1.0.0 --end-block latest
```

Deploy dataset, stopping at specific block number:
```
ampctl dep-dataset my_namespace/my_dataset@1.0.0 --end-block 5000000
```

Deploy dataset, staying 100 blocks behind chain tip:
```
ampctl dep-dataset my_namespace/my_dataset@1.0.0 --end-block -100
```

Use custom admin URL:
```
ampctl dep-dataset my_namespace/my_dataset@1.0.0 --admin-url http://production:1610
```

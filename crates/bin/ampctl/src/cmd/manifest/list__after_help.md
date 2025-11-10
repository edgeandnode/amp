# EXAMPLES

List all registered manifests:

    ampctl manifest list

List manifests with custom admin API URL:

    ampctl --admin-url http://localhost:8080 manifest list

# OUTPUT

The command outputs a JSON array with all registered manifests:

```json
{
  "manifests": [
    {
      "hash": "3d1c0c2d3d7c276e18bd92a09728ecd76ad8be05ebb30656c448859f3e5c22b9",
      "dataset_count": 2
    },
    {
      "hash": "7a2f8b4c1e9d3f5a6b8c0d2e4f6a8b0c2e4f6a8b0c2e4f6a8b0c2e4f6a8b0c2e",
      "dataset_count": 0
    }
  ]
}
```

Each manifest entry includes:
- `hash`: Content-addressable SHA-256 hash (unique identifier)
- `dataset_count`: Number of datasets currently using this manifest (0 means orphaned)

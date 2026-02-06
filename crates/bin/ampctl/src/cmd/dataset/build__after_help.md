## Examples

Build dataset in current directory:
```
ampctl dataset build --output build/
```

Build dataset from specific directory:
```
ampctl dataset build --dir my_dataset/ --output build/
```

Build with custom output directory:
```
ampctl dataset build --dir my_dataset/ --output dist/
```

Override template variables:
```
ampctl dataset build --output build/ --var network=mainnet --var chain_id=1
```

Build with locked dependencies (fail if amp.lock missing or stale):
```
ampctl dataset build --output build/ --locked
```

Build with offline cache only (requires amp.lock when version refs are present):
```
ampctl dataset build --output build/ --offline
```

Build with frozen dependencies (equivalent to --locked + --offline):
```
ampctl dataset build --output build/ --frozen
```

Build with custom admin URL:
```
ampctl dataset build --output build/ --admin-url http://production:1610
```

## Build Workflow

1. Parses `amp.yaml` or `amp.yml` configuration file
2. Resolves dependencies from the admin API
3. Renders Jinja SQL templates with variables
4. Validates SELECT-only statements and incremental constraints
5. Infers table schemas via DataFusion planning
6. Writes output files to the target directory

## Output Structure

```
<output>/
  manifest.json           # Canonical manifest with file refs
  sql/
    <table>.sql           # Rendered SQL (SELECT statement)
    <table>.schema.json   # Inferred Arrow schema
  functions/
    <func>.js             # Copied function sources
```

## Lockfile Behavior

- **Default**: Creates/updates `amp.lock` if dependencies changed
- **--locked**: Fails if lockfile missing or dependencies don't match
- **--offline**: Disables network fetches; requires `amp.lock` when version refs are present; cache misses are errors
- **--frozen**: Equivalent to `--locked` + `--offline`

## Structured Output

Use `--json` to emit machine-readable output for automation.

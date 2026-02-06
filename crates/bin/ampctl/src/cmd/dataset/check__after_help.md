## Examples

Validate dataset in current directory:
```
ampctl dataset check
```

Validate dataset from specific directory:
```
ampctl dataset check --dir my_dataset/
```

Override template variables:
```
ampctl dataset check --var network=mainnet --var chain_id=1
```

Validate with locked dependencies (fail if amp.lock missing or stale):
```
ampctl dataset check --locked
```

Validate with offline cache only (requires amp.lock when version refs are present):
```
ampctl dataset check --offline
```

Validate with frozen dependencies (equivalent to --locked + --offline):
```
ampctl dataset check --frozen
```

Machine-readable output:
```
ampctl dataset check --json
```

## What It Validates

- Parses `amp.yaml` or `amp.yml` configuration
- Resolves dependencies (respecting `--locked`, `--offline`, `--frozen`)
- Renders Jinja SQL templates with variables
- Validates SELECT-only SQL and incremental constraints
- Infers table schemas via DataFusion planning

## Notes

- This command does not write build artifacts (no `sql/` output, no `manifest.json`).

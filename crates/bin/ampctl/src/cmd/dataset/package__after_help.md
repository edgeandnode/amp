## Examples

Package from current directory (when `manifest.json` is present):
```
ampctl dataset package
```

Package from build directory:
```
ampctl dataset package --dir build
```

Specify output filename:
```
ampctl dataset package --output my_dataset.tgz
```

Full workflow example:
```
ampctl dataset build --output build
ampctl dataset package --dir build --output my_dataset.tgz
ampctl dataset register my_dataset.tgz
```

## Package Contents

The archive contains all files needed for deployment:

```
dataset.tgz/
  manifest.json           # Canonical manifest with file refs
  sql/
    <table>.sql           # Rendered SQL (SELECT statement)
    <table>.schema.json   # Inferred Arrow schema
  functions/
    <func>.js             # Function sources (if any)
```

## Determinism

The package is built deterministically:
- Files are sorted alphabetically
- Timestamps are set to Unix epoch (0)
- Consistent permissions (0o644 for files, 0o755 for directories)
- No ownership metadata (uid/gid = 0)

This ensures the same inputs always produce the same package hash.

## Verification

The command prints the SHA-256 hash of the package, which can be used
to verify the package was built correctly or check for reproducibility.

## Structured Output

Use `--json` to emit machine-readable output for automation.

---
name: code-gen
description: Regenerate JSON schemas and OpenAPI specs after manifest/API changes. Use when modifying dataset manifests, API definitions, or schema fields. Outputs updated specs to docs/ directories.
---

# Code Generation Skill

This skill provides code generation operations for the project codebase, including JSON schemas, OpenAPI specs, and protobuf bindings.

## When to Use This Skill

Use this skill when:
- Dataset manifest structures change and schemas need regeneration
- API definitions are modified and OpenAPI specs need updating
- Protobuf definitions change (currently disabled)
- You need to regenerate all schemas at once

## Available Commands

All codegen tasks are in the `[codegen]` group in the justfile. To discover available commands:

```bash
just --list | grep -A100 '\[codegen\]'
```

### Key commands

| Command | When to use |
|---------|-------------|
| `just gen` | Run all codegen tasks (alias: `just codegen`) |
| `just gen-<name>` | Run a specific codegen task (see `just --list`) |

Most `gen-*` commands accept an optional `DEST_DIR` parameter to override the output location.

## How Code Generation Works

### JSON Schema Generation
1. Uses conditional compilation with `--cfg gen_schema`
2. Build scripts in `-gen` crates generate schemas during build
3. Schemas are written to `target/debug/build/<crate>/out/schema.json`
4. The just command copies the schema to the docs directory

### OpenAPI Specification Generation
1. Uses conditional compilation with `--cfg gen_openapi_spec`
2. Build scripts generate OpenAPI specs during build
3. Specs are written to `target/debug/build/<crate>/out/openapi.spec.json`
4. The just command copies the spec to the docs directory

## Important Guidelines

### When to Regenerate Schemas
- After modifying dataset manifest structures
- After changing API endpoint definitions
- When adding/removing fields from schemas
- Before committing changes to manifest-related code

### Output Locations
- **Manifest Schemas**: `docs/schemas/manifest/` (default)
- **OpenAPI Specs**: `docs/schemas/openapi/` (default)

## Concrete Examples

### Example 1: After Adding a New Field to Dataset Manifest
**Situation**: Added `indexer_url` field to EVM RPC dataset manifest
**Action**:
```bash
just gen-raw-dataset-manifest-schema
```
**Result**: Updated `docs/schemas/manifest/raw.spec.json` with new field

### Example 2: After Modifying Admin API Endpoint
**Situation**: Added new `/workers` endpoint to admin API
**Action**:
```bash
just gen-admin-api-openapi-spec
```
**Result**: Updated `docs/schemas/openapi/admin.spec.json` with new endpoint

### Example 3: Multiple Changes Across Datasets
**Situation**: Refactored field types across multiple dataset types
**Action**:
```bash
just gen  # Regenerates all schemas
```
**Result**: All schema files updated consistently

## Common Mistakes to Avoid

### ❌ Anti-patterns
- **Never manually edit generated files** - They will be overwritten
- **Never forget to regenerate** - Schema changes without regeneration cause validation errors
- **Never skip committing generated files** - They're part of the codebase
- **Never run cargo build directly for generation** - Use justfile tasks

### ✅ Best Practices
- Regenerate immediately after schema changes
- Run `just gen` when unsure which schemas changed
- Commit generated files with the changes that triggered them
- Review generated diffs to ensure correctness

## Next Steps

After generating schemas:
1. **Review the generated files** - Check diffs for correctness
2. **Test schema validation** - Ensure manifests still validate
3. **Commit both source and generated files** - Keep them in sync

## Project Context

- Generated schemas are used for validation and documentation
- Schemas should be regenerated whenever manifest structures change
- Generated files are checked into version control
- The project uses JSON Schema for dataset manifests
- OpenAPI specs document the Admin API for external consumers

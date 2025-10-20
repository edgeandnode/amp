---
name: Code Generation
description: Generate JSON schemas and OpenAPI specs using justfile tasks (just gen, just gen-*-schema). Use when user modifies dataset manifests, API definitions, adds/removes schema fields, or mentions code generation, schemas, OpenAPI specs. Outputs to docs/manifest-schemas/ and docs/openapi-specs/.
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

### Generate All Schemas
```bash
just gen
```
Runs all codegen tasks in sequence:
- Common dataset manifest schema
- Derived dataset manifest schema
- EVM RPC dataset manifest schema
- Firehose dataset manifest schema
- Admin API OpenAPI specification

**Alias**: `just codegen` (same as `gen`)

**Use this when**: Multiple schemas need regeneration or you're unsure which specific schema to regenerate.

### Generate Common Dataset Manifest Schema
```bash
just gen-common-dataset-manifest-schema [DEST_DIR]
```
Generates the JSON schema for common dataset manifests. Uses `RUSTFLAGS="--cfg gen_schema" cargo build -p datasets-common-gen`.

**Output**: `docs/manifest-schemas/common.spec.json` (default)

**Use this when**: Core dataset manifest structures in `datasets-common` change.

### Generate Derived Dataset Manifest Schema
```bash
just gen-derived-dataset-manifest-schema [DEST_DIR]
```
Generates the JSON schema for derived dataset manifests. Uses `RUSTFLAGS="--cfg gen_schema" cargo build -p datasets-derived-gen`.

**Output**: `docs/manifest-schemas/derived.spec.json` (default)

**Use this when**: Derived dataset manifest structures change.

### Generate EVM RPC Dataset Manifest Schema
```bash
just gen-evm-rpc-dataset-manifest-schema [DEST_DIR]
```
Generates the JSON schema for EVM RPC dataset definitions. Uses `RUSTFLAGS="--cfg gen_schema" cargo build -p datasets-evm-rpc-gen`.

**Output**: `docs/manifest-schemas/evm-rpc.spec.json` (default)

**Use this when**: EVM RPC dataset manifest structures change.

### Generate Firehose Dataset Manifest Schema
```bash
just gen-firehose-dataset-manifest-schema [DEST_DIR]
```
Generates the JSON schema for Firehose dataset definitions. Uses `RUSTFLAGS="--cfg gen_schema" cargo build -p datasets-firehose-gen`.

**Output**: `docs/manifest-schemas/firehose.spec.json` (default)

**Use this when**: Firehose dataset manifest structures change.

### Generate Admin API OpenAPI Specification
```bash
just gen-admin-api-openapi-spec [DEST_DIR]
```
Generates the OpenAPI specification for the Admin API. Uses `RUSTFLAGS="--cfg gen_openapi_spec" cargo build -p admin-api-gen`.

**Output**: `docs/openapi-specs/admin.spec.json` (default)

**Use this when**: Admin API endpoint definitions or schemas change.

### Generate Firehose Protobuf Bindings (Currently Disabled)
```bash
just gen-firehose-datasets-proto
```
Generates Firehose protobuf bindings. Uses `RUSTFLAGS="--cfg gen_proto" cargo build -p firehose-datasets`.

**Note**: This is currently commented out in the main `gen` task. Use only when specifically needed.

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

### Custom Destination
All generation commands accept an optional `DEST_DIR` parameter:
```bash
just gen-common-dataset-manifest-schema /custom/path
```

## Important Guidelines

### When to Regenerate Schemas
- After modifying dataset manifest structures
- After changing API endpoint definitions
- When adding/removing fields from schemas
- Before committing changes to manifest-related code

### Output Locations
- **Manifest Schemas**: `docs/manifest-schemas/` (default)
- **OpenAPI Specs**: `docs/openapi-specs/` (default)

### Generation Crates
These special `-gen` crates are used for code generation:
- `datasets-common-gen`
- `datasets-derived-gen`
- `datasets-evm-rpc-gen`
- `datasets-firehose-gen`
- `admin-api-gen`

## Concrete Examples

### Example 1: After Adding a New Field to Dataset Manifest
**Situation**: Added `indexer_url` field to EVM RPC dataset manifest
**Action**:
```bash
just gen-evm-rpc-dataset-manifest-schema
```
**Result**: Updated `docs/manifest-schemas/evm-rpc.spec.json` with new field

### Example 2: After Modifying Admin API Endpoint
**Situation**: Added new `/workers` endpoint to admin API
**Action**:
```bash
just gen-admin-api-openapi-spec
```
**Result**: Updated `docs/openapi-specs/admin.spec.json` with new endpoint

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

# JSON Schema Generation

A generation crate for common dataset manifest JSON schemas. This crate generates JSON schemas for derived and SQL dataset manifests using the schemars library for external validation and documentation purposes.

## JSON Schema Generation

The library uses a build configuration flag `gen_schema` that enables JSON schema generation during the build process. When enabled, the build script will generate JSON schemas from Rust structs using schemars for common dataset manifest validation.

To generate JSON schema bindings, run:

```bash
just gen-common-derived-dataset-manifest-schema
```

Or using the full `cargo build` command:

```bash
RUSTFLAGS="--cfg gen_schema" cargo build -p common-gen

mkdir -p docs/dataset-def-schemas
cp target/debug/build/common-gen-*/out/schema.json docs/dataset-def-schemas/derived.spec.json
cp target/debug/build/common-gen-*/out/sql_schema.json docs/dataset-def-schemas/sql.spec.json
```

This will generate JSON schemas from the common dataset manifest definitions and copy them to:
- `docs/dataset-def-schemas/derived.spec.json` for derived datasets
- `docs/dataset-def-schemas/sql.spec.json` for SQL datasets
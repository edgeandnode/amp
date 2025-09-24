# JSON Schema Generation

A generation crate for Firehose dataset definition JSON schemas. This crate generates JSON schemas for Firehose dataset definitions using the schemars library for external validation and documentation purposes.

## JSON Schema Generation

The library uses a build configuration flag `gen_schema` that enables JSON schema generation during the build process. When enabled, the build script will generate JSON schemas from Rust structs using schemars for Firehose dataset definition validation.

To generate JSON schema bindings, run:

```bash
just gen-datasets-firehose-manifest-schema
```

Or using the full `cargo build` command:

```bash
RUSTFLAGS="--cfg gen_schema" cargo build -p datasets-firehose-gen

mkdir -p docs/dataset-def-schemas
cp target/debug/build/datasets-firehose-gen-*/out/schema.json docs/dataset-def-schemas/firehose.spec.json
```

This will generate JSON schemas from the Firehose dataset definitions and copy them to `docs/dataset-def-schemas/firehose.spec.json`.
# JSON Schema Generation

A generation crate for common dataset manifest JSON schemas. This crate generates JSON schemas for dataset manifests using the schemars library for external validation and documentation purposes.

## JSON Schema Generation

The library uses a build configuration flag `gen_schema` that enables JSON schema generation during the build process. When enabled, the build script will generate JSON schemas from Rust structs using schemars for common dataset manifest validation.

To generate JSON schema bindings, run:

```bash
just gen-datasets-common-manifest-schema
```

Or using the full `cargo build` command:

```bash
RUSTFLAGS="--cfg gen_schema" cargo build -p datasets-common-gen

cp target/debug/build/datasets-common-gen-*/out/schema.json docs/manifest-schemas/common.spec.json
```

This will generate JSON schemas from the common dataset manifest definitions and copy them to `docs/dataset-def-schemas/common.spec.json`.

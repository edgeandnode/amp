# JSON Schema Generation

A generation crate for Solana dataset definition JSON schemas. This crate generates JSON schemas for Solana dataset definitions using the schemars library for external validation and documentation purposes.

## JSON Schema Generation

The library uses a build configuration flag `gen_schema` that enables JSON schema generation during the build process. When enabled, the build script will generate JSON schemas from Rust structs using schemars for Solana dataset definition validation.

To generate JSON schema bindings, run:

```bash
just gen-solana-dataset-def-schema
```

Or using the full `cargo build` command:

```bash
RUSTFLAGS="--cfg gen_schema" cargo build -p solana-gen

mkdir -p docs/manifest-schemas
cp target/debug/build/solana-gen-*/out/schema.json docs/manifest-schemas/Solana.json
```

This will generate JSON schemas from the Solana dataset definitions and copy them to `docs/dataset-def-schemas/Solana.json`.

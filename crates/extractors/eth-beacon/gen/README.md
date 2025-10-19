# JSON Schema Generation

A generation crate for ETH Beacon dataset definition JSON schemas. This crate generates JSON schemas for ETH Beacon dataset definitions using the schemars library for external validation and documentation purposes.

## JSON Schema Generation

The library uses a build configuration flag `gen_schema` that enables JSON schema generation during the build process. When enabled, the build script will generate JSON schemas from Rust structs using schemars for ETH Beacon dataset definition validation.

To generate JSON schema bindings, run:

```bash
just gen-eth-beacon-dataset-manifest-schema
```

Or using the full `cargo build` command:

```bash
RUSTFLAGS="--cfg gen_schema" cargo build -p datasets-eth-beacon-gen

mkdir -p docs/manifest-schemas
cp target/debug/build/datasets-eth-beacon-gen-*/out/schema.json docs/manifest-schemas/eth-beacon.spec.json
```

This will generate JSON schemas from the ETH Beacon dataset definitions and copy them to `docs/manifest-schemas/eth-beacon.spec.json`.

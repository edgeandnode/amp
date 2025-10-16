# JSON Schema Generation

A generation crate for EVM RPC dataset definition JSON schemas. This crate generates JSON schemas for EVM RPC dataset definitions using the schemars library for external validation and documentation purposes.

## JSON Schema Generation

The library uses a build configuration flag `gen_schema` that enables JSON schema generation during the build process. When enabled, the build script will generate JSON schemas from Rust structs using schemars for EVM RPC dataset definition validation.

To generate JSON schema bindings, run:

```bash
just gen-evm-rpc-dataset-def-schema
```

Or using the full `cargo build` command:

```bash
RUSTFLAGS="--cfg gen_schema" cargo build -p evm-rpc-gen

mkdir -p docs/manifest-schemas
cp target/debug/build/evm-rpc-gen-*/out/schema.json docs/manifest-schemas/EvmRpc.json
```

This will generate JSON schemas from the EVM RPC dataset definitions and copy them to `docs/dataset-def-schemas/EvmRpc.json`.

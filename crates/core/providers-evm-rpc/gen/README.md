# JSON Schema Generation

A generation crate for EVM RPC provider configuration JSON schemas. This crate generates JSON schemas for EVM RPC provider configurations using the schemars library for external validation and documentation purposes.

## JSON Schema Generation

The library uses a build configuration flag `gen_schema_provider` that enables JSON schema generation during the build process. When enabled, the build script will generate JSON schemas from Rust structs using schemars for EVM RPC provider configuration validation.

To generate JSON schema bindings, run:

```bash
just gen-evm-rpc-provider-schema
```

Or using the full `cargo` command:

```bash
RUSTFLAGS="--cfg gen_schema_provider" cargo check -p amp-providers-evm-rpc-gen

mkdir -p docs/providers
cp target/debug/build/amp-providers-evm-rpc-gen-*/out/schema.json docs/providers/evm-rpc.spec.json
```

This will generate JSON schemas from the EVM RPC provider configurations and copy them to `docs/providers/evm-rpc.spec.json`.

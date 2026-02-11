# JSON Schema Generation

A generation crate for Solana provider configuration JSON schemas. This crate generates JSON schemas for Solana provider configurations using the schemars library for external validation and documentation purposes.

## JSON Schema Generation

The library uses a build configuration flag `gen_schema_provider` that enables JSON schema generation during the build process. When enabled, the build script will generate JSON schemas from Rust structs using schemars for Solana provider configuration validation.

To generate JSON schema bindings, run:

```bash
just gen-solana-provider-schema
```

Or using the full `cargo` command:

```bash
RUSTFLAGS="--cfg gen_schema_provider" cargo check -p amp-providers-solana-gen

mkdir -p docs/providers
cp target/debug/build/amp-providers-solana-gen-*/out/schema.json docs/providers/solana.spec.json
```

This will generate JSON schemas from the Solana provider configurations and copy them to `docs/providers/solana.spec.json`.

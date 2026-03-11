# JSON Schema Generation

A generation crate for static provider configuration JSON schemas. This crate generates JSON schemas for static provider configurations using the schemars library for external validation and documentation purposes.

## JSON Schema Generation

The library uses a build configuration flag `gen_schema_provider` that enables JSON schema generation during the build process. When enabled, the build script will generate JSON schemas from Rust structs using schemars for static provider configuration validation.

To generate JSON schema bindings, run:

```bash
just gen-static-provider-schema
```

Or using the full `cargo` command:

```bash
RUSTFLAGS="--cfg gen_schema_provider" cargo check -p amp-providers-static-gen

mkdir -p docs/providers
cp target/debug/build/amp-providers-static-gen-*/out/schema.json docs/schemas/providers/static.spec.json
```

This will generate JSON schemas from the static provider configurations and copy them to `docs/schemas/providers/static.spec.json`.

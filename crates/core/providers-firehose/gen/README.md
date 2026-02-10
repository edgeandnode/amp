# JSON Schema Generation

A generation crate for Firehose provider configuration JSON schemas. This crate generates JSON schemas for Firehose provider configurations using the schemars library for external validation and documentation purposes.

## JSON Schema Generation

The library uses a build configuration flag `gen_schema_provider` that enables JSON schema generation during the build process. When enabled, the build script will generate JSON schemas from Rust structs using schemars for Firehose provider configuration validation.

To generate JSON schema bindings, run:

```bash
just gen-firehose-provider-schema
```

Or using the full `cargo` command:

```bash
RUSTFLAGS="--cfg gen_schema_provider" cargo check -p amp-providers-firehose-gen

mkdir -p docs/providers
cp target/debug/build/amp-providers-firehose-gen-*/out/schema.json docs/providers/firehose.spec.json
```

This will generate JSON schemas from the Firehose provider configurations and copy them to `docs/providers/firehose.spec.json`.

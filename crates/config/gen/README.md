# JSON Schema Generation

A generation crate for ampd configuration JSON schemas. This crate generates a JSON schema for the ampd configuration file (`config.toml`) using the schemars library for external validation and documentation purposes.

## JSON Schema Generation

The library uses a build configuration flag `gen_schema_config` that enables JSON schema generation during the build process. When enabled, the build script will generate a JSON schema from Rust structs using schemars for ampd configuration validation.

To generate the JSON schema, run:

```bash
just gen-config-schema
```

Or using the full `cargo` command:

```bash
RUSTFLAGS="--cfg gen_schema_config" cargo check -p amp-config-gen

mkdir -p docs/config
cp target/debug/build/amp-config-gen-*/out/schema.json docs/config/ampd.spec.json
```

This will generate a JSON schema from the ampd configuration types and copy it to `docs/config/ampd.spec.json`.

# OpenAPI Schema Generation

A generation crate for admin API OpenAPI specification. This crate generates OpenAPI schemas for the admin API endpoints using the utoipa library for external documentation and API client generation purposes.

## OpenAPI Schema Generation

The library uses a build configuration flag `gen_openapi_spec` that enables OpenAPI schema generation during the build process. When enabled, the build script will generate OpenAPI schemas from Rust structs and endpoint definitions using utoipa for admin API documentation.

To generate OpenAPI schema bindings, run:

```bash
just gen-admin-api-openapi-spec
```

Or using the full `cargo build` command:

```bash
RUSTFLAGS="--cfg gen_openapi_spec" cargo build -p admin-api-gen

mkdir -p docs/openapi-specs
cp target/debug/build/admin-api-gen-*/out/openapi.spec.json docs/openapi-specs/admin.spec.json
```

This will generate OpenAPI schemas from the admin API endpoint definitions and copy them to `docs/openapi-specs/admin.spec.json`.

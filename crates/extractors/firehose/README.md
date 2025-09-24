# Dataset Configuration

A Firehose base dataset is configured through a dataset definition file and a provider file with the
connection details. The [example_config](example_config) documents the necessary fields.

## Protobuf Code Generation

The library uses a build configuration flag `gen_proto` that enables protobuf code generation during the build process.
When enabled, the build script will generate Rust bindings from `.proto` files using prost and tonic
for Firehose protocol support.

To generate protobuf bindings, run:

```bash
just gen-firehose-datasets-proto
```

Or using the full `cargo build` command:

```bash
RUSTFLAGS="--cfg gen_proto" cargo build -p firehose-datasets
```

This will generate Rust structs and gRPC client code from the Firehose protocol definitions
and save them to `src/proto/`.

## JSON Schema Generation

JSON schemas for Firehose dataset definitions can be generated using the companion `datasets-firehose-gen` crate. This generates schemas for external validation and documentation purposes.

To generate JSON schema bindings, run:

```bash
just gen-datasets-firehose-manifest-schema
```

This will generate JSON schemas from the Firehose dataset definition structs and copy them to `docs/dataset-def-schemas/firehose.spec.json`.
# Dataset Configuration

A Firehose base dataset is configured through a dataset definition file and a provider file with the
connection details. The [example_config](example_config) documents the necessary fields.

## Protobuf Code Generation

The library includes a build feature `gen-proto` that enables protobuf code generation during the build process.
When enabled, the build script will generate Rust bindings from `.proto` files using prost and tonic
for Firehose protocol support.

To generate protobuf bindings, run:

```bash
just gen-firehose-datasets-proto
```

Or using the full `cargo build` command:

```bash
cargo build -p firehose-datasets --features=gen-proto
```

This will generate Rust structs and gRPC client code from the Firehose protocol definitions
and save them to `src/proto/`.
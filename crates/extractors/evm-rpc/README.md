# Dataset Configuration

An EVM RPC base dataset is configured through a dataset definition file and a provider file with the
connection details. The [example_config](example_config) documents the necessary fields.

## JSON Schema Generation

JSON schemas for EVM RPC dataset definitions can be generated using the companion `evm-rpc-gen` crate. This generates schemas for external validation and documentation purposes.

To generate JSON schema bindings, run:

```bash
just gen-evm-rpc-dataset-def-schema
```

This will generate JSON schemas from the EVM RPC dataset definition structs and copy them to `docs/dataset-def-schemas/EvmRpc.json`.

This directory contains [JSON schemas](https://json-schema.org/) corresponding to
our dataset definitions. You can use these to learn what the format for dataset
definitions looks like, or with automated tooling to check your dataset definition
files automatically in your editor.

The `kind` field determines what dataset type schemas are supported:

- for raw datasets (`evm-rpc`, `firehose`, `solana`), see [raw.spec.json](./raw.spec.json)
- for derived (`manifest` kind) datasets, see [derived.spec.json](./derived.spec.json)

Raw datasets extract blockchain data directly, while derived datasets execute
user-defined SQL queries against existing datasets' tables.

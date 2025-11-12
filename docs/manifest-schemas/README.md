This directory contains [JSON schemas](https://json-schema.org/) corresponding to
our dataset definitions. You can use these to learn what the format for dataset
definitions looks like, or with automated tooling to check your dataset definition
files automatically in your editor.

The `kind` field determines what dataset type schemas are supported:

- for `eth-beacon` kind datasets, see [eth-beacon.spec.json](./eth-beacon.spec.json)
- for `evm-rpc` kind datasets, see [evm-rpc.spec.json](./evm-rpc.spec.json)
- for `firehose` kind datasets, see [firehose.spec.json](./firehose.spec.json)
- for `manifest` kind datasets, see [derived.spec.json](./derived.spec.json)

The `manifest` datasets are also referred to as "derived datasets", whereas the other
types are also referred to as "raw datasets". Unlike raw datasets, which extract
blockchain data directly, derived datasets execute user-defined SQL queries against
existing datasets' tables.

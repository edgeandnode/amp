This directory contains [JSON schemas](https://json-schema.org/) corresponding to
our dataset definitions. You can use these to learn what the format for dataset
definitions looks like, or with automated tooling to check your dataset definition
files automatically in your editor.

There are a few fields that are common to all dataset definitions. These are defined
in [common.spec.json](./common.spec.json). The `kind` field determines what other fields might
be supported:
- for `evm-rpc` datasets, see [evm-rpc.spec.json](./evm-rpc.spec.json)
- for `firehose` datasets, see [firehose.spec.json](./firehose.spec.json)
- for `manifest` datasets, see [derived.spec.json](./derived.spec.json)
- for `substreams` datasets, see [substreams.spec.json](./substreams.spec.json)

The `manifest` datasets are also referred to as "user datasets", whereas the other
types are also referred to as "raw datasets". Unlike raw datasets, which extract
blockchain data directly, user datasets execute user-defined SQL queries against
existing datasets' tables to create derived or transformed datasets.

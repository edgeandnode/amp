This directory contains [JSON schemas](https://json-schema.org/) corresponding to
our dataset definitions. You can use these to learn what the format for dataset
definitions looks like, or with automated tooling to check your dataset definition
files automatically in your editor.

There are a few fields that are common to all dataset definitions. These are defined
in [Common.json](./Common.json). The `kind` field determines what other fields might
be supported:
- for `evm-rpc` datasets, see [EvmRpc.json](./EvmRpc.json)
- for `firehose` datasets, see [Firehose.json](./Firehose.json)
- for `manifest` datasets, see [Manifest.json](./Manifest.json)
- for `substreams` datasets, see [Substreams.json](./Substreams.json)

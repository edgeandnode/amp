# Dump

A CLI to dump extractor interfaces to parquet files. Currently supports dumping EVM Firehose to a simplified schema and Substreams with schema inferred from the manifest.

First, you will need to setup your config file and point `AMP_CONFIG` to it. Please refer to [config.md](../../../docs/config.md) and [the sample config](../../../config.sample.toml) for that initial setup. Then also see the definition format for the desired dataset, such as the currently supported [firehose](../../extractors/firehose/example_config/) or [substreams](../../extractors/substreams/example_config/) datasets.

Once you have a config with a dataset definition directory setup, dump becomes very easy to use, as you can simply refer to the dataset by name. An example usage to dump first four million blocks of a dataset named `eth_firehose`, running two parallel jobs:

```
cargo run --release dump -- --dataset eth_firehose -e 4000000 -j 2
```

You will now be able to find your tables under `<dataset>/<table>/` in your data directory. Each
table may contain multiple files, named by their start block, corresponding to table partitions. If
the process is interrupted, it is safe to resume by running the same command again.

Check the `--help` text for more configuration options.

### System requirements

Dump needs some RAM, because the contents of each parquet row group, which usually corresponds to a file, needs to be entirely buffered in memory before being written. This memory requirement varies linearely with the number of jobs (`DUMP_N_JOBS`), the partition size (`DUMP_PARTITION_SIZE_MB`) and the number of tables being written to. As a reference point, at 50 workers and a 4 GB partition size, memory usage was measured to peak at about 10 GB for the EVM dataset with 4 tables.

## Environment variables

All configuration can be set through env vars instead of the CLI.

- **AMP_CONFIG**
  - Description: Sets the path to a config file.
  - Example: `AMP_CONFIG=/path/to/config.toml`

- **DUMP_DATASET**
  - Description: The name of the dataset to dump. This is the name of the dataset definition `.toml`,
    without the extension. So if you have an `eth_firehose.toml`, you can set
    `DUMP_DATASET=eth_firehose`. Also accepts a comma-separated list of datasets, which will be
    dumped in the provided order.

### Optional

- **DUMP_END_BLOCK**
  - Description: Specifies the block number to end at, inclusive.
  - Default: For Firehose datasets, 100 blocks behind chain head. For Substreams, the latest final
    block. For SQL datasets, the latest block that has been scanned by all dependencies.
  - Example: `DUMP_END_BLOCK=10000000`

- **DUMP_N_JOBS**
  - Description: Determines the number of parallel firehoses and extractor jobs to run. Not supported
    for SQL datasets.
  - Default: `DUMP_N_JOBS=1`

- **DUMP_PARTITION_SIZE_MB**
  - Description: Specifies the size of each partition in megabytes, which dictates when new files are
    created. Has no effect for SQL datasets.
  - Default: `DUMP_PARTITION_SIZE_MB=1024`

- **DUMP_DISABLE_COMPRESSION**
  - Description: Controls whether compression is disabled when writing Parquet files.
  - Default: `DUMP_DISABLE_COMPRESSION=false`

- **DUMP_RUN_EVERY_MINS**
  - Description: If set, the dump command will be re-run periodically. Value in minutes.
    Default: Runs once and exits.
# Dump

A CLI to dump extractor interfaces to parquet files. Currently supports dumping EVM Firehose to a simplified schema.
Example usage to dump first one million blocks:
```
cargo run --release -p dump -- -o local/firehose_files 0 1000000
```

This will create one directory per table, with one file for the whole range. If you want to split the
range into separate files, this is not supported yet. But you can invoke repeatedly with
non-overlapping ranges. There is an `--n-jobs` option, if you set `--n-jobs 5` that will both use 5
concurrent Firehose connections and split the range into five chunks.

## Config

A config file toml is required. The path to the file can be configured in the CLI with `--config` or
in the environment with `DUMP_FIREHOSE_PROVIDER`. The current config format is:
```
url = "<FIREHOSE URL>"
token = "<AUTH_TOKEN>"
```

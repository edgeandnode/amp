# Dump

A CLI to dump extractor interfaces to parquet files. Currently supports dumping EVM Firehose to a simplified schema.
Example usage to dump first one million blocks:
```
cargo run --release -p dump -- --to local/firehose_files 1000000
```

This will create one directory per table. Each directory may contain multiple files, named by their
start block, corresponding to table partitions. If the process is interrupted, it is safe to resume
by running the command again.

Check the `--help` text for more configuration options.

## Config

A config file toml is required. The path to the file can be configured in the CLI with `--config` or
in the environment with `DUMP_FIREHOSE_PROVIDER`. The current config format is:
```
url = "<FIREHOSE URL>"
token = "<AUTH_TOKEN>"
```

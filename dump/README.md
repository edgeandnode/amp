# Dump

A CLI to dump extractor interfaces to parquet files. Currently supports dumping EVM Firehose to a simplified schema.
Example usage to dump first one million blocks:
```
cargo run --release -p dump -- --to local/firehose_files -e 1000000
```

This will create one directory per table. Each directory may contain multiple files, named by their
start block, corresponding to table partitions. If the process is interrupted, it is safe to resume
by running the command again.

Check the `--help` text for more configuration options.

To get logs, set `RUST_LOG=info`.

## Config

A config file toml is required. The path to the file can be configured in the CLI with `--config` or
in the environment with `FIREHOSE_PROVIDER`. The current config format is:
```
url = "<FIREHOSE URL>"
token = "<AUTH_TOKEN>"
```

## Environment variables

All configuration can be set through env vars instead of the CLI.

- **FIREHOSE_PROVIDER**
  - Description: Sets the path to a provider config file.
  - Example: `FIREHOSE_PROVIDER=/path/to/config.toml`

- **DUMP_END_BLOCK**
  - Description: Specifies the block number to end at, inclusive.
  - Example: `DUMP_END_BLOCK=10000000`

- **DUMP_TO**
  - Description: Defines the output location and path.
  - Examples:
    - Local storage: `DUMP_TO=/data/output`
    - Google Cloud Storage: `DUMP_TO=gs://my-bucket`

#### Google Cloud Storage
- **GOOGLE_SERVICE_ACCOUNT_PATH**
  - Description: Path to the Google Cloud service account file.
  - Example: `GOOGLE_SERVICE_ACCOUNT_PATH=/path/to/service-account.json`

- **GOOGLE_SERVICE_ACCOUNT_KEY**
  - Description: JSON serialized Google service account key.
  - Example: `GOOGLE_SERVICE_ACCOUNT_KEY='{"type": "service_account", ...}'`

### Optional

- **DUMP_START_BLOCK**
  - Description: The block number to start from, inclusive.
  - Default: `DUMP_START_BLOCK=0`

- **DUMP_N_JOBS**
  - Description: Determines the number of parallel firehoses and extractor jobs to run.
  - Default: `DUMP_N_JOBS=1`

- **DUMP_PARTITION_SIZE_MB**
  - Description: Specifies the size of each partition in megabytes.
  - Usage: Set the partition size, which dictates when new files are created.
  - Default: `DUMP_PARTITION_SIZE_MB=1024`

- **DUMP_DISABLE_COMPRESSION**
  - Description: Controls whether compression is disabled when writing Parquet files.
  - Default: `DUMP_DISABLE_COMPRESSION=false`

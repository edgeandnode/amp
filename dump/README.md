# Dump

A CLI to dump extractor interfaces to parquet files. Currently supports dumping EVM Firehose to a simplified schema and Substreams with schema inferred from the manifest.

## Firehose

Example usage to dump first four million blocks to local disk, running two parallel jobs:
```
cargo run --release -p dump -- --to local/firehose_files -e 4000000 -j 2
```

This will create one directory per table. Each directory may contain multiple files, named by their
start block, corresponding to table partitions. If the process is interrupted, it is safe to resume
by running the same command again.

Check the `--help` text for more configuration options.

To get logs, set `RUST_LOG=info`.

## Substreams

To dump substreams module output you will need a substreams module that contains repeated messages in the output, which is a common substreams pattern.
For example, a module with the following output:
```proto
message Events {
  repeated Transfer transfers = 1;
  repeated Mint mints = 2;
  repeated Burn burns = 3;
}
```
will produce `transfers`, `mints`, and `burns` parquet files, where each column matches the field of the corresponding event message type. All non-repeated fields in the module output are dropped from the schema.

To provide substreams manifest and output module use `--manifest` and `--module` cli arguments or environment variables (see below).

An example that dumps all UniswapV3 smart contract events from a specified block range:
```bash
cargo run --release -p dump -- -s=18000000 -e=18001000 --to=local/uniswap --manifest=https://spkg.io/streamingfast/uniswap-v3-v0.2.8.spkg --module=map_extract_data_types
```


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
    - Amazon S3 Storage: `DUMP_TO=s3://my-bucket`

#### Google Cloud Storage
- **GOOGLE_SERVICE_ACCOUNT_PATH**
  - Description: Path to the Google Cloud service account file.
  - Example: `GOOGLE_SERVICE_ACCOUNT_PATH=/path/to/service-account.json`

- **GOOGLE_SERVICE_ACCOUNT_KEY**
  - Description: JSON serialized Google service account key.
  - Example: `GOOGLE_SERVICE_ACCOUNT_KEY='{"type": "service_account", ...}'`

#### Amazon S3 Cloud Storage
- **AWS_ACCESS_KEY_ID**
  - Description: S3 access key ID.
  - Example: `AWS_ACCESS_KEY_ID=MY_KEY_ID`

- **AWS_SECRET_ACCESS_KEY**
  - Description: S3 secret access key.
  - Example: `AWS_SECRET_ACCESS_KEY=MY_SECRET_KEY`

- **AWS_ENDPOINT**
  - Description: S3 compatible endpoint. If unset, uses Amazon S3 service. If set, uses specified S3 compatible object service.
  - Example: `AWS_ENDPOINT=http://s3.mycompany.com:8333`

- **AWS_DEFAULT_REGION**
  - Description: Amazon S3 region.
  - Default: `AWS_DEFAULT_REGION=us-east-1`

- **AWS_ALLOW_HTTP**
  - Description: Allow non-TLS connections.
  - Default: `AWS_ALLOW_HTTP=false`

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

- **DUMP_SUBSTREAMS_MANIFEST**
  - Description: URL of the substreams SPKG manifest.
  - Example: `DUMP_SUBSTREAMS_MANIFEST=https://spkg.io/streamingfast/uniswap-v3-v0.2.8.spkg`

- **DUMP_SUBSTREAMS_MODULE**
  - Description: substreams output module to use.
  - Example: `DUMP_SUBSTREAMS_MODULE=map_extract_data_types`

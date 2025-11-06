# Configuration

The main configuration, used for both writing and serving datasets, is specified in a toml file which
is exemplified and documented in the [sample config](config.sample.toml). Its path should be passed
in as the `AMP_CONFIG` environment variable.

Configuring datasets to be extracted and served requires three different object storage directories:

- `manifests_dir`: Contains the dataset definitions. This is the input to the extraction process.
- `providers_dir`: Auxiliary to the dataset definitions, configures providers for external services
  like Firehose.
- `data_dir`: Where the actual dataset parquet tables are stored once extracted. Can be initially empty.

Although the initial setup with three directories may seem cumbersome, it allows for a highly
flexible configuration.

# Using env vars

Note that the values in the `AMP_CONFIG` file can be overridden from the environment, by prefixing
the env var name with `AMP_CONFIG_`. For example, to override the `data_dir` value, you can set a
`AMP_CONFIG_DATA_DIR` env var to the desired path.

For nested configuration values, use double underscores (`__`) to represent the nesting hierarchy. For example:
- To override `metadata_db.url`, set `AMP_CONFIG_METADATA_DB__URL`
- To override `metadata_db.pool_size`, set `AMP_CONFIG_METADATA_DB__POOL_SIZE`
- To override `writer.compression`, set `AMP_CONFIG_WRITER__COMPRESSION`

# Service addresses

The following optional configuration keys allow you to control the hostname and port that each service binds to:

- `flight_addr`: Arrow Flight RPC server address (default: `0.0.0.0:1602`)
- `jsonl_addr`: JSON Lines server address (default: `0.0.0.0:1603`)
- `admin_api_addr`: Admin API server address (default: `0.0.0.0:1610`)

## Logging

Simplified control of the logging verbosity level is offered by the `AMP_LOG` env var. It accepts
the values `error`, `warn`, `info`, `debug` or `trace`. The default value is `debug`. The standard
`RUST_LOG` env var can be used for finer-grained log filtering.

# Configuring object stores

All directory configurations (the `*_dir` keys) support both filesystem and object store locations.
So they can either be a filesystem path, for local storage, or a URL for object an store. For
production usage, an object store is recommended. Object store URLs can be in one of the following
formats:

### S3-compatible stores

URL format: `s3://<bucket>`

Sessions can be configured through the following environment variables:

- `AWS_ACCESS_KEY_ID`: access key ID
- `AWS_SECRET_ACCESS_KEY`: secret access key
- `AWS_DEFAULT_REGION`: AWS region
- `AWS_ENDPOINT`: endpoint
- `AWS_SESSION_TOKEN`: session token
- `AWS_ALLOW_HTTP`: allow non-TLS connections

### Google Cloud Storage (GCS)

URL format: `gs://<bucket>`

GCS Authorization can be configured through one of the following environment variables:

- `GOOGLE_SERVICE_ACCOUNT_PATH`: location of service account file, or
- `GOOGLE_SERVICE_ACCOUNT_KEY`: JSON serialized service account key, or
- Application Default Credentials.

## Datasets

All datasets have a name. This will be used as the dataset directory name under the data directory,
and also as the catalog schema name in the SQL interface. So if you have a dataset name `foobar`, it
will by default be placed under `<data_dir>/foobar/` and tables will be refered to in SQL as
`foobar.table`.

Conceptually there are raw datasets, which are extracted from external systems such as Firehose,
and then there are datasets defined as views on other datasets.

## Raw datasets

Details for the raw datasets currently implemented:

- EVM RPC [dataset docs](../crates/extractors/evm-rpc/README.md)
- Firehose [dataset docs](../crates/extractors/firehose/README.md)

### Generating Raw Dataset Manifests

The `ampctl gen-manifest` command provides a convenient way to generate manifest JSON files for raw datasets. These manifests define the schema and configuration that will be used during extraction.

#### Usage

```bash
# Generate manifest for EVM RPC dataset
ampctl gen-manifest --network mainnet --kind evm-rpc --name eth_mainnet

# Generate manifest for EVM RPC dataset with custom start block
ampctl gen-manifest --network mainnet --kind evm-rpc --name eth_mainnet --start-block 1000000

# Generate manifest for Firehose dataset
ampctl gen-manifest --network mainnet --kind firehose --name eth_firehose

# Output to file
ampctl gen-manifest --network mainnet --kind evm-rpc --name eth_mainnet -o ./manifests_dir/eth_mainnet.json

# Output to directory (will create ./manifests/evm-rpc.json)
ampctl gen-manifest --network mainnet --kind evm-rpc --name eth_mainnet -o ./manifests/

# Only include finalized blocks
ampctl gen-manifest --network mainnet --kind evm-rpc --name eth_mainnet --finalized-blocks-only
```

#### Parameters

- `--network`: Network name (e.g., mainnet, goerli, polygon, anvil)
- `--kind`: Dataset type (evm-rpc, firehose, eth-beacon)
- `--name`: Dataset name (must be a valid dataset identifier)
- `--out` (or `-o`): Optional output file or directory path. If a directory is specified, the file will be named `{kind}.json`. If not specified, the manifest is printed to stdout.
- `--start-block`: Starting block number for extraction (defaults to 0). Applies to evm-rpc, firehose, and eth-beacon datasets.
- `--finalized-blocks-only`: Only include finalized block data (flag, defaults to false)

The generated manifest includes the complete schema definition with all tables and columns for the specified dataset type and network.

# Providers

Providers are external data source configurations that raw datasets connect to for extracting blockchain data. Each provider is configured as a separate TOML file in the `providers_dir` directory.

Environment variable substitution is supported using `${VAR_NAME}` syntax for any field value.

## Provider Kinds

The `kind` field in a provider configuration must be one of the following:

- **`evm-rpc`**: Ethereum-compatible JSON-RPC endpoints (supports HTTP, WebSocket, and IPC connections)
- **`firehose`**: Firehose gRPC endpoints
- **`eth-beacon`**: Ethereum Beacon Chain (consensus layer) REST API endpoints

Each kind has its own set of required and optional configuration fields.

## Provider Configuration Structure

All provider configurations must define:
- `kind`: The provider type (must be one of the four kinds listed above)
- `network`: The blockchain network identifier (e.g., "mainnet", "goerli", "polygon")

Additional fields vary by provider kind. The provider name is derived from the filename (without the `.toml` extension), though it can be explicitly set with a `name` field.

## Sample Provider Configurations

Complete sample configuration files for each provider kind are available in the [docs/providers/](providers/) directory:

- **[evm-rpc.sample.toml](providers/evm-rpc.sample.toml)** - Configuration for Ethereum-compatible JSON-RPC endpoints. Includes fields for URL (HTTP/WebSocket/IPC), concurrent request limits, RPC batching, rate limiting, and receipt fetching options.

- **[firehose.sample.toml](providers/firehose.sample.toml)** - Configuration for StreamingFast Firehose gRPC endpoints. Includes fields for gRPC URL and authentication token.

- **[eth-beacon.sample.toml](providers/eth-beacon.sample.toml)** - Configuration for Ethereum Beacon Chain REST API endpoints. Includes fields for API URL, concurrent request limits, and rate limiting.

These sample files document all available configuration fields for each provider kind, including both required and optional parameters with their default values.

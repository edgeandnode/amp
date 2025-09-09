# Configuration

The main configuration, used for both writing and serving datasets, is specified in a toml file which
is exemplified and documented in the [sample config](config.sample.toml). Its path should be passed
in as the `NOZZLE_CONFIG` environment variable.

Configuring datasets to be extracted and served requires three different object storage directories:

- `dataset_defs_dir`: Contains the dataset definitions. This is the input to the extraction process.
- `providers_dir`: Auxiliary to the dataset definitions, configures providers for external services
  like Firehose and Substreams.
- `data_dir`: Where the actual dataset parquet tables are stored once extracted. Can be initially empty.

Although the initial setup with three directories may seem cumbersome, it allows for a highly
flexible configuration.

# Using env vars

Note that the values in the `NOZZLE_CONFIG` file can be overridden from the environment, by prefixing
the env var name with `NOZZLE_CONFIG_`. For example, to override the `data_dir` value, you can set a
`NOZZLE_CONFIG_DATA_DIR` env var to the desired path.

# Service addresses

The following optional configuration keys allow you to control the hostname and port that each service binds to:

- `flight_addr`: Arrow Flight RPC server address (default: `0.0.0.0:1602`)
- `jsonl_addr`: JSON Lines server address (default: `0.0.0.0:1603`)  
- `registry_service_addr`: Registry service address (default: `0.0.0.0:1611`)
- `admin_api_addr`: Admin API server address (default: `0.0.0.0:1610`)

## Logging

Simplified control of the logging verbosity level is offered by the `NOZZLE_LOG` env var. It accepts
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

Conceptually there are raw datasets, which are extracted from external systems such Firehose and
Substreams, and then there are datasets defined as views on other datasets.

## Raw datasets

Details for the raw datasets currently implemented:

- EVM RPC [dataset docs](../crates/extractors/evm-rpc/README.md)
- Firehose [dataset docs](../crates/extractors/firehose/README.md)
- Substreams [dataset docs](../crates/extractors/substreams/README.md)

## Datasets

A dataset can be defined as a set of queries on other datasets, with each query defining a view. Dataset views can be materialized, to share a result
among multiple queries or to do ahead of time work that is too slow or expensive to do at query time.
As an example, we will show how to define an `erc20_transfer` dataset, containing all erc20 transfer
events. The `erc20_transfer.toml` is straightforward:

```
kind = "sql"
name = "erc20_transfer"
```

The actual SQL queries should be placed beside the dataset definition, in a directory of the same
name. In this case we will only have one query for the transfers table, so the file `erc_20_transfer/transfers.sql` would contain the query:

```sql
SELECT 
    t.block_num,
    t.timestamp,
    t.event['from'] AS from,
    t.event['to'] AS to,
    t.event['value'] AS value
FROM (
    SELECT 
        l.block_num,
        l.timestamp,
        evm_decode_log(l.topic1, l.topic2, l.topic3, l.data, 'Transfer(address indexed from, address indexed to, uint256 value)') AS event
    FROM eth_firehose.logs l
    WHERE
        l.topic0 = evm_topic('Transfer(address indexed from, address indexed to, uint256 value)')
        and l.topic3 is null
) t;

```

The dataset is now ready to be dumped with `nozzle dump --dataset erc_20_transfer`, assuming the dependency
`eth_firehose` is already present, and then queried as in `select * from erc_20_transfer.transfers`.

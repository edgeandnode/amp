# Configuration

The main configuration, used for both writing and serving datasets, is specified in a toml file which
is exemplified and documented in the [sample config](config.sample.toml). Its path should be passed
in as the `NOZZLE_CONFIG` environment variable.

Configuring datasets to be extracted and served requires three different directories:
- `dataset_defs_dir`: Contains the dataset definitions. This is the input to the extraction process.
- `providers_dir`: Auxiliary to the dataset definitions, configures providers for external services
  like Firehose and Substreams.
- `data_dir`: Where the actual dataset parquet tables are stored once extracted. Can be initially empty.

Although the initial setup with three directories may seem cumbersome, it allows for a highly
flexible configuration.

Note that the values in the `NOZZLE_CONFIG` file can be overridden from the environment, by prefixing
the env var name with `NOZZLE_CONFIG_`. For example, to override the `data_dir` value, you can set a
`NOZZLE_CONFIG_DATA_DIR` env var to the desired path.

## Datasets
All datasets have a name. This will be used as the dataset directory name under the data directory,
and also as the catalog schema name in the SQL interface. So if you have a dataset name `foobar`, it
will by default be placed under `<data_dir>/foobar/` and tables will be refered to in SQL as
`foobar.table`.

Conceptually there are base datasets, which are extracted from external systems such Firehose and
Substreams, and then there are datasets defined as queries on other datasets.

Details for the base datasets currently implemented:
- EVM RPC [dataset docs](evm-rpc-datasets/README.md)
- Firehose [dataset docs](firehose-datasets/README.md)
- Substreams [dataset docs](substreams-datasets/README.md)

### SQL datasets

A dataset can be defined as a set of SQL queries on other datasets, with each query defining a table.
SQL datasets serve as materialized views, and can be used to share a result
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
select t.block_num,
    t.timestamp,
    t.event['from'] as from,
    t.event['to'] as to,
    t.event['value'] as value
    from (select l.block_num,
                l.timestamp,
                evm_decode(l.topic1, l.topic2, l.topic3, l.data, 'Transfer(address indexed from, address indexed to, uint256 value)') as event
            from eth_firehose.logs l
            where l.topic0 = evm_topic('Transfer(address indexed from, address indexed to, uint256 value)')) t
```
The dataset is now ready to be dumped with `dump --dataset erc_20_transfer`, assuming the dependency
`eth_firehose` is already present, and then queried as in `select * from erc_20_transfer.transfers`.

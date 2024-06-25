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

## Datasets
All datasets have a name. This will be used as the dataset directory name under the data directory,
and also as the catalog schema name in the SQL interface. So if you have a dataset name `foobar`, it
will by default be placed under `<data_dir>/foobar/` and tables will be refered to in SQL as
`foobar.table`.

Conceptually there are base datasets, which are extracted from external systems such Firehose and
Substreams, and then there are datasets defined as a transformation, or pipeline of transformations,
on top of one or more base dataset.

Currently, two kinds of base datasets are implemented, Firehose and Substreams. For details on those,
see the Firehose [dataset docs](firehose-datasets/configuration.md) and the Substreams [dataset
docs](substreams-datasets/configuring.md).
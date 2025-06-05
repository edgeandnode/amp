# Project Nozzle

_The data goes in the bucket_

**This repository is currently private. But it may eventually be open sourced, retaining commit history, issues and PRs so don't commit sensitive stuff.**

## What is this

An experiment in ETL architecture for data services on The Graph. 'Project Nozzle' is a codename.

### Python Quickstart

To quickly dive into the power of Nozzle with Python, best is to run the [`marimo_example_nb.ipynb](python/notebooks/marimo_example_nb.pyy). Follow the instructions in thy [Python README](python/README.md).

## Components

See [config.md](config.md) for how to configure nozzle.

### Prerequisites

If you want your `dump` command to persist and the data to be visible when you run the `server`, you
will need to run the metadata DB. You can do this with `docker-compose`:

```
docker-compose up -d
```

This will run the metadata DB at `postgresql://postgres:postgres@localhost:5432/nozzle`.
Update your config file to include it:
```toml
metadata_db_url = "postgresql://postgres:postgres@localhost:5432/nozzle"
```

### nozzle dump

Dump extractor interfaces to parquet files.

- Supports parallel and resumeable extraction.
- Supports dumping EVM Firehose, Substreams, and JSON-RPC to Parquet. EVM Firehose uses a simplified
  schema and Substreams uses a schema inferred from the manifest.

Once you have a config with a dataset definition directory setup, dump becomes very easy to use, as
you can simply refer to the dataset by name. An example usage to dump first four million blocks of a
dataset named `eth_firehose`, running two parallel jobs:

```
cargo run --release -p nozzle -- dump --dataset eth_firehose -e 4000000 -j 2
```

You will now be able to find your tables under `<dataset>/<table>/` in your data directory. Each
table may contain multiple files, named by their start block, corresponding to table partitions. If
the process is interrupted, it is safe to resume by running the same command again.

Check the `--help` text for more configuration options.

#### System requirements

Dump needs some RAM, because the contents of each parquet row group, which usually corresponds to a
file, needs to be entirely buffered in memory before being written. This memory requirement varies
linearely with the number of jobs (`DUMP_N_JOBS`), the partition size (`DUMP_PARTITION_SIZE_MB`) and
the number of tables being written to. As a reference point, at 50 workers and a 4 GB partition
size, memory usage was measured to peak at about 10 GB for the EVM dataset with 4 tables.

### nozzle server

To run, just `cargo run -p nozzle -- server`.

This starts both a JSON Lines over HTTP server and an Arrow Flight (gRPC) server.
The HTTP server is straightforward, send it a query and get results, one row per line:

```
curl -X POST http://localhost:1603 --data "select * from eth_rpc.logs limit 10"
```

The Arrow Flight server requires a specialized client. We currently have a Python client,
see docs for that [here](https://github.com/edgeandnode/project-nozzle/tree/main/python).

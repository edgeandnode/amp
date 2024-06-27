# Project Nozzle

_Probably Nothing_

__This repository is currently private. But it may eventually be open sourced, retaining commit history, issues and PRs so don't commit sensitive stuff.__

## What is this

An experiment in ETL architecture for data services in The Graph. 'Project Nozzle' is a codename.

## Components
See [config.md](config.md) for how to configure both the dump tool and the server.

### Dump
[Dump tool](dump/README.md), currently able to dump EVM Firehose & Substreams to Parquet. Supports parallel and resumeable extraction.

### Server
Arrow Flight server. To run, just `cargo run -p server`. See the Python [Client](python/client.py) and its [example](python/client_example.py) for how to send a SQL query over the wire.

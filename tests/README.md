# Tests

This crate aims to contain unit tests that are fast enough to be regularly run locally, that can be
run in parallel, and with input data that is small enough to be checked in directly in the Git repo.

## Dump tests
The dump tests are based on snapshot testing. A snapshot is commited into the repo, and the dump
tests will do a fresh snapshot to a temporary directory and compare the resutls. The datasets
currently are configured just like in the real dump tool, so real Firehose and JSON-RPC providers are
required.

To update the blessed snapshot for a dataset, run:
```
cargo run -p tests -- bless <dataset_name> <start_block> <end_block>`
```

## Debugging

To help debug a failing test, you can set `KEEP_TEMP_DIRS` in the env to be able to inspect the
temporary dump files and the temporary metadata DB.
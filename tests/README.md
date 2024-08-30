# Tests

This crate aims to contain unit tests that are fast enough to be regularly run locally, that can be
run in parallel, and with input data that is small enough to be checked in directly in the Git repo.

## Dump tests
The dump tests are essentially based on snapshot testing. A snapshot is commited into the repo, and
the dump tests will run `dump-check` against it. The datasets currently are configured just like in
the real dump, so an actual Firehose provider is required for Firehose tests. To update the snapshot,
run the test with the `NOZZLE_TESTS_BLESS` env var set.
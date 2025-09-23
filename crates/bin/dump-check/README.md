# Dump-check

A CLI tool to validate data extracted by Dump
```
cargo run --release -p dump-check -- --to local/firehose_files -e 1000000
```

# Config

Using the same config and parameters as **Dump** with the following addition:

- **DUMP_BATCH_SIZE**
  - Description: Buffer in blocks for validation against existing data.
  - Default: `DUMP_BATCH_SIZE=1000`
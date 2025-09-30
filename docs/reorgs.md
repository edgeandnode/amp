# Handling Blockchain Reorganization

## Background

Blockchain reorganizations, commonly referred to as "reorgs", are a fundamental aspect of blockchain consensus mechanisms where previously confirmed blocks are replaced by a new canonical chain. The reorg depth refers to how many blocks are replaced from the prior canonical chain.

```text
┌─────┬─────┬─────┐
│ 100 │ 101 │ 102 │
└─────┴─────┴─────┘
      ┌─────┬─────┬─────┐
      │ 101'│ 102'│ 103'│
      └─────┴─────┴─────┘

canonical chain: 100, 101', 102', 103'
orphaned blocks: 101, 102
reorg depth: 2
```

For each parquet file and streaming query microbatch, Nozzle tracks metadata for the block range the data is associated with, so that when a reorg occurs any data associated with orphaned blocks is invalidated.

## Client Side

### Arrow Flight Metadata

Arrow Flight clients receive metadata about the block range associated with each data batch via the `app_metadata` field in `FlightData` messages. This metadata is crucial for handling reors at the client level.

#### Metadata Format

The `app_metadata` field contains protobuf-serialized metadata with the following structure:
```proto3
syntax = "proto3";

message AppMetadata {
  message BlockRange {
    string network = 1;
    uint64 start = 2;
    uint64 end = 3;
    string hash = 4;
    optional string prev_hash = 5;
  }
  repeated BlockRange ranges = 1;
}
```
where:
- `numbers` is an inclusive range of block numbers.
- `hash` is the hash associated with the end block.
- `prev_hash` is the hash associated with the parent of the start block.

#### Usage

Clients should track block ranges from consecutive batches to handle reorgs. The basic logic is:
1. Store block ranges from `app_metadata` of the previously processed batch.
2. For each new batch, compare current ranges with previous ranges. If any network range in the current batch is not equal to the prior range and starts at or before a previous batch's end block, a reorg has occurred.
3. Invalidate prior batches associated with block ranges that overlap with the current batch start block number up to the latest block number processed.

For a reference implementation in Rust, see `nozzle_client::with_reorg` which automatically wraps query result streams to emit reorg events alongside data batches.

#### Resuming Streams

Nozzle supports resuming streaming queries by adding a `nozzle-resume` header to the `GetFlightInfo` request to the Nozzle server. The header value, "resume watermark" can be constructed from the `app_metadata` ranges of prior record batches. To avoid missing batches, construct the resume watermark from the ranges known to be fully processed.

The `nozzle-resume` header value is expected to be protobuf-serialized data with the following structure:

```proto3
syntax = "proto3";

message ResumeWatermarks {
  message ResumeWatermark {
    string network = 1;
    uint64 number = 2;
    string hash = 3;
  }
  repeated ResumeWatermark watermarks = 1;
}
```

The protobuf message is expected to have a block number & hash entry for each network present in the ranges metadata.

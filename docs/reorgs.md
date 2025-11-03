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

For each parquet file and streaming query microbatch, Amp tracks metadata for the block range the data is associated with, so that when a reorg occurs any data associated with orphaned blocks is invalidated.

## Client Side

### Arrow Flight Metadata

Arrow Flight clients receive metadata about the block range associated with each data batch via the `app_metadata` field in `FlightData` messages. This metadata is crucial for handling reorgs at the client level.

For streaming queries, all `RecordBatch` messages include `app_metadata` with block range information and a `ranges_complete` flag. When `ranges_complete` is `true`, it signals that the associated block ranges have been fully processed in the current microbatch, allowing clients to safely use this as a resumption watermark.

#### Metadata Format

The `app_metadata` field contains JSON-serialized metadata with the following structure:
```json
{
  "ranges": [
    {
      "network": "anvil",
      "numbers": { "start": 0, "end": 2 },
      "hash": "0x0deee2eaa7adb2b28c7fa731f79ea86e77e375f8ee0a0f2619ba6ec3eb2f68e6",
      "prev_hash": "0x0000000000000000000000000000000000000000000000000000000000000000"
    }
  ],
  "ranges_complete": false
}
```
where:
- `numbers` is an inclusive range of block numbers.
- `hash` is the hash associated with the end block.
- `prev_hash` is the hash associated with the parent of the start block.
- `ranges_complete` indicates whether this is the final record batch associated to the ranges.

#### Usage

Clients should track block ranges from consecutive batches to handle reorgs. The basic logic is:
1. Store block ranges from `app_metadata` of the previously processed batch.
2. For each new batch, compare current ranges with previous ranges. If any network range in the current batch is not equal to the prior range and starts at or before a previous batch's end block, a reorg has occurred.
3. Invalidate prior batches associated with block ranges that overlap with the current batch start block number up to the latest block number processed. If a start number from the incomming ranges lies in the middle of a previously processed range (`range.start < incomming.start < range.end`), then the client should invalidate all batches associated with the previously processed range and reconnect to the client using a `amp-resume` header (see [Resuming streams](#resuming-streams)) set to a watermark before the start of the incomming ranges such that all record batches after the watermark can be invalidated.

For a reference implementation in Rust, see `amp_client::AmpClient::stream` which automatically wraps query result streams to emit three types of events:
- `ProtocolMessage::Data` - Normal data batches with metadata
- `ProtocolMessage::Watermark` - Emitted when a batch with `ranges_complete: true` is received, indicating the stream has fully processed up to the associated block ranges. This watermark can be used to safely resume a stream when reconnecting.
- `ProtocolMessage::Reorg` - Reorg detection events with invalidation ranges

#### Resuming Streams

Amp supports resuming streaming queries by adding a `amp-resume` header to the `GetFlightInfo` request to the Amp server. The header value, "resume watermark" can be constructed from the `app_metadata` ranges of prior record batches. To avoid missing batches, construct the resume watermark from the ranges known to be fully processed.

The `amp-resume` header value is expected to be JSON-serialized data with the following structure:

```json
{
  "anvil": {
    "number": 2,
    "hash": "0x0deee2eaa7adb2b28c7fa731f79ea86e77e375f8ee0a0f2619ba6ec3eb2f68e6"
  }
}
```

The JSON value is expected to have a block number & hash entry for each network present in the ranges metadata.

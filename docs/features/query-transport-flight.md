---
name: "query-transport-flight"
description: "Arrow Flight RPC transport for high-performance SQL queries. Load when asking about Flight endpoint, port 1602, gRPC queries, or streaming queries"
components: "service:server"
---

# Arrow Flight Query Transport

## Summary

The Arrow Flight transport provides a high-performance gRPC interface for executing SQL queries. It supports both batch and streaming query modes, returning results in Apache Arrow columnar format. This is the recommended transport for production workloads, streaming queries, and applications that can consume Arrow data directly.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Architecture](#architecture)
3. [Usage](#usage)
4. [Implementation](#implementation)
5. [Limitations](#limitations)
6. [References](#references)

## Key Concepts

- **Arrow Flight**: Apache Arrow's RPC framework for high-performance data transfer over gRPC
- **FlightInfo**: Metadata response describing available data endpoints and schema
- **FlightData**: Streaming data messages containing Arrow RecordBatches
- **Ticket**: Token used to retrieve specific query results via `doGet`

## Architecture

### Request Flow

1. Client calls `getFlightInfo` with SQL query in `FlightDescriptor`
2. Server parses SQL, builds query plan, returns `FlightInfo` with schema and ticket
3. Client calls `doGet` with the ticket to retrieve results
4. Server executes query and streams `FlightData` messages
5. Each message contains Arrow RecordBatch with result rows

### Batch vs Streaming

| Mode | Activation | Behavior |
|------|------------|----------|
| Batch | Default (no SETTINGS) | Query runs once, returns complete results |
| Streaming | `SETTINGS stream = true` | Continuous execution, incremental results |

### Headers

| Header | Description |
|--------|-------------|
| `amp-stream` | Override streaming mode (`true` or `1`) |
| `amp-resume` | Resume streaming from watermark (JSON) |

### Streaming Metadata

For streaming queries, `FlightData.app_metadata` contains:

```json
{
  "ranges": [{"network": "eth", "numbers": {"start": 100, "end": 102}, "hash": "0x..."}],
  "ranges_complete": true
}
```

## Usage

### Python Client (pyarrow)

```python
import pyarrow.flight as flight

client = flight.connect("grpc://localhost:1602")

# Get flight info
info = client.get_flight_info(
    flight.FlightDescriptor.for_command(
        b"SELECT * FROM eth_rpc.blocks LIMIT 10"
    )
)

# Retrieve results
reader = client.do_get(info.endpoints[0].ticket)
table = reader.read_all()
print(table.to_pandas())
```

### Streaming Query (Python)

```python
import pyarrow.flight as flight

client = flight.connect("grpc://localhost:1602")

info = client.get_flight_info(
    flight.FlightDescriptor.for_command(
        b"SELECT * FROM eth_rpc.blocks SETTINGS stream = true"
    )
)

reader = client.do_get(info.endpoints[0].ticket)
for batch in reader:
    # Process each batch as it arrives
    print(f"Received {batch.data.num_rows} rows")
```

### Rust Client (arrow-flight)

```rust
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::FlightDescriptor;

let mut client = FlightServiceClient::connect("http://localhost:1602").await?;

let descriptor = FlightDescriptor::new_cmd(
    "SELECT * FROM eth_rpc.blocks LIMIT 10".as_bytes().to_vec()
);

let flight_info = client.get_flight_info(descriptor).await?;
let ticket = flight_info.endpoint[0].ticket.clone();
let stream = client.do_get(ticket).await?;
```

## Implementation

### Source Files

- `crates/services/server/src/flight.rs` - Flight server
- `crates/core/dump/src/streaming_query.rs` - Streaming queries

## Limitations

- **Arrow format only**: Results are always Apache Arrow RecordBatches
- **gRPC transport**: Requires gRPC client library
- **Schema required**: Client must handle Arrow schema for data interpretation

## References

- [app-ampd-server](app-ampd-server.md) - Configuration: Server endpoint settings
- [query-sql-batch](query-sql-batch.md) - Base: Batch query execution mode
- [query-sql-streaming](query-sql-streaming.md) - Base: Streaming query execution mode
- [query-transport-jsonl](query-transport-jsonl.md) - Alternative: Simple HTTP/JSON interface

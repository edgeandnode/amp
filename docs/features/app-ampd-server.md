---
name: "app-ampd-server"
description: "ampd query server endpoints and configuration. Load when asking about ampd server, query endpoints, or server ports"
components: "app:ampd,service:server,crate:config"
---

# ampd Query Server

## Summary

The ampd query server exposes SQL query capabilities through multiple transport endpoints. It provides an Arrow Flight endpoint for high-performance queries and streaming, plus a JSON Lines endpoint for simple HTTP access. This document covers server configuration and endpoint overview; see transport-specific docs for protocol details.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Architecture](#architecture)
3. [Configuration](#configuration)
4. [Usage](#usage)
5. [Implementation](#implementation)
6. [References](#references)

## Key Concepts

- **Query Server**: The ampd component that handles SQL query execution and serves results
- **Transport Endpoint**: A network interface exposing query capabilities (Flight or JSONL)
- **Streaming Support**: Real-time query execution available via Arrow Flight transport

## Architecture

### Available Endpoints

| Endpoint | Default Port | Protocol | Streaming |
|----------|--------------|----------|-----------|
| Arrow Flight | 1602 | gRPC | Yes |
| JSON Lines | 1603 | HTTP | No |

### Component Overview

```
ampd server
├── Arrow Flight (port 1602)
│   ├── Batch queries
│   └── Streaming queries
└── JSON Lines (port 1603)
    └── Batch queries only
```

## Configuration

### Transport Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `flight_addr` | `0.0.0.0:1602` | Arrow Flight server binding |
| `jsonl_addr` | `0.0.0.0:1603` | JSON Lines server binding |

### Streaming Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `server_microbatch_max_interval` | - | Maximum blocks per streaming microbatch |
| `keep_alive_interval` | - | Seconds between keep-alive messages (min 30s) |

### Config File

```toml
# .amp/config.toml
flight_addr = "0.0.0.0:1602"
jsonl_addr = "0.0.0.0:1603"
server_microbatch_max_interval = 100
keep_alive_interval = 60
```

### Environment Overrides

```bash
export AMP_CONFIG_FLIGHT_ADDR="0.0.0.0:1602"
export AMP_CONFIG_JSONL_ADDR="0.0.0.0:1603"
export AMP_CONFIG_SERVER_MICROBATCH_MAX_INTERVAL="100"
export AMP_CONFIG_KEEP_ALIVE_INTERVAL="60"
```

## Usage

### Starting the Server

```bash
# Start ampd with default server configuration
ampd

# The query server starts automatically with both endpoints:
# - Arrow Flight on port 1602
# - JSON Lines on port 1603
```

### Verifying Server Status

```bash
# Check if JSON Lines endpoint is responding
curl http://localhost:1603/health

# Check if Arrow Flight endpoint is responding (requires grpcurl)
grpcurl -plaintext localhost:1602 list
```

### Querying via JSON Lines

```bash
# Execute a simple query
curl -X POST http://localhost:1603/query/jsonl \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT 1 as value"}'
```

### Querying via Arrow Flight

See [query-transport-flight](query-transport-flight.md) for detailed Flight client usage.

## Implementation

### Source Files

- `crates/services/server/src/service.rs` - Service orchestration
- `crates/services/server/src/flight.rs` - Flight server
- `crates/services/server/src/jsonl.rs` - JSONL server
- `crates/services/server/src/config.rs` - Configuration

## References

- [app-ampd](app-ampd.md) - Base: ampd daemon overview
- [query-transport-flight](query-transport-flight.md) - Transport: Arrow Flight protocol details
- [query-transport-jsonl](query-transport-jsonl.md) - Transport: JSON Lines protocol details
- [query-sql-streaming](query-sql-streaming.md) - Related: Streaming query execution

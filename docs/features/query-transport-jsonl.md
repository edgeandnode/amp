---
name: "query-transport-jsonl"
description: "HTTP transport for SQL queries returning JSON Lines (NDJSON) responses. Load when asking about JSONL endpoint, port 1603, or HTTP query interface"
components: "service:server"
---

# JSON Lines Query Transport

## Summary

The JSON Lines (JSONL) transport provides a simple HTTP interface for executing SQL queries against blockchain datasets. It accepts SQL as plain text POST body and returns results as newline-delimited JSON (NDJSON). This is the simplest way to query data from Project Amp, ideal for ad-hoc queries and integration with tools that support HTTP.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Architecture](#architecture)
3. [Usage](#usage)
4. [API Reference](#api-reference)
5. [Implementation](#implementation)
6. [Limitations](#limitations)
7. [References](#references)

## Key Concepts

- **NDJSON Format**: Newline-Delimited JSON - each result row is a separate JSON object on its own line
- **Plain Text Input**: SQL queries sent as plain text in request body
- **Synchronous Response**: Results returned after full query execution

## Architecture

### Request Flow

1. Client sends HTTP POST with SQL query as plain text body
2. Server validates SQL string (non-empty, single statement)
3. SQL is parsed and checked for streaming settings (rejected if streaming)
4. Query is executed via DataFusion against the dataset catalog
5. Results are encoded as JSON Lines format
6. Response streamed back with `application/x-ndjson` content type

## Usage

### Basic Query

```bash
curl -X POST http://localhost:1603 --data "SELECT * FROM eth_rpc.blocks LIMIT 10"
```

### Query with Namespace

```bash
curl -X POST http://localhost:1603 --data 'SELECT * FROM "my_namespace/eth_rpc".blocks LIMIT 10'
```

### Query with UDFs

```bash
curl -X POST http://localhost:1603 --data "
SELECT
  evm_decode_hex(address) as contract,
  evm_decode_hex(topics[1]) as event_signature
FROM eth_rpc.logs
LIMIT 5
"
```

### Response Format

Success responses return NDJSON (one JSON object per line):

```
{"block_num": 0, "hash": "0x123..."}
{"block_num": 1, "hash": "0x456..."}
{"block_num": 2, "hash": "0x789..."}
```

### Error Responses

**Invalid SQL (400 Bad Request):**
```json
{"error_code": "INVALID_SQL_STRING", "error_message": "SQL string validation failed"}
```

**Parse Error (400 Bad Request):**
```json
{"error_code": "SQL_PARSE_ERROR", "error_message": "Syntax error in SQL statement"}
```

**Streaming Not Supported (400 Bad Request):**
```json
{"error_code": "STREAMING_NOT_SUPPORTED", "error_message": "Streaming queries (SETTINGS stream = true) are not supported on the JSONL endpoint. Please use the Arrow Flight endpoint instead."}
```

## API Reference

### POST /

Execute a SQL query.

- **Method**: `POST`
- **Path**: `/`
- **Content-Type**: `text/plain`
- **Body**: SQL query string
- **Response Content-Type**: `application/x-ndjson`
- **Response Body**: Newline-delimited JSON records

### GET /health

Health check endpoint.

- **Method**: `GET`
- **Path**: `/health`
- **Response**: `200 OK`

## Implementation

### Source Files

- `crates/services/server/src/jsonl.rs` - Server implementation
- `tests/src/testlib/fixtures/jsonl_client.rs` - Test client

## Limitations

- **Batch queries only**: Streaming queries (`SETTINGS stream = true`) are rejected. Use Arrow Flight for streaming.
- **Single statements only**: Multiple SQL statements per request are not supported.
- **JSON output only**: Results are always NDJSON format.

## References

- [app-ampd-server](app-ampd-server.md) - Dependency: Server endpoint settings
- [query-sql-batch](query-sql-batch.md) - Base: Batch query execution mode
- [query-transport-flight](query-transport-flight.md) - Alternative: Arrow Flight for streaming support

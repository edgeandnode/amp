# Ampsync

PostgreSQL synchronization tool for Amp datasets.

## Overview

Ampsync streams data from Amp datasets to PostgreSQL with exactly-once semantics, automatic crash recovery, and blockchain reorg handling. It uses amp-client's `TransactionalStream` for state management and crash safety.

## Configuration

Configuration can be provided via **CLI arguments** or **environment variables**. CLI arguments take precedence over environment variables.

### CLI Arguments

Run `ampsync --help` for full documentation.

```bash
ampsync sync [OPTIONS]

Options:
  -d, --dataset <DATASET>                      Dataset to sync (required)
  -t, --tables <TABLES>                        Tables to sync, comma-separated (required)
      --database-url <URL>                     PostgreSQL connection URL (required)
      --amp-flight-addr <ADDR>                 Amp Flight server (default: http://localhost:1602)
      --max-db-connections <N>                 Max connections (default: 10, range: 1-1000)
      --retention-blocks <N>                   Retention blocks (default: 128, min: 64)
      --auth-token <TOKEN>                     Authentication token for Arrow Flight
  -h, --help                                   Print help
  -V, --version                                Print version
```

### Environment Variables

All CLI arguments can also be set via environment variables:

- **`DATASET`** (required): Dataset reference to sync. Supports flexible formats:
  - Full: `namespace/name@revision` (e.g., `_/eth_rpc@1.0.0`)
  - No namespace: `name@revision` (e.g., `eth_rpc@1.0.0`, defaults to `_` namespace)
  - No revision: `namespace/name` (e.g., `_/eth_rpc`, defaults to `latest`)
  - Minimal: `name` (e.g., `eth_rpc`, defaults to `_/eth_rpc@latest`)

  Revision can be:
  - Semantic version (e.g., `1.0.0`)
  - Hash (64-character hex string)
  - `latest` (resolves to latest version)
  - `dev` (resolves to development version)

- **`TABLES`** (required): Comma-separated list of tables to sync (e.g., "logs,blocks,transactions")
- **`DATABASE_URL`** (required): PostgreSQL connection URL
  Format: `postgresql://[user]:[password]@[host]:[port]/[database]`
  Example: `postgresql://user:pass@localhost:5432/amp`
- **`AMP_FLIGHT_ADDR`** (default: `http://localhost:1602`): Amp Arrow Flight server
- **`MAX_DB_CONNECTIONS`** (default: `10`): Database connection pool size (valid range: 1-1000)
- **`RETENTION_BLOCKS`** (default: `128`): Watermark retention window (must be >= 64)
- **`AMP_AUTH_TOKEN`** (optional): Bearer token for authenticating requests to the Arrow Flight server

## Running

### Docker Compose Example

```yaml
services:
  postgres:
    image: postgres:17-alpine
    environment:
      POSTGRES_DB: amp
      POSTGRES_USER: amp
      POSTGRES_PASSWORD: amp
    ports:
      - "5432:5432"

  amp:
    image: ghcr.io/edgeandnode/amp:latest
    command: ["solo"]
    ports:
      - "1602:1602"  # Arrow Flight
      - "1610:1610"  # Admin API

  ampsync:
    image: ghcr.io/edgeandnode/ampsync:latest
    command: ["sync"]
    environment:
      DATASET: uniswap_v3
      TABLES: logs,blocks,transactions
      DATABASE_URL: postgresql://amp:amp@postgres:5432/amp
      AMP_FLIGHT_ADDR: http://amp:1602
      RUST_LOG: info,ampsync=debug
    depends_on:
      - postgres
      - amp
    restart: unless-stopped
```

## Database Schema

### System Columns

All tables automatically include these system columns:

- **`_tx_id` (INT64)**: Transaction ID from amp-client (part of composite PK)
- **`_row_index` (INT32)**: Row index within transaction (part of composite PK)
- **Primary Key**: `(_tx_id, _row_index)` for uniqueness and crash safety

### User Columns

User-defined columns from the dataset schema, automatically mapped from Arrow types to PostgreSQL types.

### Example Table

```sql
CREATE TABLE blocks (
    _tx_id      BIGINT  NOT NULL,
    _row_index  INT     NOT NULL,
    number      BIGINT  NOT NULL,
    hash        BYTEA   NOT NULL,
    timestamp   TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (_tx_id, _row_index)
);
```

## Crash Safety

Ampsync uses amp-client's `TransactionalStream` for crash-safe state management:

1. **State is persisted** in PostgreSQL (`amp_client_state` table)
2. **Automatic recovery**: On crash, uncommitted data is detected and deleted via Undo events
3. **No data loss**: Retry gets a fresh transaction ID with no conflicts

## Performance

- **Concurrent processing**: Each table runs in its own async task
- **PostgreSQL COPY protocol**: Binary format for high throughput
- **Connection pooling**: Shared connection pool across all tables
- **Automatic retries**: Exponential backoff for transient errors

### Tuning

For many tables, consider:
- Increasing `MAX_DB_CONNECTIONS` (default: 10, max: 1000)
  - Higher values don't always improve performance
  - Consider running multiple instances for horizontal scaling
- Running multiple ampsync instances for different datasets
- Monitoring PostgreSQL connection usage

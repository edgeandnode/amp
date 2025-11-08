# Ampsync

PostgreSQL synchronization tool for Amp datasets.

## Overview

Ampsync streams data from Amp datasets to PostgreSQL with exactly-once semantics, automatic crash recovery, and blockchain reorg handling. It uses amp-client's `TransactionalStream` for state management and crash safety.

## Configuration

Configuration can be provided via **CLI arguments** or **environment variables**. CLI arguments take precedence over environment variables.

### CLI Arguments

Run `ampsync --help` for full documentation.

```bash
ampsync [OPTIONS]

Options:
  -d, --dataset-name <NAME>                    Dataset to sync (required)
      --database-url <URL>                     PostgreSQL connection URL (required)
  -v, --dataset-version <VERSION>              Dataset version (default: "latest")
      --amp-flight-addr <ADDR>                 Amp Flight server (default: http://localhost:1602)
      --amp-admin-api-addr <ADDR>              Amp Admin API (default: http://localhost:1610)
      --max-db-connections <N>                 Max connections (default: 10, range: 1-1000)
      --retention-blocks <N>                   Retention blocks (default: 128, min: 64)
      --manifest-fetch-max-backoff-secs <N>    Max backoff for manifest retries (default: 60)
  -h, --help                                   Print help
  -V, --version                                Print version
```

### Environment Variables

All CLI arguments can also be set via environment variables:

- **`DATASET_NAME`** (required): Dataset to sync (e.g., "uniswap_v3")
- **`DATABASE_URL`** (required): PostgreSQL connection URL
  Format: `postgresql://[user]:[password]@[host]:[port]/[database]`
  Example: `postgresql://user:pass@localhost:5432/amp`
- **`DATASET_VERSION`** (default: `latest`): Pin to specific version
- **`AMP_FLIGHT_ADDR`** (default: `http://localhost:1602`): Amp Arrow Flight server
- **`AMP_ADMIN_API_ADDR`** (default: `http://localhost:1610`): Amp Admin API server
- **`MAX_DB_CONNECTIONS`** (default: `10`): Database connection pool size (valid range: 1-1000)
- **`RETENTION_BLOCKS`** (default: `128`): Watermark retention window (must be >= 64)
- **`MANIFEST_FETCH_MAX_BACKOFF_SECS`** (default: `60`): Maximum backoff duration in seconds for manifest fetch retries. Transient errors (network issues, HTTP status errors) will retry indefinitely with exponential backoff capped at this value. Fatal errors (validation failures) fail immediately.

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
    command: ["dev"]
    ports:
      - "1602:1602"  # Arrow Flight
      - "1610:1610"  # Admin API

  ampsync:
    image: ghcr.io/edgeandnode/ampsync:latest
    environment:
      DATASET_NAME: uniswap_v3
      DATABASE_URL: postgresql://amp:amp@postgres:5432/amp
      AMP_FLIGHT_ADDR: http://amp:1602
      AMP_ADMIN_API_ADDR: http://amp:1610
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

## License

See [LICENSE](../../../LICENSE) file for details.

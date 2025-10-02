# Ampsync

A synchronization service that streams dataset changes from a Nozzle server and syncs them to a PostgreSQL database.
Designed to run alongside your application in a Docker environment.

## Overview

Ampsync connects to a Nozzle server to stream dataset changes and synchronizes them to a local PostgreSQL database. This
enables applications to work with a subset of Nozzle data while maintaining real-time updates and handling blockchain
reorganizations.

## Configuration

The service is configured through environment variables:

### Required Environment Variables

- `DATASET_MANIFEST` - Path to your nozzle config file (e.g., `./nozzle.config.ts`)
    - Must be a valid file with extension: `.ts`, `.js`, `.mts`, `.mjs`, or `.json`
    - Can be a relative path (service runs in same repository as config)
- Database connection (one of the following):
    - `DATABASE_URL` - Full PostgreSQL connection string
    - OR individual components:
        - `DATABASE_USER` - Database username (required if not using DATABASE_URL)
        - `DATABASE_NAME` - Database name (required if not using DATABASE_URL)
        - `DATABASE_PASSWORD` - Database password (optional)
        - `DATABASE_HOST` - Database host (defaults to `localhost`)
        - `DATABASE_PORT` - Database port (defaults to `5432`)

### Optional Environment Variables

- `NOZZLE_ENDPOINT` - URL to Nozzle server (defaults to `http://localhost:1602`)

## Usage

### Docker Compose Example

```yaml
services:
  postgres:
    image: postgres:17
    environment:
      POSTGRES_DB: myapp
      POSTGRES_USER: myuser
      POSTGRES_PASSWORD: mypassword
    ports:
      - "5432:5432"

  ampsync:
    build:
      context: .
      dockerfile: Dockerfile
    image: ampsync:latest
    environment:
      DATASET_MANIFEST: /home/ampsync/nozzle.config.ts
      DATABASE_URL: postgresql://myuser:mypassword@postgres:5432/myapp
      NOZZLE_ENDPOINT: https://nozzle.example.com:1602
    volumes:
      - ./nozzle.config.ts:/home/ampsync/nozzle.config.ts:ro
    depends_on:
      - postgres
```

### Local Development

#### Running with Docker Compose

```bash
cd crates/bin/ampsync/examples/with-electricsql
docker compose up                   # Builds and starts all services
```

#### Running Directly with Cargo

```bash
# Set environment variables
export DATASET_MANIFEST=./nozzle.config.ts
export DATABASE_URL=postgresql://user:pass@localhost:5432/mydb
export NOZZLE_ENDPOINT=http://localhost:1602

# Run the service
cargo run --release -p ampsync
```

## Features

- **Real-time Streaming**: Continuously syncs dataset changes as they occur
- **Reorg Handling**: Automatically handles blockchain reorganizations
- **Automatic Schema Inference**: Queries Nozzle server to automatically infer table schemas
- **Developer-Friendly Config**: Simple nozzle.config format - just write SQL, schemas are inferred
- **SQL Validation**: Rejects non-incremental queries (ORDER BY not supported)
- **Connection Pooling**: Efficient database connection management
- **Error Recovery**: Robust error handling and retry logic
- **High Performance**: PostgreSQL COPY protocol with binary format for maximum throughput
- **Adaptive Batching**: Dynamic batch size optimization based on performance metrics

## Nozzle Config Format

Ampsync uses a simple, developer-friendly configuration format. You write just SQL queries - schemas are automatically inferred!

### Simple Example (TypeScript)

```typescript
import { defineDataset } from "nozzl"

export default defineDataset(() => ({
  name: "my-dataset",
  version: "0.1.0",
  network: "mainnet",
  dependencies: {
    anvil: {
      owner: "graphprotocol",
      name: "anvil",
      version: "0.1.0",
    },
  },
  tables: {
    blocks: {
      sql: "SELECT * FROM anvil.blocks",
    },
    transfers: {
      sql: `
        SELECT block_num, timestamp, event['from'] as from, event['to'] as to, event['value'] as value
        FROM (SELECT * FROM anvil.logs WHERE topic0 = evm_topic('Transfer(address,address,uint256)'))
      `,
    },
  },
  functions: {},
}))
```

### Key Points

- **Tables**: Just specify the `sql` query - schemas are automatically inferred from Nozzle
- **No Manual Schemas**: Don't write Arrow schemas or field types manually
- **SQL Validation**: Queries with ORDER BY are rejected (non-incremental queries not supported)
- **Dependencies**: Reference other datasets by owner/name/version
- **Supported Formats**: `.ts`, `.js`, `.mts`, `.mjs`, or `.json`

### How Schema Inference Works

1. Ampsync parses your nozzle.config file
2. For each table, it wraps your SQL in a subquery: `SELECT * FROM ({your_sql}) AS alias LIMIT 1`
3. Executes the query to get the schema (retrieves minimal data - just 1 row)
4. Extracts the Arrow schema from the query result
5. Creates PostgreSQL tables with the inferred schema
6. Starts streaming data using the original SQL

This means you don't need to maintain schemas in two places - they're derived from your SQL!

## Docker Architecture

### Multi-Stage Build

The Docker setup uses a multi-stage build for optimal image size and security:

1. **Builder Stage**: Compiles the Rust binary with all necessary build tools
2. **Runtime Stage**: Minimal Debian image with only runtime dependencies

### Benefits

- **Consistent Builds**: Same build environment regardless of host platform
- **Minimal Image Size**: Runtime image only contains binary and essential libraries
- **Clean Separation**: Build tools isolated from runtime
- **Security**: Runs as non-root user (uid 1001) with minimal attack surface
- **Cross-Platform**: Works on x86_64 and aarch64 (ARM64) architectures

### Files

- `Dockerfile` - Multi-stage build configuration
- `.dockerignore` - Excludes unnecessary files from Docker context


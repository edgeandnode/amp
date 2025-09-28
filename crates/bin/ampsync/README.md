# Ampsync

A synchronization service that streams dataset changes from a Nozzle server and syncs them to a PostgreSQL database. Designed to run alongside your application in a Docker environment.

## Overview

Ampsync connects to a Nozzle server to stream dataset changes and synchronizes them to a local PostgreSQL database. This enables applications to work with a subset of Nozzle data while maintaining real-time updates and handling blockchain reorganizations.

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
version: '3.8'
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: myapp
      POSTGRES_USER: myuser
      POSTGRES_PASSWORD: mypassword
    ports:
      - "5432:5432"

  ampsync:
    image: ampsync:latest
    environment:
      DATASET_MANIFEST: ./nozzle.config.ts
      DATABASE_URL: postgresql://myuser:mypassword@postgres:5432/myapp
      NOZZLE_ENDPOINT: https://nozzle.example.com:1602
    volumes:
      - ./nozzle.config.ts:/app/nozzle.config.ts
    depends_on:
      - postgres
```

### Local Development

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
- **Schema Inference**: Dynamically creates database tables based on Arrow schema
- **Connection Pooling**: Efficient database connection management
- **Error Recovery**: Robust error handling and retry logic


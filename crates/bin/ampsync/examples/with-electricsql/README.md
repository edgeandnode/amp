# Ampsync with ElectricSQL Example

This example demonstrates using `ampsync` to sync Nozzle datasets to PostgreSQL, with ElectricSQL providing real-time sync to a Next.js frontend.

## Architecture

```
Anvil (Local Blockchain)
    ↓
Amp (Nozzle Server) - indexes blockchain data
    ↓
Ampsync - syncs to PostgreSQL
    ↓
ElectricSQL - syncs to frontend
    ↓
Next.js App - displays real-time data
```

## Quick Start

### 1. Start infrastructure services

```bash
just up
```

This will:

- Build ampsync inside Docker (multi-stage build)
- Start all services:
  - PostgreSQL (ports 5432, 6434)
  - Amp/Nozzle server (ports 1602, 1603, 1610)
  - Anvil local blockchain (port 8545)
  - Ampsync sync service
  - ElectricSQL sync engine (port 3000)

### 2. Start development server

```bash
just dev
```

This runs in parallel:

- Next.js app (http://localhost:3001)
- Nozzle proxy
- Nozzle dev watcher

### 3. View the app

Open [http://localhost:3001](http://localhost:3001) to see real-time blockchain data.

## Development Workflow

### Modify the dataset configuration

Edit [nozzle.config.ts](./nozzle.config.ts) to change what data is synced. Ampsync will automatically detect changes and reload.

**Note** it is best when you make changes to the `nozzle.config.ts`, that you bump the version each time. This helps ampsync find the dataset schema and keeps each version <-> schema unique.

### View logs

```bash
# All services
just logs

# Specific service
just logs ampsync
just logs electric
```

### Rebuild ampsync after code changes

```bash
docker compose up -d --build ampsync
```

This will rebuild the Docker image from source and restart the service.

### Clean shutdown

```bash
just down
```

This removes all containers and volumes.

## Dataset Configuration

The example syncs Anvil blockchain data:

- `anvil_blocks` - Block headers
- `anvil_transactions` - Transaction data
- `anvil_logs` - Event logs

See `nozzle.config.ts` for the full configuration.

## Troubleshooting

### "Dataset not found in admin-api"

This is normal on first start. Wait for Amp to index some blocks from Anvil.

### Ampsync not detecting config changes

Check that the file watcher is working:

```bash
just logs ampsync | grep -i "hot-reload"
```

### Database connection errors

Ensure PostgreSQL is healthy:

```bash
docker compose ps db
```

## Tech Stack

- **Blockchain**: [Foundry Anvil](https://book.getfoundry.sh/anvil/)
- **Indexing**: [Nozzle/Amp](https://github.com/edgeandnode/nozzle)
- **Sync**: [Ampsync](../../)
- **Real-time Sync**: [ElectricSQL](https://electric-sql.com/)
- **Frontend**: [Next.js](https://nextjs.org/)

## Learn More

- [Ampsync Documentation](../../README.md)
- [Nozzle Documentation](../../../../README.md)
- [ElectricSQL Documentation](https://electric-sql.com/docs)

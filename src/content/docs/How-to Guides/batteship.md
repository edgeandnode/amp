---
title: How to Build and Run a Battleship App with Amp
description: tep-by-step guide to deploy and query a live blockchain app using Amp.
slug: build-battleship-with-amp
category: how-to-guide
---

This guide shows how to build and run a live blockchain application using Amp. You'll deploy a fully functional [Battleship game](https://github.com/edgeandnode/battleship?tab=readme-ov-file), and learn how Amp indexes smart contract events and serves them as queryable data without manual event listeners or indexing code required.

By the end, you’ll know how to:

- Configure Amp to extract and store contract events
- Query those events with SQL
- Run the system locally for development and testing

> Tip: The Battleship example is actively maintained to showcase the latest Amp features. Star the repo to track updates.

## Part 1. Concept: The Problem Amp Solves

Building blockchain apps means handling event data. Every contract every contract emits logs that must be parsed, stored, and queried.

Traditionally, you have to:

- Write listeners for each event
- Parse logs manually (ABIs, hex decoding, conversions)
- Maintain schemas, databases, and indexing infrastructure
- Handle reorgs, rate limits, and failures

This creates heavy, repetitive infrastructure work.

### Amp’s Solution

Amp replaces all of this with a configuration-driven model:

- Define a dataset once with your contract ABI
- Amp automatically extracts, decodes, and stores events
- Query them directly using SQL

Your app becomes query-driven, not event-driven.

## Part 2. Build: Configure Amp Infrastructure

Follow these steps to configure Amp for the Battleship game.

### Step 1. Define Your Dataset (amp.config.ts)

Tells Amp which contract events to index.

```ts
import { defineDataset, eventTables } from "@edgeandnode/amp";
import { abi } from "./app/src/lib/abi.ts";

export default defineDataset(() => ({
  name: "battleship",
  version: "0.1.0",
  network: "anvil",
  dependencies: {
    anvil: { owner: "graphprotocol", name: "anvil", version: "0.1.0" },
  },
  tables: eventTables(abi),
}));
```

Amp automatically:

- Creates a table for each event in the ABI
- Adds metadata (block number, timestamp, tx hash)
- Makes events queryable via SQL tables

Example query:

```sql
SELECT * FROM battleship.shot_fired WHERE game_id = 123;
```

> **Key insight**: You never write event listeners or database logic—Amp generates schema and indexing automatically.

### Step 2: Configure Core Infrastructure (`infra/amp/config.toml`)

Controls where Amp stores and tracks data.

```toml
data_dir = "data"
providers_dir = "providers"
dataset_defs_dir = "datasets"
metadata_db_url = "postgres://postgres:postgres@postgres:5432/amp?sslmode=disable"
```

Amp separates:

- Parquet files store raw event data
- PostgreSQL metadata DB tracks indexing progress

This allows automatic crash recovery and scalable storage.

**Why Parquet?**

- Compressed and efficient for analytics
- Supports fast column-based queries
- Enables skipping irrelevant files for faster filtering

### Step 3. Define the Blockchain Provider (`infra/amp/providers/anvil.toml`)

Specifies the blockchain data source.

```toml
kind = "evm-rpc"
url = "http://anvil:8545"
network = "anvil"
```

Providers make datasets portable. Use this for local testing, and switch to a production RPC by creating a `mainnet.toml`.

### Step 4. Run Amp Locally (`docker-compose.yaml`)

Start the full Amp stack in development.

```yaml
amp:
  image: ghcr.io/edgeandnode/amp:latest
  command: ["--config", "/var/lib/amp/config.toml", "dev"]
  depends_on: [postgres]
  ports:
    - 1610:1610 # Admin API
    - 1603:1603 # JSON Lines
    - 1602:1602 # Arrow Flight
```

In dev mode, Amp runs:

- Controller (schedules jobs)
- Worker (extracts events)
- Query server (serves SQL queries)

> **Scaling Tip**: In production, run these separately to scale horizontally.

## Part 3. Integrate: Use Amp in the Application Layer

### Step 5. Define SQL Queries `queries.ts`

Use SQL to join and aggregate event data instead of listeners:

```ts
const gamesListQuery = `
  SELECT gc.*, gs.*, ge.*
  FROM battleship.game_created gc
  LEFT JOIN battleship.game_started gs ON gc.game_id = gs.game_id
  LEFT JOIN battleship.game_ended ge ON gc.game_id = ge.game_id
  ORDER BY gs.block_num DESC
```

Amp texposes all events as relational tables, making it easy to join and filter data directly.

> **ELT advantage**: Data is extracted and loaded once. Transformations happen at query time. You can instantly add new derived views without re-indexing.

### Step 6. Connect to Amp (`runtime.ts` )

Establish Arrow Flight connection for high-speed binary queries.

```ts
import { createConnectTransport } from "@connectrpc/connect-web";
import { ArrowFlight } from "@edgeandnode/amp";
import { Atom } from "@effect-atom/atom-react";

const transport = createConnectTransport({ baseUrl: "/amp" });
export const runtime = Atom.runtime(ArrowFlight.layer(transport));
```

**Why Arrow Flight?**

- Binary columnar format (faster than JSON)
- BigInt-safe for EVM values
- Streams results efficiently

### Step 7: Create Reactive Queries with Atoms

Atoms make SQL queries reactive:

```ts
export const gamesListAtom = runtime
  .atom(queryGamesList())
  .pipe(Atom.withReactivity(["gamesList"]));
```

When Amp indexes new events, atoms update automatically—React components re-render in real time.

**Components**

- `GameList.tsx` subscribes to `gamesListAtom` to render all active games.
- `GameBoard.tsx` combines multiple reactive atoms (`gameDetailsAtom`, `shotsFiredAtom`, etc.) to display live gameplay.

> **Flow**: Blockchain --> Amp --> Atom → React --> Live UI.

## Part 4. Operate: Run and Verify Locally

### Step 8. Run Everything with `just`

Use a single command:

```bash
just dev
```

Starts:

- Frontend (Vite)
- Connect proxy (HTTP -> gRPC)
- Amp in dev mode

Other Commands:

- `just up` to start infra
- `just deploy` to deploy dataset
- `just logs` to view logs

### Step 9. Configure Frontend Proxy (vite.config.ts)

Routes frontend requests to Amp and Anvil:

```ts
proxy: {
  "/rpc": { target: "http://localhost:8545" },
  "/amp": { target: "http://localhost:8080" },
}
```

> Browsers can’t use gRPC directly, so Vite proxies through Connect RPC before reaching Amp’s query server.

## Part 5. Verify and Extend

### Check Indexing Progress

```bash
docker logs battleship-amp-1
```

**Query Directly**

```bash
pnpm amp query 'SELECT * FROM battleship.game_created LIMIT 5'
```

### Add New Events

1. Add event to your contract
2. Redeploy it
3. Update the ABI in `amp.config.ts`
4. Restart Amp

New event tables appear automatically.

## Part 6. Summary

| Concept              | Purpose                                    |
| -------------------- | ------------------------------------------ |
| **Amp**              | Converts blockchain events into SQL tables |
| **eventTables(abi)** | Generates schema from contract ABI         |
| **Arrow Flight**     | High-speed query protocol                  |
| **Atoms**            | Reactive data streams in React             |
| **just dev**         | Unified local dev command                  |

## Part 7. Next Steps

To adapt the Battleship template:

1. Replace the ABI with your contract’s ABI.
2. Update the provider configuration for your network.
3. Write new SQL queries.
4. Build reactive UI components with atoms.
5. See `docs/modes.md` for scaling Amp in production.

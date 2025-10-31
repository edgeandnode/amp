---
title: Operational Mode Reference
description: Learn a technical overview of Amp's Operational Modes
slug: operational-mode
category: reference
---

## Purpose

This describes Amp's operational modes, components, and deployment topologies. Use it to understand what commands and services are available, their configuration options, and how they interact.

## Overview

Amp provides several CLI commands that can be combined into different deployment modes depending on scale, environment, and operational requirements.

### Core Commands

| Command      | Description                                         |
| ------------ | --------------------------------------------------- |
| `dump`       | Extract dataset data to Parquet files (synchronous) |
| `server`     | Runs query servers (Arrow Flight, JSON Lines)       |
| `worker`     | Executes scheduled extraction jobs                  |
| `controller` | Hosts Admin API for job and dataset management      |
| `migrate`    | Applies database migrations to metadata DB          |

# Operational Modes

## Operational Modes

Amp supports three primary modes of operation:

| Mode                 | Description                                                                       | Typical Use                          |
| -------------------- | --------------------------------------------------------------------------------- | ------------------------------------ |
| **Serverless Mode**  | Ephemeral, on-demand extractions using `ampd dump`                                | CI/CD, cloud functions, one-off jobs |
| **Single-Node Mode** | Combined controller, server, and worker via `ampd dev`                            | Local testing and prototyping        |
| **Distributed Mode** | Independent controller, server, and worker processes coordinating via metadata DB | Production deployments               |

## 1. Serverless Mode (`ampd dump`)

Purpose: Run synchronous, ephemeral extractions of blockchain data.

### Key Characteristics

- **Command:** `ampd dump`
- **Execution:** Runs until completion, then exits
- **Persistence:** Tracks progress in metadata DB
- **Concurrency:** Supports multi-job parallel extraction (`--n-jobs`)
- **Resumable:** Continues from last extracted block

### Typical Use Cases

- Bootstrapping datasets
- One-off data extraction
- CI/CD or scheduled jobs
- Dataset configuration testing

### Basic Usage

```bash
# Extract a single dataset
`ampd dump --dataset eth_mainnet`

# Extract multiple datasets
`ampd dump --dataset eth_mainnet,uniswap_v3`

# Extract with parallel jobs up to block 4M
`ampd dump --dataset eth_mainnet --end-block 4000000 --n-jobs 4`

# From manifest file
`ampd dump --dataset ./datasets/production.json`
```

### Supported Dataset Types

#### EVM RPC (Ethereum-compatible endpoints)

- **Firehose**
- **Eth Beacon**
- **SQL-derived datasets**

---

## 2. Distributed Mode

**Purpose:** Enable production-scale, multi-node deployments with isolated components and horizontal scalability.

Distributed mode separates Amp into three core components: **server**, **worker**, and **controller**.

---

### 2.1 Server Component (`ampd server`)

Description: Runs Amp as a long-lived query service. Provides read-only interfaces for client queries.

| Interface    | Port | Format              | Notes                      |
| ------------ | ---- | ------------------- | -------------------------- |
| Arrow Flight | 1602 | Binary (Apache Row) | gRPC, high-performance     |
| JSON Lines   | 1603 | BNDJSON             | Simple HTTP POST interface |

#### **Example Usage**

```bash
# Start all query servers
`ampd server`

# Start only Arrow Flight
`ampd server --flight-server`

# Start only JSON Lines
`ampd server --jsonl-server`
```

#### Query Examples

**JSON Lines**

```bash
curl -X POST http://localhost:1603 \
  --data "SELECT * FROM eth_mainnet.blocks LIMIT 10"
```

**Arrow Flight (Python)**

```bash
from pyarrow import flight
client = flight.connect("grpc://localhost:1602")
reader = client.do_get(flight.Ticket("SELECT * FROM eth_mainnet.blocks LIMIT 10"))
print(reader.read_all().to_pandas())
```

> Note: Server flags are **explicit selectors**, not toggles.

- `ampd server` - enables both servers

- `ampd server --flight-server` - enables only Flight

- `ampd server --jsonl-server` - enables only JSON Lines

---

### 2.2 Worker Component (`ampd worker`)

Description: Executes extraction jobs scheduled by the controller, coordinating via metadata DB.

| Attribute          | Description              |
| ------------------ | ------------------------ |
| Communication      | PostgreSQL LISTEN/NOTIFY |
| Heartbeat          | Every 1 second           |
| Job Reconciliation | Every 60 seconds         |
| Interfaces         | None (internal-only)     |

#### Example Usage

```bash
ampd worker --node-id worker-01
ampd worker --node-id worker-02
```

#### Worker Lifecycle

```bash
START → Register → LISTEN → EXECUTE → UPDATE → SHUTDOWN
```

#### Coordination

- Job assignment via metadata DB
- Automatic failover on worker crash
- Load-balanced job distribution

---

### 2.3 Controller Component (`ampd controller`)

Description: Hosts the Admin API (port 1610) for managing datasets, jobs, and workers.

| API              | Method | Example                 |
| ---------------- | ------ | ----------------------- |
| List datasets    | GET    | `/datasets`             |
| Start dump job   | POST   | `/datasets/<name>/dump` |
| Check job status | GET    | `/jobs/<id>`            |

#### Example Usage

```
ampd controller
```

**Trigger a job**

```bash
curl -X POST http://localhost:1610/datasets/eth_mainnet/dump \
  -H "Content-Type: application/json" \
  -d '{"end_block": 20000000}'
```

> Development mode (`ampd dev`) automatically includes the Admin API; no separate controller needed.

---

## 3. Single-Node Mode (`ampd dev`)

Purpose: Simplified, all-in-one process for local testing and development.

| Attribute    | Description                   |
| ------------ | ----------------------------- |
| Command      | `ampd dev`                    |
| Component    | Server + Controller + Workers |
| Mode         | Single process                |
| Intended for | Development, CI/CD            |

#### Example Usage

```bash
ampd dev
```

#### Features

- Embedded worker
- Auto-start Admin API
- Simplified logging and lifecycle
- Local metadata tracking

#### Limitations

- No isolation or scaling
- Not production safe
- Shared resource contention

---

## **Deployment Patterns**

| Pattern                                            | Mode          | Use Case                  |
| -------------------------------------------------- | ------------- | ------------------------- |
| Single-Node (`ampd dev`)                           | Local testing | Development, CI/CD        |
| Query-Only (`ampd server`)                         | PDistributed  | Read-only deployments     |
| Full Distributed (`controller + server + workers`) | Production    | Scalable, fault-tolerant  |
| Multi-Region Distributed                           | Global        | Low-latency, cross-region |

Each pattern shares PostgreSQL (metadata) and object storage (Parquet files).

## Security Reference

| Component              | Port      | Security Level | Recommendations                  |
| ---------------------- | --------- | -------------- | -------------------------------- |
| Controller (Admin API) | 1610      | High           | Private network only, VPN access |
| Server (Query APIs)    | 1602/1603 | Medium         | Public read-only, rate limits    |
| Worker                 | -         | SLow           | Internal-only, no exposed ports  |
| Dev Mode               | multiple  | Critical       | Never use in production          |

### External Security Layers

- **API Gateway** (Auth, rate limiting)
- **mTLS** for Flight connections
- **VPN / Zero-Trust** for admin access
- **IAM roles / Secret managers** for credentials

---

## Scaling Progression

| Stage | Mode                        | Description                                |
| ----- | --------------------------- | ------------------------------------------ |
| 1     | Serverless / Single-Node    | Local testing and validation               |
| 2     | Distributed (Single Region) | Production-ready                           |
| 3     | Distributed (Scaled)        | Multiple workers and servers               |
| 4     | Multi-Region                | Global replication and low-latency queries |

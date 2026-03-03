---
name: "app-ampd-worker-events"
description: "Worker event streaming via Kafka for real-time sync progress updates. Load when asking about push-based sync notifications, event emission, or Kafka integration"
type: feature
status: experimental
components: "service:worker"
---

# Worker Event Streaming

## Summary

Worker Event Streaming enables a **Push Model** where workers emit events to Kafka when significant state changes occur. This complements the pull-based [Job Progress API](admin-jobs-progress.md) by enabling real-time dashboards, event-driven Platform backend updates, alerting on sync failures, and audit logging of sync activity.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Architecture](#architecture)
3. [Configuration](#configuration)
   - [Prerequisites](#prerequisites)
4. [Usage](#usage)
5. [Event Reference](#event-reference)
6. [References](#references)

## Key Concepts

- **Push Model**: Event-driven architecture where workers proactively emit state changes to Kafka, rather than clients polling for updates
- **Protobuf Encoding**: Events are encoded using Protocol Buffers for compact, schema-enforced messages
- **Event Envelope**: Common wrapper around all events containing metadata like `event_id`, `event_type`, `timestamp`, and `source`
- **At-Least-Once Delivery**: Events may be delivered multiple times; consumers must handle duplicates via `event_id` deduplication
- **Idempotent Updates**: Progress events are snapshots of current state, not deltas - duplicate delivery is harmless

## Architecture

Workers emit events directly to Kafka as sync jobs progress:

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│     Worker      │───►│      Kafka       │───►│   Consumers     │
│                 │    │                  │    │                 │
│ - Extracts      │    │  Topic:          │    │ - Platform API  │
│ - Commits       │    │  amp.worker      │    │ - Alerting      │
│ - Reports       │    │  .events         │    │ - Dashboards    │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

### Event Types

| Event Type       | Trigger                      | Purpose                    |
| ---------------- | ---------------------------- | -------------------------- |
| `sync.started`   | Job begins extraction        | Track job lifecycle        |
| `sync.progress`  | Time interval elapsed        | Report sync progress       |
| `sync.completed` | Job finishes successfully    | Mark dataset as synced     |
| `sync.failed`    | Job encounters fatal error   | Trigger alerts             |

#### Progress Throttling

Progress events are emitted on a **configurable time interval** (default: 10 seconds) rather than per-block to avoid excessive noise, especially for high-throughput chains like Solana. Events are only emitted when there is new progress to report since the last event.

#### Continuous vs Bounded Jobs

- **Bounded jobs** (with a configured end block): Emit all lifecycle events (`sync.started`, `sync.progress`, `sync.completed`, `sync.failed`)
- **Continuous jobs** (no end block): Emit `sync.started`, `sync.progress`, and `sync.failed`, but **never emit `sync.completed`** since they sync indefinitely

### Partitioning

Events are **partitioned by table** using the discriminator:

```
{namespace}/{name}/{manifest_hash}/{table_name}
```

This ensures ordering per-table while allowing parallelism across tables.

### Kafka Client

Uses [rskafka](https://github.com/influxdata/rskafka) - a pure Rust Kafka client with no C dependencies.

### Failure Handling

Event emission is a by-product of sync jobs - Kafka unavailability must not fail the job:

- **Retry with fixed delays**: 3 attempts with delays of 1s, 5s, 60s
- **Log errors**: Record failures for debugging without blocking sync
- **Fallback**: The [Job Progress API](admin-jobs-progress.md) remains available when events are disabled or Kafka is unavailable

## Configuration

Event streaming is **opt-in** (disabled by default). When disabled, the [Job Progress API](admin-jobs-progress.md) remains available for polling sync status.

```toml
[worker_events]
enabled = true
progress_interval = 10  # Emit progress events at most every 10 seconds

[worker_events.kafka]
brokers = ["kafka-1:9092", "kafka-2:9092"]
topic = "amp.worker.events"
partitions = 16
```

| Setting                     | Default               | Description                                           |
| --------------------------- | --------------------- | ----------------------------------------------------- |
| `enabled`                   | `false`               | Enable/disable event emission                         |
| `progress_interval`         | `10`                  | Time interval for progress events (in seconds)        |
| `kafka.brokers`             | -                     | Kafka broker addresses                                |
| `kafka.topic`               | `"amp.worker.events"` | Kafka topic name                                      |
| `kafka.partitions`          | `16`                  | Number of partitions (must match actual topic config) |
| `kafka.sasl_mechanism`      | -                     | SASL auth: `"PLAIN"`, `"SCRAM-SHA-256"`, `"SCRAM-SHA-512"` |
| `kafka.sasl_username`       | -                     | SASL username (required if sasl_mechanism set)        |
| `kafka.sasl_password`       | -                     | SASL password (required if sasl_mechanism set)        |
| `kafka.tls_enabled`         | `false`               | Enable TLS encryption for broker connections          |

### Authentication (Managed Kafka)

For managed Kafka services (AWS MSK, Confluent Cloud, Redpanda Cloud), configure SASL authentication:

```toml
[worker_events.kafka]
brokers = ["pkc-xxxxx.us-east-1.aws.confluent.cloud:9092"]
topic = "amp.worker.events"
sasl_mechanism = "SCRAM-SHA-256"
sasl_username = "your-api-key"
sasl_password = "your-api-secret"
tls_enabled = true
```

### Prerequisites

The Kafka topic must be created by the operator before enabling event streaming. Amp does not auto-create topics.

```bash
# Example: Create topic with Kafka CLI
kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic amp.worker.events \
  --partitions 16 \
  --replication-factor 3
```

If the topic doesn't exist, event sends will retry then log a warning and drop the event. Events are best-effort and do not block sync jobs.

## Usage

### Enable Kafka Events

```toml
[worker_events]
enabled = true
progress_interval = 10  # Optional: emit progress every 10s (default)

[worker_events.kafka]
brokers = ["localhost:9092"]
topic = "amp.worker.events"
partitions = 16  # Must match actual topic partition count
```

### Consuming Events

```bash
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic amp.worker.events \
  --from-beginning
```

## Event Reference

Events are encoded using Protocol Buffers. Examples below are shown as JSON for readability.

### Event Envelope

All events share a common envelope:

```json
{
  "event_id": "uuid-v7",
  "event_type": "sync.progress",
  "event_version": "1.0",
  "timestamp": "2026-01-15T10:30:00Z",
  "source": {
    "worker_id": "worker-01"
  },
  "payload": { ... }
}
```

### `sync.progress` Payload

```json
{
  "job_id": 12345,
  "dataset": {
    "namespace": "ethereum",
    "name": "mainnet",
    "manifest_hash": "2dbf16e8..."
  },
  "table_name": "blocks",
  "progress": {
    "start_block": 18900000,
    "current_block": 19000000,
    "files_count": 100,
    "total_size_bytes": 50000000
  }
}
```

The `dataset` + `table_name` fields form the **partition key** and discriminator for consumers.

### `sync.started` / `sync.completed` / `sync.failed`

All event payloads include the same `dataset` and `table_name` discriminator. Payloads align with the [Job Progress API](admin-jobs-progress.md) response format.

## References

- [admin-jobs-progress](admin-jobs-progress.md) - Related: Pull Model (REST API) for sync progress
- [app-ampd](app-ampd.md) - Base: ampd application overview
- [app-ampd-worker](app-ampd-worker.md) - Base: Worker architecture

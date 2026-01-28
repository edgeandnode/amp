---
name: "app-ampd-worker-events"
description: "Worker event streaming via Kafka for real-time sync progress updates. Load when asking about push-based sync notifications, event emission, or Kafka integration"
type: feature
components: "service:worker,crate:kafka"
---

# Worker Event Streaming

## Summary

Worker Event Streaming enables a **Push Model** where workers emit events to Kafka when significant state changes occur. This complements the pull-based [Job Progress API](admin-jobs-progress.md) by enabling real-time dashboards, event-driven Platform backend updates, alerting on sync failures, and audit logging of sync activity.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Architecture](#architecture)
3. [Configuration](#configuration)
4. [Usage](#usage)
5. [Event Reference](#event-reference)
6. [References](#references)

## Key Concepts

- **Push Model**: Event-driven architecture where workers proactively emit state changes to Kafka, rather than clients polling for updates
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

| Event Type       | Trigger                    | Purpose                    |
| ---------------- | -------------------------- | -------------------------- |
| `sync.started`   | Job begins extraction      | Track job lifecycle        |
| `sync.progress`  | Segment committed          | Report block range updates |
| `sync.completed` | Job finishes successfully  | Mark dataset as synced     |
| `sync.failed`    | Job encounters fatal error | Trigger alerts             |

### Partitioning

Events are **partitioned by table** using the discriminator:

```
{namespace}/{name}/{manifest_hash}/{table_name}
```

This ensures ordering per-table while allowing parallelism across tables.

### Kafka Client

Uses [rskafka](https://github.com/influxdata/rskafka) - a pure Rust Kafka client with no C dependencies.

## Configuration

```toml
[worker.events]
enabled = true

[worker.events.kafka]
brokers = ["kafka-1:9092", "kafka-2:9092"]
topic = "amp.worker.events"
```

| Setting         | Default               | Description                   |
| --------------- | --------------------- | ----------------------------- |
| `enabled`       | `false`               | Enable/disable event emission |
| `kafka.brokers` | -                     | Kafka broker addresses        |
| `kafka.topic`   | `"amp.worker.events"` | Kafka topic name              |

## Usage

### Enable Kafka Events

```toml
[worker.events]
enabled = true

[worker.events.kafka]
brokers = ["localhost:9092"]
topic = "amp.worker.events"
```

### Consuming Events

```bash
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic amp.worker.events \
  --from-beginning
```

## Event Reference

### Event Envelope

All events share a common envelope:

```json
{
  "event_id": "uuid-v4",
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

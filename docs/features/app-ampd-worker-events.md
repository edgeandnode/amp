---
name: "app-ampd-worker-events"
description: "Worker event streaming via Kafka or GCP Pub/Sub for real-time sync progress updates. Load when asking about push-based sync notifications, event emission, or message broker integration"
components: "service:worker,crate:common"
---

# Worker Event Streaming

## Summary

Worker Event Streaming enables a **Push Model** where workers emit events to a message broker (Kafka or GCP Pub/Sub) when significant state changes occur. This complements the pull-based [Dataset Sync Progress API](../glossary.md#sync-progress) by enabling real-time dashboards, event-driven Platform backend updates, alerting on sync failures, and audit logging of sync activity.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Architecture](#architecture)
3. [Configuration](#configuration)
4. [Usage](#usage)
5. [API Reference](#api-reference)
6. [Implementation](#implementation)
7. [Limitations](#limitations)
8. [References](#references)

## Key Concepts

- **Push Model**: Event-driven architecture where workers proactively emit state changes to a message broker, rather than clients polling for updates
- **Event Envelope**: Common wrapper around all events containing metadata like `event_id`, `event_type`, `timestamp`, and `source`
- **Broker Abstraction**: Runtime-configurable backend supporting both Kafka (self-hosted) and GCP Pub/Sub (managed)
- **At-Least-Once Delivery**: Events may be delivered multiple times; consumers must handle duplicates via `event_id` deduplication
- **Partition Key**: Events are partitioned by `manifest_hash` to ensure ordering per-dataset

## Architecture

### Event Flow

```
┌─────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Worker    │───►│  Message Broker  │───►│   Consumers     │
│             │    │  (Kafka/PubSub)  │    │                 │
│ - Extracts  │    │                  │    │ - Platform API  │
│ - Commits   │    │  Topic:          │    │ - Alerting      │
│ - Reports   │    │  amp.worker.events│   │ - Dashboards    │
└─────────────┘    └──────────────────┘    └─────────────────┘
```

### Event Types

| Event Type | Trigger | Purpose |
|------------|---------|---------|
| `sync.progress` | Segment committed | Report block range updates |
| `sync.started` | Job begins extraction | Track job lifecycle |
| `sync.completed` | Job finishes successfully | Mark dataset as synced |
| `sync.failed` | Job encounters fatal error | Trigger alerts |
| `sync.stalled` | No progress for threshold | Detect stuck jobs |
| `worker.heartbeat` | Periodic (configurable) | Worker liveness |

### Broker Selection

| Broker | Use Case | Pros | Cons |
|--------|----------|------|------|
| **Kafka** | Self-hosted deployments | Full control, replay capability, high throughput | Operational overhead |
| **GCP Pub/Sub** | Cloud deployments | Managed, serverless, native GCP integration | Vendor lock-in, no replay |

The implementation abstracts the broker behind a trait, allowing runtime configuration.

## Configuration

```toml
[worker.events]
# Enable event emission
enabled = true

# Broker type: "kafka" or "pubsub"
broker = "kafka"

# Kafka-specific
[worker.events.kafka]
bootstrap_servers = ["kafka-1:9092", "kafka-2:9092"]
topic = "amp.worker.events"
client_id = "amp-worker"
acks = "all"
compression = "zstd"

# Pub/Sub-specific
[worker.events.pubsub]
project_id = "my-gcp-project"
topic = "amp-worker-events"
```

| Setting | Default | Description |
|---------|---------|-------------|
| `enabled` | `false` | Enable/disable event emission |
| `broker` | `"kafka"` | Broker type: `kafka` or `pubsub` |
| `kafka.bootstrap_servers` | - | Kafka broker addresses |
| `kafka.topic` | `"amp.worker.events"` | Kafka topic name |
| `kafka.acks` | `"all"` | Acknowledgment level |
| `pubsub.project_id` | - | GCP project ID |
| `pubsub.topic` | - | Pub/Sub topic name |

## Usage

### Enable Kafka Events

```toml
[worker.events]
enabled = true
broker = "kafka"

[worker.events.kafka]
bootstrap_servers = ["localhost:9092"]
topic = "amp.worker.events"
```

### Enable Pub/Sub Events

```toml
[worker.events]
enabled = true
broker = "pubsub"

[worker.events.pubsub]
project_id = "my-gcp-project"
topic = "amp-worker-events"
```

### Consuming Events (Kafka)

```bash
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic amp.worker.events \
  --from-beginning
```

## API Reference

### Event Envelope

All events share a common envelope:

```json
{
  "event_id": "uuid-v4",
  "event_type": "sync.progress",
  "event_version": "1.0",
  "timestamp": "2026-01-15T10:30:00Z",
  "source": {
    "worker_id": "worker-01",
    "cluster_id": "prod-us-east"
  },
  "payload": { ... }
}
```

### `sync.progress` Payload

Emitted when a new segment is committed to a table.

```json
{
  "dataset": {
    "namespace": "ethereum",
    "name": "mainnet",
    "revision": "1.0.0",
    "manifest_hash": "2dbf16e8..."
  },
  "table_name": "blocks",
  "job_id": 12345,
  "progress": {
    "start_block": 10000,
    "current_block": 15000,
    "blocks_synced": 5000,
    "files_count": 50,
    "total_size_bytes": 2147483648
  },
  "segment": {
    "path": "ethereum/mainnet/blocks/00010000-00010999.parquet",
    "block_range": [10000, 10999],
    "size_bytes": 42949672
  }
}
```

### `sync.started` Payload

```json
{
  "dataset": {
    "namespace": "ethereum",
    "name": "mainnet",
    "revision": "1.0.0",
    "manifest_hash": "2dbf16e8..."
  },
  "table_name": "blocks",
  "job_id": 12345,
  "target_range": {
    "start_block": 10000,
    "end_block": null
  }
}
```

### `sync.failed` Payload

```json
{
  "dataset": {
    "namespace": "ethereum",
    "name": "mainnet",
    "revision": "1.0.0",
    "manifest_hash": "2dbf16e8..."
  },
  "table_name": "blocks",
  "job_id": 12345,
  "error": {
    "code": "PROVIDER_UNAVAILABLE",
    "message": "RPC endpoint returned 503",
    "retryable": true
  },
  "last_progress": {
    "current_block": 14500
  }
}
```

### Topic Structure

**Single Topic (Recommended):**
```
amp.worker.events
```

All events go to one topic, partitioned by `manifest_hash` to ensure ordering per-dataset. Consumers filter by `event_type`.

**Multi-Topic (Alternative):**
```
amp.worker.sync.progress
amp.worker.sync.lifecycle
amp.worker.heartbeat
```

## Implementation

### Event Emitter Trait

```rust
#[async_trait]
pub trait EventEmitter: Send + Sync {
    async fn emit(&self, event: WorkerEvent) -> Result<(), EventError>;
    async fn flush(&self) -> Result<(), EventError>;
}

pub struct KafkaEmitter { /* ... */ }
pub struct PubSubEmitter { /* ... */ }
pub struct NoOpEmitter; // For disabled events
```

### Worker Integration

Events are emitted from key points in the worker extraction loop:

```rust
impl Worker {
    async fn run_job(&self, job: Job) -> Result<()> {
        self.events.emit(SyncStarted { ... }).await?;

        loop {
            let segment = self.extract_segment().await?;
            self.commit_segment(segment).await?;

            // Emit progress after each commit
            self.events.emit(SyncProgress {
                segment: segment.metadata(),
                progress: self.calculate_progress(),
            }).await?;
        }

        self.events.emit(SyncCompleted { ... }).await?;
    }
}
```

### Source Files

- `crates/services/worker/src/events.rs` - Event emitter trait and implementations
- `crates/services/worker/src/lib.rs` - Worker integration points

## Limitations

- **At-least-once delivery**: Consumers must handle duplicate events via `event_id` deduplication
- **No local persistence**: If the broker is unavailable, events may be lost (progress can be reconstructed from the data lake)
- **Partition hotspots**: Hot datasets may cause partition imbalance when partitioning by `manifest_hash`
- **No schema registry**: Schema evolution requires manual coordination; future integration with Confluent Schema Registry planned
- **No webhook delivery**: HTTP webhook support for simpler integrations not yet available
- **No event replay**: Historical event replay requires separate object storage integration

## References

- [app-ampd](app-ampd.md) - Base: ampd application overview
- [app-ampd-worker](app-ampd-worker.md) - Base: Worker architecture

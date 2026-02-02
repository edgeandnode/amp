//! Event emitter trait and implementations.

use async_trait::async_trait;
use kafka_client::proto;

/// Trait for emitting worker events.
///
/// Implementations must be `Send + Sync` for use across async tasks.
#[async_trait]
pub trait EventEmitter: Send + Sync {
    /// Emits a sync.started event.
    async fn emit_sync_started(&self, event: proto::SyncStarted);

    /// Emits a sync.progress event.
    async fn emit_sync_progress(&self, event: proto::SyncProgress);

    /// Emits a sync.completed event.
    async fn emit_sync_completed(&self, event: proto::SyncCompleted);

    /// Emits a sync.failed event.
    async fn emit_sync_failed(&self, event: proto::SyncFailed);
}

/// No-op implementation for when events are disabled.
///
/// This implementation does nothing and is used when event streaming is disabled.
pub struct NoOpEmitter;

#[async_trait]
impl EventEmitter for NoOpEmitter {
    async fn emit_sync_started(&self, _event: proto::SyncStarted) {
        // No-op: events disabled
    }

    async fn emit_sync_progress(&self, _event: proto::SyncProgress) {
        // No-op: events disabled
    }

    async fn emit_sync_completed(&self, _event: proto::SyncCompleted) {
        // No-op: events disabled
    }

    async fn emit_sync_failed(&self, _event: proto::SyncFailed) {
        // No-op: events disabled
    }
}

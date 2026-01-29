//! Event emitter trait and implementations.

use async_trait::async_trait;

use super::types::{SyncCompletedEvent, SyncFailedEvent, SyncProgressEvent, SyncStartedEvent};

/// Trait for emitting worker events.
///
/// Implementations must be `Send + Sync` for use across async tasks.
#[async_trait]
pub trait EventEmitter: Send + Sync {
    /// Emits a sync.started event.
    async fn emit_sync_started(&self, event: SyncStartedEvent);

    /// Emits a sync.progress event.
    async fn emit_sync_progress(&self, event: SyncProgressEvent);

    /// Emits a sync.completed event.
    async fn emit_sync_completed(&self, event: SyncCompletedEvent);

    /// Emits a sync.failed event.
    async fn emit_sync_failed(&self, event: SyncFailedEvent);
}

/// No-op implementation for when events are disabled.
///
/// This implementation does nothing and is used when event streaming is disabled.
pub struct NoOpEmitter;

#[async_trait]
impl EventEmitter for NoOpEmitter {
    async fn emit_sync_started(&self, _event: SyncStartedEvent) {
        // No-op: events disabled
    }

    async fn emit_sync_progress(&self, _event: SyncProgressEvent) {
        // No-op: events disabled
    }

    async fn emit_sync_completed(&self, _event: SyncCompletedEvent) {
        // No-op: events disabled
    }

    async fn emit_sync_failed(&self, _event: SyncFailedEvent) {
        // No-op: events disabled
    }
}

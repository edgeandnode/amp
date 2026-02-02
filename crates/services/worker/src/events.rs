//! Worker event streaming module.
//!
//! This module provides infrastructure for emitting worker lifecycle events
//! (sync.started, sync.progress, sync.completed, sync.failed) to external systems.

mod emitter;
mod kafka;
mod progress_adapter;

pub use emitter::{EventEmitter, NoOpEmitter};
pub use kafka::KafkaEventEmitter;
pub use progress_adapter::WorkerProgressCallback;

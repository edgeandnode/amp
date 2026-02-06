//! Worker event streaming module.
//!
//! This module provides infrastructure for emitting worker lifecycle events
//! (sync.started, sync.progress, sync.completed, sync.failed) to external systems.

mod emitter;
mod kafka;
mod progress_adapter;

pub use self::{
    emitter::{EventEmitter, NoOpEmitter},
    kafka::KafkaEventEmitter,
    progress_adapter::WorkerProgressReporter,
};

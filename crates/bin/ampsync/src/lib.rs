// Core modules
pub mod arrow;
pub mod commands;
pub mod config;
pub mod engine;
pub mod manager;
pub mod manifest;
pub mod sql;
pub mod task;

// Re-export for convenience
pub use config::SyncConfig;
pub use engine::Engine;
pub use manager::StreamManager;

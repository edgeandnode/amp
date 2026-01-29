//! Persistent PostgreSQL database service
//!
//! This crate provides a service that manages PostgreSQL instances for development
//! and testing purposes. Unlike temporary database solutions, this service supports
//! **persistent data directories** that survive across restarts.
//!
//! # Features
//!
//! - **Persistence**: Data survives across restarts when using the same data directory
//! - **Zero-config startup**: Automatically initializes new databases or reuses existing ones
//! - **Unix socket connections**: No port conflicts, better security
//! - **Async-first design**: Built on Tokio for integration with async applications
//!
//! # Prerequisites
//!
//! PostgreSQL must be installed on the system. The following binaries must be available
//! in PATH or common installation locations:
//!
//! - `initdb` - For initializing new database clusters
//! - `postgres` - The database server
//!
//! # Examples
//!
//! ## Quick start with `service::new()` (app defaults)
//!
//! ```ignore
//! use std::path::PathBuf;
//! use metadata_db_postgres::service;
//!
//! let data_dir = PathBuf::from(".amp/metadb");
//! let (handle, fut) = service::new(data_dir).await?;
//!
//! // The future handles signals and process monitoring internally.
//! // Include it in a select! for structured concurrency:
//! tokio::select! {
//!     res = fut => { /* postgres exited or received signal */ }
//!     _ = other_service => { /* other service completed */ }
//! }
//! ```
//!
//! ## Full control with `PostgresBuilder`
//!
//! ```ignore
//! use metadata_db_postgres::PostgresBuilder;
//!
//! let (handle, fut) = PostgresBuilder::new(".amp/metadb")
//!     .locale("C")
//!     .encoding("UTF8")
//!     .config_param("max_connections", "50")
//!     .start()
//!     .await?;
//!
//! // Own the future in a select! — do NOT use tokio::spawn()
//! tokio::select! {
//!     res = fut => res?,
//!     _ = other_service => {},
//! }
//! ```
//!
//! # Data Directory Behavior
//!
//! When starting a PostgreSQL instance (via `service::new()` or `PostgresBuilder::start()`):
//!
//! 1. If the data directory doesn't exist, it is created with proper permissions (700)
//! 2. If `PG_VERSION` file exists in the directory, the existing database is reused
//! 3. If the directory is empty or has no `PG_VERSION`, `initdb` initializes a new database
//!
//! This allows the same data directory (e.g., `.amp/metadb/`) to be used across
//! multiple runs of `ampd solo`, preserving all database state.

mod postgres;
pub mod service;

pub use postgres::{PostgresBuilder, PostgresError};

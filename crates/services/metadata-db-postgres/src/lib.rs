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
//! # Example
//!
//! ```ignore
//! use std::path::PathBuf;
//! use metadata_db_postgres::service;
//!
//! // Start PostgreSQL with a persistent data directory
//! let data_dir = PathBuf::from(".amp/metadb");
//! let (handle, fut) = service::new(data_dir).await?;
//!
//! // Spawn the service future to keep the database running
//! tokio::spawn(fut);
//!
//! // Connect using the Unix socket URL
//! let url = handle.url();
//! println!("Database ready at: {}", url);
//!
//! // When done, gracefully shut down
//! handle.stop().await;
//! ```
//!
//! # Data Directory Behavior
//!
//! When `service::new()` is called:
//!
//! 1. If the data directory doesn't exist, it is created with proper permissions (700)
//! 2. If `PG_VERSION` file exists in the directory, the existing database is reused
//! 3. If the directory is empty or has no `PG_VERSION`, `initdb` initializes a new database
//!
//! This allows the same data directory (e.g., `.amp/metadb/`) to be used across
//! multiple runs of `ampd solo`, preserving all database state.

mod error;
mod postgres;
pub mod service;

pub use error::PostgresError;

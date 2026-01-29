//! Error types for PostgreSQL process management
//!
//! This module defines error types for PostgreSQL lifecycle operations including
//! binary discovery, initialization, startup, and shutdown.

use std::path::PathBuf;

/// Errors that can occur during PostgreSQL process management
///
/// This enum covers all failure modes when managing a PostgreSQL instance,
/// from finding binaries to starting and stopping the database.
#[derive(Debug, thiserror::Error)]
pub enum PostgresError {
    /// PostgreSQL binary not found in PATH
    ///
    /// This error occurs when a required PostgreSQL binary (`initdb`, `postgres`,
    /// or `pg_ctl`) cannot be found in the system PATH.
    ///
    /// Common causes:
    /// - PostgreSQL is not installed on the system
    /// - PostgreSQL bin directory is not in PATH
    /// - Wrong PostgreSQL version installed
    #[error("PostgreSQL binary '{name}' not found in PATH")]
    BinaryNotFound {
        /// Name of the binary that was not found
        name: String,
    },

    /// Failed to initialize the PostgreSQL data directory
    ///
    /// This error occurs when `initdb` fails to create a new database cluster.
    ///
    /// Common causes:
    /// - Insufficient permissions to create the data directory
    /// - Data directory already exists and is not empty
    /// - Disk space issues
    #[error("Failed to initialize PostgreSQL data directory at '{data_dir}'")]
    InitDbFailed {
        /// Path to the data directory that failed to initialize
        data_dir: PathBuf,
        /// The underlying IO or process error
        #[source]
        source: std::io::Error,
    },

    /// initdb process exited with non-zero status
    ///
    /// This error occurs when `initdb` runs but exits with an error code.
    #[error("initdb exited with status {status}: {stderr}")]
    InitDbExitError {
        /// Exit status code
        status: i32,
        /// Standard error output from initdb
        stderr: String,
    },

    /// Failed to start the PostgreSQL server
    ///
    /// This error occurs when the `postgres` process cannot be spawned.
    ///
    /// Common causes:
    /// - Insufficient permissions
    /// - Data directory does not exist or is corrupted
    /// - Another PostgreSQL instance is using the same data directory
    #[error("Failed to start PostgreSQL server")]
    StartFailed {
        /// The underlying IO error
        #[source]
        source: std::io::Error,
    },

    /// PostgreSQL server failed to become ready
    ///
    /// This error occurs when the PostgreSQL server starts but doesn't
    /// create its socket file within the expected timeout.
    #[error("PostgreSQL server failed to become ready within {timeout_secs} seconds")]
    ReadinessTimeout {
        /// Number of seconds waited before timing out
        timeout_secs: u64,
    },

    /// Failed to create the data directory
    ///
    /// This error occurs when the parent directories for the data path
    /// cannot be created.
    #[error("Failed to create data directory at '{path}'")]
    CreateDataDir {
        /// Path that could not be created
        path: PathBuf,
        /// The underlying IO error
        #[source]
        source: std::io::Error,
    },

    /// Failed to set permissions on the data directory
    ///
    /// PostgreSQL requires the data directory to have restricted permissions (700).
    #[error("Failed to set permissions on data directory at '{path}'")]
    SetPermissions {
        /// Path where permissions could not be set
        path: PathBuf,
        /// The underlying IO error
        #[source]
        source: std::io::Error,
    },

    /// Failed to shut down the PostgreSQL server
    ///
    /// This error occurs when the server cannot be stopped gracefully.
    #[error("Failed to shut down PostgreSQL server")]
    ShutdownFailed {
        /// The underlying IO error
        #[source]
        source: std::io::Error,
    },

    /// PostgreSQL server exited unexpectedly
    ///
    /// This error occurs when the postgres process terminates while still expected
    /// to be running.
    #[error("PostgreSQL server exited unexpectedly with status {status:?}")]
    UnexpectedExit {
        /// Exit status code, if available
        status: Option<i32>,
    },
}

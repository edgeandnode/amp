//! Test infrastructure for creating isolated end-to-end test environments.
//!
//! This module provides a comprehensive testing framework for the Amp ETL pipeline,
//! enabling the creation of completely isolated test environments with their own
//! temporary directories, configurations, and resource loading. The infrastructure
//! supports full end-to-end testing of the distributed Amp architecture including
//! servers, workers, and database components.
//!
//! # Core Principles
//!
//! ## Complete Isolation
//! Each test environment is completely isolated from others and from the main project
//! configuration. Tests cannot interfere with each other, ensuring reliable and
//! reproducible test execution.
//!
//! ## Selective Resource Loading
//! Only explicitly requested datasets and provider configurations are copied to test
//! environments, improving performance and reducing test setup overhead.
//!
//! ## Dynamic Configuration Generation
//! Test-specific configurations are generated at runtime with customizable overrides,
//! allowing tests to specify exactly the configuration they need.
//!
//! ## Automatic Cleanup
//! Temporary directories and resources are automatically cleaned up when test
//! environments are dropped, unless the `TESTS_KEEP_TEMP_DIRS=1` environment
//! variable is set for debugging purposes.
//!
//! # Module Organization
//!
//! - [`ctx`]: **Primary entry point** - High-level test environment creation and management
//! - [`env_dir`]: Low-level directory structure creation and resource preloading
//! - [`fixtures`]: Test fixtures for daemon components (servers, workers, databases)
//! - [`helpers`]: Common test helper functions for data extraction and validation
//! - [`debug`]: Debug utilities and environment variable handling for test development

mod config;
pub mod ctx;
pub mod debug;
mod env_dir;
pub mod helpers;
pub mod metrics;

/// Low-level test fixtures and directory utilities.
///
/// This module provides access to the underlying fixture types for advanced use cases
/// requiring direct directory manipulation. For most testing scenarios, use the [`ctx`]
/// module which provides the high-level [`TestCtxBuilder`](ctx::TestCtxBuilder) API.
///
/// The fixtures module serves test authors who need direct access to temporary
/// directory structures and file operations within their test environments.
/// All fixture types maintain the same isolation and cleanup guarantees as the
/// parent testlib infrastructure.
pub mod fixtures {
    mod ampctl;
    mod anvil;
    mod cli;
    mod contract_artifact;
    mod daemon_config;
    mod daemon_controller;
    mod daemon_server;
    mod daemon_state_dir;
    mod daemon_worker;
    mod flight_client;
    mod jsonl_client;
    mod metadata_db;
    mod package;
    mod snapshot_ctx;

    // Re-export commonly used types for convenience
    pub use ampctl::*;
    pub use anvil::*;
    pub use cli::*;
    pub use contract_artifact::*;
    pub use daemon_config::*;
    pub use daemon_controller::*;
    pub use daemon_server::*;
    pub use daemon_state_dir::*;
    pub use daemon_worker::*;
    pub use flight_client::*;
    pub use jsonl_client::*;
    pub use metadata_db::*;
    pub use package::*;
    pub use snapshot_ctx::*;
}

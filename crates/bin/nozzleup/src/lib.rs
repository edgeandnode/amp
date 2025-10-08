pub mod builder;
pub mod commands;
pub mod config;
pub mod github;
pub mod install;
pub mod platform;
pub mod shell;
pub mod ui;
pub mod updater;
pub mod version_manager;

/// Default GitHub repository for nozzle releases
pub const DEFAULT_REPO: &str = "edgeandnode/project-nozzle";

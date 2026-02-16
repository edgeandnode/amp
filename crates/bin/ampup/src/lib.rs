pub mod builder;
pub mod commands;
pub mod config;
pub mod github;
pub mod install;
pub mod platform;
pub mod rate_limiter;
pub mod shell;
pub mod token;
pub mod updater;
pub mod version_manager;

#[macro_use]
pub mod ui;

/// Default GitHub repository for amp releases
pub const DEFAULT_REPO: &str = "edgeandnode/amp";

/// Default GitHub repository for amp development
pub const DEFAULT_REPO_PRIVATE: &str = "edgeandnode/amp-private";

#[cfg(test)]
mod tests;

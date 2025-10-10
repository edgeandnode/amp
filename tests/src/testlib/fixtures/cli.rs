//! amp CLI fixture for executing dataset commands.
//!
//! This fixture provides a convenient interface for executing amp CLI commands
//! such as `build`, `register`, and `dump` in test environments. It handles the command
//! execution, environment variable setup, and error handling.

use std::{
    path::Path,
    process::{ExitStatus, Stdio},
};

use common::BoxError;

/// amp CLI fixture for executing dataset commands.
///
/// This fixture wraps the amp CLI and provides convenient methods for
/// building, registering, and dumping datasets in test environments. It automatically
/// handles the environment variable setup and command execution.
#[derive(Clone, Debug)]
pub struct AmpCli {
    admin_url: String,
}

impl AmpCli {
    /// Create a new amp CLI fixture from server bound addresses.
    ///
    /// Takes the bound addresses from a running Amp server and formats
    /// them into the appropriate URLs for CLI commands.
    pub fn new(admin_api_url: impl Into<String>) -> Self {
        Self {
            admin_url: admin_api_url.into(),
        }
    }

    /// Get the admin URL this CLI is configured to use.
    pub fn admin_url(&self) -> &str {
        &self.admin_url
    }

    /// Build a dataset manifest using amp build command.
    ///
    /// Runs `pnpm amp build` in the specified dataset directory.
    /// Optionally accepts a config file parameter.
    #[tracing::instrument(skip_all, err)]
    pub async fn build(&self, path: &Path, config: Option<&str>) -> Result<(), BoxError> {
        tracing::debug!(
            "Running 'amp build' in `{}` with config: {:?}",
            path.to_string_lossy(),
            config
        );

        let mut args = vec!["amp", "build"];
        if let Some(config) = config {
            args.push("--config");
            args.push(config);
        }

        run_amp_command(&self.admin_url, path, &args, "build dataset").await
    }

    /// Register a dataset using amp register command.
    ///
    /// Runs `pnpm amp register` in the specified dataset directory.
    /// Optionally accepts a config file parameter.
    #[tracing::instrument(skip_all, err)]
    pub async fn register(
        &self,
        dataset_path: &Path,
        config: Option<&str>,
    ) -> Result<(), BoxError> {
        tracing::debug!(
            "Running 'amp register' in `{}` with config: {:?}",
            dataset_path.to_string_lossy(),
            config
        );

        let mut args = vec!["amp", "register"];
        if let Some(config) = config {
            args.push("--config");
            args.push(config);
        }

        run_amp_command(&self.admin_url, dataset_path, &args, "register dataset").await
    }

    /// Dump a dataset using amp dump command.
    ///
    /// Runs `pnpm amp dump` with the specified dataset name and optional end block.
    /// Optionally accepts a config file parameter.
    #[tracing::instrument(skip_all, err)]
    pub async fn dump(
        &self,
        path: &Path,
        dataset: &str,
        end_block: Option<u64>,
        config: Option<&str>,
    ) -> Result<(), BoxError> {
        tracing::debug!(
            "Running 'amp dump' in `{}` for dataset {} with end block: {:?} and config: {:?}",
            path.to_string_lossy(),
            dataset,
            end_block,
            config
        );

        let mut args = vec!["amp", "dump", dataset];
        let end_block_str;
        if let Some(end) = end_block {
            args.push("--end-block");
            end_block_str = end.to_string();
            args.push(&end_block_str);
        }
        if let Some(config) = config {
            args.push("--config");
            args.push(config);
        }

        run_amp_command(&self.admin_url, path, &args, "dump dataset").await
    }
}

/// Execute a amp CLI command with common setup and error handling.
async fn run_amp_command(
    admin_url: &str,
    path: &Path,
    args: &[&str],
    command_name: &str,
) -> Result<(), BoxError> {
    let status = tokio::process::Command::new("pnpm")
        .args(args)
        .env("AMP_ADMIN_URL", admin_url)
        .current_dir(path)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .await?;

    if status != ExitStatus::default() {
        return Err(BoxError::from(format!(
            "Failed to {}: pnpm {} failed with exit code {status}",
            command_name,
            args.join(" ")
        )));
    }

    Ok(())
}

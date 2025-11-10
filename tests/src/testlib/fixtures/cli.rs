//! amp CLI fixture for executing dataset commands.
//!
//! This fixture provides a convenient interface for executing amp CLI commands
//! such as `build`, `register`, and `deploy` in test environments. It handles the command
//! execution, environment variable setup, and error handling.

use std::{
    path::Path,
    process::{ExitStatus, Stdio},
};

use common::BoxError;

/// amp CLI fixture for executing dataset commands.
///
/// This fixture wraps the amp CLI and provides convenient methods for
/// building, registering, and deploying datasets in test environments. It automatically
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
    /// Optionally accepts a version tag (semantic version or "dev") and a config file parameter.
    /// If no tag is provided or if tag is "dev", registers without a version tag (bumps dev tag).
    #[tracing::instrument(skip_all, err)]
    pub async fn register(
        &self,
        dataset_path: &Path,
        tag: impl Into<Option<&str>>,
        config: Option<&str>,
    ) -> Result<(), BoxError> {
        let tag = tag.into();
        tracing::debug!(
            "Running 'amp register' in `{}` with tag: {:?}, config: {:?}",
            dataset_path.to_string_lossy(),
            tag,
            config
        );

        let mut args = vec!["amp", "register"];

        // Add optional config (options must come before positional arguments)
        if let Some(config) = config {
            args.push("--config");
            args.push(config);
        }

        // Add optional tag (positional argument must come last)
        if let Some(tag_str) = tag {
            args.push("--tag");
            args.push(tag_str);
        }

        run_amp_command(&self.admin_url, dataset_path, &args, "register dataset").await
    }

    /// Deploy a dataset version using amp deploy command.
    ///
    /// Runs `pnpm amp deploy` with an optional dataset reference (namespace/name@revision),
    /// optional end block, and optional parallelism level. If no reference is provided,
    /// the command uses the dataset from the config file with a "dev" tag.
    /// Optionally accepts a config file parameter.
    #[tracing::instrument(skip_all, err)]
    pub async fn deploy(
        &self,
        path: &Path,
        reference: Option<&str>,
        end_block: Option<u64>,
        parallelism: Option<u64>,
        config: Option<&str>,
    ) -> Result<(), BoxError> {
        tracing::debug!(
            "Running 'amp deploy' in `{}` with reference: {:?}, end block: {:?}, parallelism: {:?}, config: {:?}",
            path.to_string_lossy(),
            reference,
            end_block,
            parallelism,
            config
        );

        let mut args = vec!["amp", "deploy"];

        // Add optional dataset reference (positional argument)
        if let Some(ref_str) = reference {
            args.push("--reference");
            args.push(ref_str);
        }

        // Add optional end block
        let end_block_str;
        if let Some(end) = end_block {
            args.push("--end-block");
            end_block_str = end.to_string();
            args.push(&end_block_str);
        }

        // Add optional parallelism
        let parallelism_str;
        if let Some(jobs) = parallelism {
            args.push("--parallelism");
            parallelism_str = jobs.to_string();
            args.push(&parallelism_str);
        }

        // Add optional config
        if let Some(config) = config {
            args.push("--config");
            args.push(config);
        }

        run_amp_command(&self.admin_url, path, &args, "deploy dataset").await
    }
}

/// Execute an Amp CLI command with common setup and error handling.
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

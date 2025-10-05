use std::os::unix::fs::symlink;

use anyhow::{Context, Result};
use fs_err as fs;

use crate::config::Config;

pub fn run(version: &str) -> Result<()> {
    let config = Config::new()?;

    let version_dir = config.versions_dir.join(version);
    if !version_dir.exists() {
        anyhow::bail!(
            "Version {} is not installed. Run 'nozzleup install {}' to install it",
            version,
            version
        );
    }

    let binary_path = config.version_binary_path(version);
    if !binary_path.exists() {
        anyhow::bail!("Binary not found for version {}", version);
    }

    let active_path = config.active_binary_path();

    // Remove existing symlink if it exists
    if active_path.exists() || active_path.is_symlink() {
        fs::remove_file(&active_path).context("Failed to remove existing symlink")?;
    }

    // Create new symlink
    symlink(&binary_path, &active_path).context("Failed to create symlink")?;

    // Update current version file
    config.set_current_version(version)?;

    println!("nozzleup: Switched to nozzle {}", version);

    Ok(())
}

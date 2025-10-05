use std::os::unix::fs::symlink;

use anyhow::{Context, Result};
use dialoguer::{Select, theme::ColorfulTheme};
use fs_err as fs;

use crate::config::Config;

pub fn run(version: Option<String>) -> Result<()> {
    let config = Config::new()?;

    // If version is provided, use it directly
    let version = match version {
        Some(v) => v,
        None => {
            // Interactive mode: prompt user to select from installed versions
            select_version(&config)?
        }
    };

    let version_dir = config.versions_dir.join(&version);
    if !version_dir.exists() {
        anyhow::bail!(
            "Version {} is not installed. Run 'nozzleup install {}' to install it",
            version,
            version
        );
    }

    let binary_path = config.version_binary_path(&version);
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
    config.set_current_version(&version)?;

    println!("nozzleup: Switched to nozzle {}", version);

    Ok(())
}

fn select_version(config: &Config) -> Result<String> {
    // Check if versions directory exists
    if !config.versions_dir.exists() {
        anyhow::bail!("No versions installed. Run 'nozzleup install' to install nozzle");
    }

    // Get list of installed versions
    let mut versions = Vec::new();
    for entry in fs::read_dir(&config.versions_dir).context("Failed to read versions directory")? {
        let entry = entry.context("Failed to read directory entry")?;
        if entry
            .file_type()
            .context("Failed to get file type")?
            .is_dir()
        {
            let version = entry.file_name().to_string_lossy().to_string();
            versions.push(version);
        }
    }

    if versions.is_empty() {
        anyhow::bail!("No versions installed. Run 'nozzleup install' to install nozzle");
    }

    // Sort versions
    versions.sort();

    // Get current version
    let current_version = config.current_version()?;

    // Create display items with current indicator
    let display_items: Vec<String> = versions
        .iter()
        .map(|v| {
            if Some(v) == current_version.as_ref() {
                format!("{} (current)", v)
            } else {
                v.clone()
            }
        })
        .collect();

    // Find default selection (current version if exists)
    let default_index = current_version
        .as_ref()
        .and_then(|cv| versions.iter().position(|v| v == cv))
        .unwrap_or(0);

    // Show interactive selection
    let selection = Select::with_theme(&ColorfulTheme::default())
        .with_prompt("Select a version to use")
        .default(default_index)
        .items(&display_items)
        .interact()
        .context("Failed to get user selection")?;

    Ok(versions[selection].clone())
}

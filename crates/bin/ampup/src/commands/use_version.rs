use anyhow::{Context, Result};
use dialoguer::{Select, theme::ColorfulTheme};

use crate::{
    config::Config,
    ui,
    version_manager::{VersionError, VersionManager},
};

pub fn run(install_dir: Option<std::path::PathBuf>, version: Option<String>) -> Result<()> {
    let config = Config::new(install_dir)?;
    let version_manager = VersionManager::new(config);

    // If version is provided, use it directly, otherwise prompt user to select from installed versions
    let version = match version {
        Some(v) => v,
        None => select_version(&version_manager)?,
    };

    switch_to_version(&version_manager, &version)?;
    ui::success!("Switched to ampd {}", ui::version(&version));

    Ok(())
}

/// Switch to a specific installed version
pub fn switch_to_version(version_manager: &VersionManager, version: &str) -> Result<()> {
    version_manager.activate(version)?;
    Ok(())
}

fn select_version(version_manager: &VersionManager) -> Result<String> {
    let versions = version_manager.list_installed()?;

    if versions.is_empty() {
        return Err(VersionError::NoVersionsInstalled.into());
    }

    // Get current version
    let current_version = version_manager.get_current()?;

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

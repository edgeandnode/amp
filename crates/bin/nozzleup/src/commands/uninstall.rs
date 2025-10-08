use anyhow::{Context, Result};
use fs_err as fs;

use crate::config::Config;

pub fn run(install_dir: Option<std::path::PathBuf>, version: &str) -> Result<()> {
    let config = Config::new(install_dir)?;

    let version_dir = config.versions_dir.join(version);
    if !version_dir.exists() {
        anyhow::bail!("Version {} is not installed", version);
    }

    // Check if this is the current version
    let current = config.current_version()?;
    if current.as_deref() == Some(version) {
        println!("nozzleup: Warning: Uninstalling the currently active version");
        println!("nozzleup: You may want to switch to another version first");
    }

    // Remove the version directory
    fs::remove_dir_all(&version_dir).context("Failed to remove version directory")?;

    println!("nozzleup: Uninstalled nozzle {}", version);

    // If this was the current version, clear the current version file
    if current.as_deref() == Some(version) {
        let current_file = config.current_version_file();
        if current_file.exists() {
            fs::remove_file(&current_file).context("Failed to remove current version file")?;
        }

        // Remove the symlink
        let active_path = config.active_binary_path();
        if active_path.exists() || active_path.is_symlink() {
            fs::remove_file(&active_path).context("Failed to remove symlink")?;
        }

        println!("nozzleup: No version is currently active");
        println!("nozzleup: Run 'nozzleup use <version>' to activate a version");
    }

    Ok(())
}

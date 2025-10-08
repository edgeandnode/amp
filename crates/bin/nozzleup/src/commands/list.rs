use anyhow::{Context, Result};
use fs_err as fs;

use crate::config::Config;

pub fn run(install_dir: Option<std::path::PathBuf>) -> Result<()> {
    let config = Config::new(install_dir)?;

    if !config.versions_dir.exists() {
        println!("nozzleup: No versions installed");
        return Ok(());
    }

    let current_version = config.current_version()?;

    println!("nozzleup: Installed versions:");

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

    versions.sort();

    for version in versions {
        if Some(&version) == current_version.as_ref() {
            println!("  * {} (current)", version);
        } else {
            println!("    {}", version);
        }
    }

    Ok(())
}

use anyhow::Result;
use console::style;

use crate::{config::Config, ui, version_manager::VersionManager};

pub fn run(install_dir: Option<std::path::PathBuf>) -> Result<()> {
    let config = Config::new(install_dir)?;
    let version_manager = VersionManager::new(config);

    let versions = version_manager.list_installed()?;

    if versions.is_empty() {
        ui::info!("No versions installed");
        return Ok(());
    }

    let current_version = version_manager.get_current()?;

    ui::info!("Installed versions:");

    for version in versions {
        if Some(&version) == current_version.as_ref() {
            println!(
                "  {} {} {}",
                style("*").green().bold(),
                style(&version).bold(),
                style("(current)").dim()
            );
        } else {
            println!("    {}", version);
        }
    }

    Ok(())
}

use anyhow::Result;

use crate::{config::Config, ui, version_manager::VersionManager};

pub fn run(install_dir: Option<std::path::PathBuf>, version: &str) -> Result<()> {
    let config = Config::new(install_dir)?;
    let version_manager = VersionManager::new(config);

    // Check if this is the current version before uninstalling
    let was_current = version_manager.get_current()?.as_deref() == Some(version);

    // Uninstall the version
    version_manager.uninstall(version)?;

    ui::success!("Uninstalled ampd {}", ui::version(version));

    if was_current {
        ui::warn!("No version is currently active");
        ui::detail!("Run 'ampup use <version>' to activate a version");
    }

    Ok(())
}

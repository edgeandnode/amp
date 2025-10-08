use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use fs_err as fs;

use crate::{DEFAULT_REPO, config::Config, shell, ui};

pub async fn run(
    install_dir: Option<PathBuf>,
    no_modify_path: bool,
    no_install_latest: bool,
) -> Result<()> {
    // Create config to get all the paths
    let config = Config::new(install_dir)?;
    let nozzleup_path = config.nozzleup_binary_path();

    // Check if already initialized
    if nozzleup_path.exists() {
        bail!(
            "nozzleup is already initialized at {}. \
             If you want to reinstall, please remove the directory first.",
            config.nozzle_dir.display()
        );
    }

    ui::info(format!(
        "Installing to {}",
        ui::path(config.nozzle_dir.display())
    ));

    // Create directory structure using Config's ensure_dirs
    config.ensure_dirs()?;

    // Copy self to installation directory
    let current_exe = std::env::current_exe().context("Failed to get current executable path")?;
    fs::copy(&current_exe, &nozzleup_path).with_context(|| {
        format!(
            "Failed to copy nozzleup from {} to {}",
            current_exe.display(),
            nozzleup_path.display()
        )
    })?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&nozzleup_path)
            .context("Failed to get nozzleup binary metadata")?
            .permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&nozzleup_path, perms)
            .context("Failed to set nozzleup binary permissions")?;
    }

    ui::success(format!(
        "Installed nozzleup to {}",
        ui::path(nozzleup_path.display())
    ));

    // Modify PATH if requested
    if !no_modify_path {
        let bin_dir_str = config.bin_dir.to_string_lossy();
        if let Err(e) = shell::add_to_path(&bin_dir_str) {
            ui::warn(format!("Failed to add to PATH: {}", e));
            ui::detail(format!("Please manually add {} to your PATH", bin_dir_str));
        }
    } else {
        ui::detail(format!(
            "Skipping PATH modification. Add {} to your PATH manually",
            ui::path(config.bin_dir.display())
        ));
    }

    // Install latest nozzle if requested
    if !no_install_latest {
        ui::info("Installing latest nozzle version");
        // We'll use the existing install command
        crate::commands::install::run(
            Some(config.nozzle_dir),
            DEFAULT_REPO.to_string(),
            None,
            None,
            None,
            None,
        )
        .await?;
    } else {
        ui::detail("Skipping installation of latest nozzle");
        ui::detail("Run 'nozzleup install' to install nozzle when ready");
    }

    ui::success("Installation complete!");

    Ok(())
}

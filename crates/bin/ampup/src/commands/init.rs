use std::path::PathBuf;

use anyhow::{Context, Result};
use fs_err as fs;

use crate::{DEFAULT_REPO, config::Config, shell, ui};

#[derive(Debug)]
pub enum InitError {
    AlreadyInitialized { install_dir: PathBuf },
}

impl std::fmt::Display for InitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyInitialized { install_dir } => {
                writeln!(f, "ampup is already initialized")?;
                writeln!(f, "  Installation: {}", install_dir.display())?;
                writeln!(f)?;
                writeln!(f, "  If you want to reinstall, remove the directory first:")?;
                writeln!(f, "    rm -rf {}", install_dir.display())?;
            }
        }
        Ok(())
    }
}

impl std::error::Error for InitError {}

pub async fn run(
    install_dir: Option<PathBuf>,
    no_modify_path: bool,
    no_install_latest: bool,
    github_token: Option<String>,
) -> Result<()> {
    // Create config to get all the paths
    let config = Config::new(install_dir)?;
    let ampup_path = config.ampup_binary_path();

    // Check if already initialized
    if ampup_path.exists() {
        return Err(InitError::AlreadyInitialized {
            install_dir: config.amp_dir.clone(),
        }
        .into());
    }

    ui::info!("Installing to {}", ui::path(config.amp_dir.display()));

    // Create directory structure using Config's ensure_dirs
    config.ensure_dirs()?;

    // Copy self to installation directory
    let current_exe = std::env::current_exe().context("Failed to get current executable path")?;
    fs::copy(&current_exe, &ampup_path).with_context(|| {
        format!(
            "Failed to copy ampup from {} to {}",
            current_exe.display(),
            ampup_path.display()
        )
    })?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&ampup_path)
            .context("Failed to get ampup binary metadata")?
            .permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&ampup_path, perms)
            .context("Failed to set ampup binary permissions")?;
    }

    ui::success!("Installed ampup to {}", ui::path(ampup_path.display()));

    // Modify PATH if requested
    if !no_modify_path {
        let bin_dir_str = config.bin_dir.to_string_lossy();
        if let Err(e) = shell::add_to_path(&bin_dir_str) {
            ui::warn!("Failed to add to PATH: {}", e);
            ui::detail!("Please manually add {} to your PATH", bin_dir_str);
        }
    } else {
        ui::detail!(
            "Skipping PATH modification. Add {} to your PATH manually",
            ui::path(config.bin_dir.display())
        );
    }

    // Install latest ampd if requested
    if !no_install_latest {
        ui::info!("Installing latest ampd version");
        // We'll use the existing install command
        crate::commands::install::run(
            Some(config.amp_dir),
            DEFAULT_REPO.to_string(),
            github_token,
            None,
            None,
            None,
        )
        .await?;
    } else {
        ui::detail!("Skipping installation of latest ampd");
        ui::detail!("Run 'ampup install' to install ampd when ready");
    }

    ui::success!("Installation complete!");

    Ok(())
}

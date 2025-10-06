use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use fs_err as fs;

use crate::{config::Config, shell};

pub async fn run(
    install_dir: Option<PathBuf>,
    no_modify_path: bool,
    no_install_latest: bool,
) -> Result<()> {
    let config = Config::new()?;

    // Determine installation directory
    let nozzle_dir = install_dir.unwrap_or(config.nozzle_dir.clone());
    let bin_dir = nozzle_dir.join("bin");
    let versions_dir = nozzle_dir.join("versions");
    let nozzleup_path = bin_dir.join("nozzleup");

    // Check if already initialized
    if nozzleup_path.exists() {
        bail!(
            "nozzleup is already initialized at {}. \
             If you want to reinstall, please remove the directory first.",
            nozzle_dir.display()
        );
    }

    println!("nozzleup: Installing to {}", nozzle_dir.display());

    // Create directory structure
    fs::create_dir_all(&bin_dir)
        .with_context(|| format!("Failed to create bin directory at {}", bin_dir.display()))?;
    fs::create_dir_all(&versions_dir).with_context(|| {
        format!(
            "Failed to create versions directory at {}",
            versions_dir.display()
        )
    })?;

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

    println!(
        "nozzleup: Installed nozzleup to {}",
        nozzleup_path.display()
    );

    // Modify PATH if requested
    if !no_modify_path {
        let bin_dir_str = bin_dir.to_string_lossy();
        if let Err(e) = shell::add_to_path(&bin_dir_str) {
            eprintln!("nozzleup: warning: Failed to add to PATH: {}", e);
            eprintln!(
                "nozzleup: warning: Please manually add {} to your PATH",
                bin_dir_str
            );
        }
    } else {
        println!(
            "nozzleup: Skipping PATH modification. Add {} to your PATH manually.",
            bin_dir.display()
        );
    }

    // Install latest nozzle if requested
    if !no_install_latest {
        println!("nozzleup: Installing latest nozzle version...");
        // We'll use the existing install command
        crate::commands::install::run(None, None, None).await?;
    } else {
        println!("nozzleup: Skipping installation of latest nozzle.");
        println!("nozzleup: Run 'nozzleup install' to install nozzle when ready.");
    }

    println!("nozzleup: Installation complete!");

    Ok(())
}

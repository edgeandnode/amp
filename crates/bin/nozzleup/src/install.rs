use std::os::unix::fs::symlink;

use anyhow::{Context, Result};
use fs_err as fs;

use crate::{
    config::Config,
    github::GitHubClient,
    platform::{Architecture, Platform, artifact_name},
};

pub struct Installer {
    config: Config,
    github: GitHubClient,
}

impl Installer {
    pub fn new(config: Config, github: GitHubClient) -> Self {
        Self { config, github }
    }

    /// Install nozzle from a GitHub release
    pub async fn install_from_release(
        &self,
        version: &str,
        platform: Platform,
        arch: Architecture,
    ) -> Result<()> {
        self.config.ensure_dirs()?;

        let artifact = artifact_name(platform, arch);

        println!("nozzleup: Downloading {} for {}...", version, artifact);

        // Download the binary over HTTPS (TLS provides integrity verification)
        let binary_data = self
            .github
            .download_release_asset(version, &artifact)
            .await
            .context("Failed to download binary")?;

        if binary_data.is_empty() {
            anyhow::bail!("Downloaded binary is empty");
        }

        println!("nozzleup: Downloaded {} bytes", binary_data.len());

        // Install the binary
        self.install_binary(version, &binary_data)?;

        Ok(())
    }

    /// Install the binary to the version directory
    fn install_binary(&self, version: &str, data: &[u8]) -> Result<()> {
        // Create version directory
        let version_dir = self.config.versions_dir.join(version);
        fs::create_dir_all(&version_dir).context("Failed to create version directory")?;

        let binary_path = version_dir.join("nozzle");

        // Write binary
        fs::write(&binary_path, data).context("Failed to write binary")?;

        // Make executable
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = fs::metadata(&binary_path)
                .context("Failed to get binary metadata")?
                .permissions();
            perms.set_mode(0o755);
            fs::set_permissions(&binary_path, perms)
                .context("Failed to set executable permissions")?;
        }

        // Create symlink
        let active_path = self.config.active_binary_path();

        // Remove existing symlink if it exists
        if active_path.exists() || active_path.is_symlink() {
            fs::remove_file(&active_path).context("Failed to remove existing symlink")?;
        }

        // Create new symlink
        symlink(&binary_path, &active_path).context("Failed to create symlink")?;

        // Update current version
        self.config.set_current_version(version)?;

        Ok(())
    }
}

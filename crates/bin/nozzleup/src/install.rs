use std::os::unix::fs::symlink;

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};

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
        skip_verification: bool,
    ) -> Result<()> {
        self.config.ensure_dirs()?;

        let artifact = artifact_name(platform, arch);

        println!("nozzleup: Downloading {} for {}...", version, artifact);

        // Download the binary
        let binary_data = self
            .github
            .download_release_asset(version, &artifact)
            .await
            .context("Failed to download binary")?;

        if binary_data.is_empty() {
            anyhow::bail!("Downloaded binary is empty");
        }

        println!("nozzleup: Downloaded {} bytes", binary_data.len());

        // Verify SHA256 checksum unless skipped
        if !skip_verification {
            self.verify_checksum(version, &artifact, &binary_data)
                .await?;
        } else {
            println!("nozzleup: Warning: Skipping SHA256 verification");
        }

        // Install the binary
        self.install_binary(version, &binary_data)?;

        Ok(())
    }

    /// Verify SHA256 checksum
    async fn verify_checksum(&self, version: &str, artifact_name: &str, data: &[u8]) -> Result<()> {
        println!("nozzleup: Verifying SHA256 checksum...");

        match self.github.download_checksum(version, artifact_name).await {
            Ok(expected_hash) => {
                // Calculate actual hash
                let mut hasher = Sha256::new();
                hasher.update(data);
                let actual_hash = format!("{:x}", hasher.finalize());

                if expected_hash.to_lowercase() != actual_hash.to_lowercase() {
                    anyhow::bail!(
                        "SHA256 verification failed!\nExpected: {}\nActual:   {}",
                        expected_hash,
                        actual_hash
                    );
                }

                println!("nozzleup: SHA256 verification successful");
                Ok(())
            }
            Err(e) => {
                println!("nozzleup: Warning: Could not download checksum file: {}", e);
                println!("nozzleup: Skipping verification");
                Ok(())
            }
        }
    }

    /// Install the binary to the version directory
    fn install_binary(&self, version: &str, data: &[u8]) -> Result<()> {
        // Create version directory
        let version_dir = self.config.versions_dir.join(version);
        fs_err::create_dir_all(&version_dir).context("Failed to create version directory")?;

        let binary_path = version_dir.join("nozzle");

        // Write binary
        fs_err::write(&binary_path, data).context("Failed to write binary")?;

        // Make executable
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = fs_err::metadata(&binary_path)
                .context("Failed to get binary metadata")?
                .permissions();
            perms.set_mode(0o755);
            fs_err::set_permissions(&binary_path, perms)
                .context("Failed to set executable permissions")?;
        }

        // Create symlink
        let active_path = self.config.active_binary_path();

        // Remove existing symlink if it exists
        if active_path.exists() || active_path.is_symlink() {
            fs_err::remove_file(&active_path).context("Failed to remove existing symlink")?;
        }

        // Create new symlink
        symlink(&binary_path, &active_path).context("Failed to create symlink")?;

        // Update current version
        self.config.set_current_version(version)?;

        Ok(())
    }
}

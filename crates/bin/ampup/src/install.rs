use anyhow::{Context, Result};
use fs_err as fs;

use crate::{
    github::GitHubClient,
    platform::{Architecture, Platform},
    ui,
    version_manager::VersionManager,
};

#[derive(Debug)]
pub enum InstallError {
    EmptyBinary { version: String },
}

impl std::fmt::Display for InstallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyBinary { version } => {
                writeln!(f, "Downloaded binary is empty")?;
                writeln!(f, "  Version: {}", version)?;
                writeln!(f)?;
                writeln!(
                    f,
                    "  The release asset was downloaded but contains no data."
                )?;
                writeln!(
                    f,
                    "  This may indicate a problem with the release packaging."
                )?;
                writeln!(
                    f,
                    "  Try downloading a different version or report this issue."
                )?;
            }
        }
        Ok(())
    }
}

impl std::error::Error for InstallError {}

pub struct Installer {
    version_manager: VersionManager,
    github: GitHubClient,
}

impl Installer {
    pub fn new(version_manager: VersionManager, github: GitHubClient) -> Self {
        Self {
            version_manager,
            github,
        }
    }

    /// Install ampd and ampctl from a GitHub release
    pub async fn install_from_release(
        &self,
        version: &str,
        platform: Platform,
        arch: Architecture,
    ) -> Result<()> {
        self.version_manager.config().ensure_dirs()?;

        // Download and install ampd
        let ampd_artifact = format!("ampd-{}-{}", platform.as_str(), arch.as_str());
        ui::info!("Downloading {} for {}", ui::version(version), ampd_artifact);

        let ampd_data = self
            .github
            .download_release_asset(version, &ampd_artifact)
            .await?;

        if ampd_data.is_empty() {
            return Err(InstallError::EmptyBinary {
                version: version.to_string(),
            }
            .into());
        }

        ui::detail!("Downloaded {} bytes for ampd", ampd_data.len());

        // Download and install ampctl
        let ampctl_artifact = format!("ampctl-{}-{}", platform.as_str(), arch.as_str());
        ui::info!(
            "Downloading {} for {}",
            ui::version(version),
            ampctl_artifact
        );

        let ampctl_data = self
            .github
            .download_release_asset(version, &ampctl_artifact)
            .await?;

        if ampctl_data.is_empty() {
            return Err(InstallError::EmptyBinary {
                version: version.to_string(),
            }
            .into());
        }

        ui::detail!("Downloaded {} bytes for ampctl", ampctl_data.len());

        // Install both binaries
        self.install_binaries(version, &ampd_data, &ampctl_data)?;

        Ok(())
    }

    /// Install both ampd and ampctl binaries to the version directory
    fn install_binaries(&self, version: &str, ampd_data: &[u8], ampctl_data: &[u8]) -> Result<()> {
        let config = self.version_manager.config();

        // Create version directory
        let version_dir = config.versions_dir.join(version);
        fs::create_dir_all(&version_dir).context("Failed to create version directory")?;

        // Install ampd
        let ampd_path = version_dir.join("ampd");
        fs::write(&ampd_path, ampd_data).context("Failed to write ampd binary")?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = fs::metadata(&ampd_path)
                .context("Failed to get ampd metadata")?
                .permissions();
            perms.set_mode(0o755);
            fs::set_permissions(&ampd_path, perms)
                .context("Failed to set executable permissions on ampd")?;
        }

        // Install ampctl
        let ampctl_path = version_dir.join("ampctl");
        fs::write(&ampctl_path, ampctl_data).context("Failed to write ampctl binary")?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = fs::metadata(&ampctl_path)
                .context("Failed to get ampctl metadata")?
                .permissions();
            perms.set_mode(0o755);
            fs::set_permissions(&ampctl_path, perms)
                .context("Failed to set executable permissions on ampctl")?;
        }

        // Activate this version using the version manager
        self.version_manager.activate(version)?;

        Ok(())
    }
}

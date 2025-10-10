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

    /// Install ampd from a GitHub release
    pub async fn install_from_release(
        &self,
        version: &str,
        platform: Platform,
        arch: Architecture,
    ) -> Result<()> {
        self.version_manager.config().ensure_dirs()?;

        let artifact = format!("ampd-{}-{}", platform.as_str(), arch.as_str());
        ui::info!("Downloading {} for {}", ui::version(version), artifact);

        // Download the binary over HTTPS (TLS provides integrity verification)
        let binary_data = self
            .github
            .download_release_asset(version, &artifact)
            .await?;

        if binary_data.is_empty() {
            return Err(InstallError::EmptyBinary {
                version: version.to_string(),
            }
            .into());
        }

        ui::detail!("Downloaded {} bytes", binary_data.len());

        // Install the binary
        self.install_binary(version, &binary_data)?;

        Ok(())
    }

    /// Install the binary to the version directory
    fn install_binary(&self, version: &str, data: &[u8]) -> Result<()> {
        let config = self.version_manager.config();

        // Create version directory
        let version_dir = config.versions_dir.join(version);
        fs::create_dir_all(&version_dir).context("Failed to create version directory")?;

        let binary_path = version_dir.join("ampd");

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

        // Activate this version using the version manager
        self.version_manager.activate(version)?;

        Ok(())
    }
}

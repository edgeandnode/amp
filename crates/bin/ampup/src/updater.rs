use anyhow::{Context, Result};
use fs_err as fs;

use crate::{
    github::GitHubClient,
    platform::{Architecture, Platform},
    ui,
};

/// Handles self-updating of ampup
pub struct Updater {
    github: GitHubClient,
}

impl Updater {
    /// Create a new updater
    pub fn new(github: GitHubClient) -> Self {
        Self { github }
    }

    /// Get the current version
    pub fn get_current_version(&self) -> String {
        env!("VERGEN_GIT_DESCRIBE").to_string()
    }

    /// Get the latest version
    pub async fn get_latest_version(&self) -> Result<String> {
        self.github.get_latest_version().await
    }

    /// Update ampup to a specific version
    pub async fn update_self(&self, version: &str) -> Result<()> {
        // Detect platform and architecture
        let platform = Platform::detect()?;
        let arch = Architecture::detect()?;

        // Download the ampup binary
        let artifact_name = format!("ampup-{}-{}", platform.as_str(), arch.as_str());
        ui::info!("Downloading {}", artifact_name);

        let binary_data = self
            .github
            .download_release_asset(version, &artifact_name)
            .await
            .context("Failed to download ampup binary")?;

        // Get the current executable path
        let current_exe =
            std::env::current_exe().context("Failed to get current executable path")?;

        // Write to a temporary file first
        let temp_path = current_exe.with_extension("tmp");
        fs::write(&temp_path, &binary_data).context("Failed to write temporary file")?;

        // Make it executable
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = fs::metadata(&temp_path)
                .context("Failed to get temp file metadata")?
                .permissions();
            perms.set_mode(0o755);
            fs::set_permissions(&temp_path, perms)
                .context("Failed to set executable permissions")?;
        }

        // Replace the current executable
        fs::rename(&temp_path, &current_exe).context("Failed to replace executable")?;

        ui::success!("Updated to {}", ui::version(version));

        Ok(())
    }
}

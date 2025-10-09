use std::os::unix::fs::symlink;

use anyhow::{Context, Result};
use fs_err as fs;

use crate::config::Config;

/// Version management errors
#[derive(Debug)]
pub enum VersionError {
    NotInstalled { version: String },
    NoVersionsInstalled,
    BinaryNotFound { version: String },
}

impl std::fmt::Display for VersionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotInstalled { version } => {
                writeln!(f, "Version not installed")?;
                writeln!(f, "  Version: {}", version)?;
                writeln!(f)?;
                writeln!(f, "  Try: nozzleup install {}", version)?;
            }
            Self::NoVersionsInstalled => {
                writeln!(f, "No versions installed")?;
                writeln!(f)?;
                writeln!(f, "  Try: nozzleup install")?;
            }
            Self::BinaryNotFound { version } => {
                writeln!(f, "Binary not found")?;
                writeln!(f, "  Version: {}", version)?;
                writeln!(f)?;
                writeln!(f, "  Installation may be corrupted.")?;
                writeln!(f, "  Try: nozzleup install {}", version)?;
            }
        }
        Ok(())
    }
}

impl std::error::Error for VersionError {}

/// Manages installed nozzle versions
pub struct VersionManager {
    config: Config,
}

impl VersionManager {
    /// Create a new version manager
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    /// Get the configuration
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// List all installed versions, sorted alphabetically
    pub fn list_installed(&self) -> Result<Vec<String>> {
        if !self.config.versions_dir.exists() {
            return Ok(Vec::new());
        }

        let mut versions = Vec::new();
        for entry in
            fs::read_dir(&self.config.versions_dir).context("Failed to read versions directory")?
        {
            let entry = entry.context("Failed to read directory entry")?;
            if entry
                .file_type()
                .context("Failed to get file type")?
                .is_dir()
            {
                let version = entry.file_name().to_string_lossy().to_string();
                versions.push(version);
            }
        }

        versions.sort();
        Ok(versions)
    }

    /// Get the currently active version
    pub fn get_current(&self) -> Result<Option<String>> {
        self.config.current_version()
    }

    /// Check if a version is installed
    pub fn is_installed(&self, version: &str) -> bool {
        self.config.version_binary_path(version).exists()
    }

    /// Activate a specific version by creating symlink and updating version file
    pub fn activate(&self, version: &str) -> Result<()> {
        let version_dir = self.config.versions_dir.join(version);
        if !version_dir.exists() {
            return Err(VersionError::NotInstalled {
                version: version.to_string(),
            }
            .into());
        }

        let binary_path = self.config.version_binary_path(version);
        if !binary_path.exists() {
            return Err(VersionError::BinaryNotFound {
                version: version.to_string(),
            }
            .into());
        }

        let active_path = self.config.active_binary_path();

        // Remove existing symlink if it exists
        if active_path.exists() || active_path.is_symlink() {
            fs::remove_file(&active_path).context("Failed to remove existing symlink")?;
        }

        // Create new symlink
        symlink(&binary_path, &active_path).context("Failed to create symlink")?;

        // Update current version file
        self.config.set_current_version(version)?;

        Ok(())
    }

    /// Uninstall a specific version
    pub fn uninstall(&self, version: &str) -> Result<()> {
        let version_dir = self.config.versions_dir.join(version);
        if !version_dir.exists() {
            return Err(VersionError::NotInstalled {
                version: version.to_string(),
            }
            .into());
        }

        // Check if this is the current version
        let current = self.get_current()?;
        let is_current = current.as_deref() == Some(version);

        // Remove the version directory
        fs::remove_dir_all(&version_dir).context("Failed to remove version directory")?;

        // If this was the current version, clear the current version file and symlink
        if is_current {
            let current_file = self.config.current_version_file();
            if current_file.exists() {
                fs::remove_file(&current_file).context("Failed to remove current version file")?;
            }

            // Remove the symlink
            let active_path = self.config.active_binary_path();
            if active_path.exists() || active_path.is_symlink() {
                fs::remove_file(&active_path).context("Failed to remove symlink")?;
            }
        }

        Ok(())
    }
}

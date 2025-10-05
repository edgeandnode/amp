use std::path::PathBuf;

use anyhow::{Context, Result};

/// Configuration for nozzleup
pub struct Config {
    /// Base directory for nozzle installation (~/.nozzle)
    pub nozzle_dir: PathBuf,
    /// Binary directory (~/.nozzle/bin)
    pub bin_dir: PathBuf,
    /// Versions directory (~/.nozzle/versions)
    pub versions_dir: PathBuf,
    /// GitHub repository (owner/repo)
    pub repo: String,
    /// GitHub token for private repository access
    pub github_token: Option<String>,
}

impl Config {
    /// Create a new configuration
    pub fn new() -> Result<Self> {
        let home = std::env::var("HOME")
            .or_else(|_| std::env::var("USERPROFILE"))
            .context("Could not determine home directory")?;

        let base = std::env::var("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(&home));

        let nozzle_dir = std::env::var("NOZZLE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| base.join(".nozzle"));

        let bin_dir = nozzle_dir.join("bin");
        let versions_dir = nozzle_dir.join("versions");

        let repo = std::env::var("NOZZLE_REPO")
            .unwrap_or_else(|_| "edgeandnode/project-nozzle".to_string());

        let github_token = std::env::var("GITHUB_TOKEN").ok();

        Ok(Self {
            nozzle_dir,
            bin_dir,
            versions_dir,
            repo,
            github_token,
        })
    }

    /// Get the path to the current version file
    pub fn current_version_file(&self) -> PathBuf {
        self.nozzle_dir.join(".version")
    }

    /// Get the currently installed version
    pub fn current_version(&self) -> Result<Option<String>> {
        let version_file = self.current_version_file();
        if !version_file.exists() {
            return Ok(None);
        }

        let version = fs_err::read_to_string(&version_file)
            .context("Failed to read current version file")?
            .trim()
            .to_string();

        Ok(Some(version))
    }

    /// Set the current version
    pub fn set_current_version(&self, version: &str) -> Result<()> {
        fs_err::create_dir_all(&self.nozzle_dir).context("Failed to create nozzle directory")?;
        fs_err::write(self.current_version_file(), version)
            .context("Failed to write current version file")?;
        Ok(())
    }

    /// Get the binary path for a specific version
    pub fn version_binary_path(&self, version: &str) -> PathBuf {
        self.versions_dir.join(version).join("nozzle")
    }

    /// Get the active nozzle binary symlink path
    pub fn active_binary_path(&self) -> PathBuf {
        self.bin_dir.join("nozzle")
    }

    /// Ensure all required directories exist
    pub fn ensure_dirs(&self) -> Result<()> {
        fs_err::create_dir_all(&self.nozzle_dir).context("Failed to create nozzle directory")?;
        fs_err::create_dir_all(&self.bin_dir).context("Failed to create bin directory")?;
        fs_err::create_dir_all(&self.versions_dir)
            .context("Failed to create versions directory")?;
        Ok(())
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::new().expect("Failed to create default config")
    }
}

use std::path::PathBuf;

use anyhow::{Context, Result};
use fs_err as fs;

/// Configuration for ampup
pub struct Config {
    /// Base directory for amp installation (~/.amp)
    pub amp_dir: PathBuf,
    /// Binary directory (~/.amp/bin)
    pub bin_dir: PathBuf,
    /// Versions directory (~/.amp/versions)
    pub versions_dir: PathBuf,
}

impl Config {
    /// Create a new configuration
    pub fn new(install_dir: Option<PathBuf>) -> Result<Self> {
        let amp_dir = if let Some(dir) = install_dir {
            dir
        } else {
            let home = std::env::var("HOME")
                .or_else(|_| std::env::var("USERPROFILE"))
                .context("Could not determine home directory")?;

            let base = std::env::var("XDG_CONFIG_HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from(&home));

            base.join(".amp")
        };

        let bin_dir = amp_dir.join("bin");
        let versions_dir = amp_dir.join("versions");

        Ok(Self {
            amp_dir,
            bin_dir,
            versions_dir,
        })
    }

    /// Get the path to the current version file
    pub fn current_version_file(&self) -> PathBuf {
        self.amp_dir.join(".version")
    }

    /// Get the currently installed version
    pub fn current_version(&self) -> Result<Option<String>> {
        let version_file = self.current_version_file();
        if !version_file.exists() {
            return Ok(None);
        }

        let version = fs::read_to_string(&version_file)
            .context("Failed to read current version file")?
            .trim()
            .to_string();

        Ok(Some(version))
    }

    /// Set the current version
    pub fn set_current_version(&self, version: &str) -> Result<()> {
        fs::create_dir_all(&self.amp_dir).context("Failed to create amp directory")?;
        fs::write(self.current_version_file(), version)
            .context("Failed to write current version file")?;
        Ok(())
    }

    /// Get the path to the ampup binary
    pub fn ampup_binary_path(&self) -> PathBuf {
        self.bin_dir.join("ampup")
    }

    /// Get the binary path for a specific version
    pub fn version_binary_path(&self, version: &str) -> PathBuf {
        self.versions_dir.join(version).join("ampd")
    }

    /// Get the active ampd binary symlink path
    pub fn active_binary_path(&self) -> PathBuf {
        self.bin_dir.join("ampd")
    }

    /// Get the ampctl binary path for a specific version
    pub fn version_ampctl_path(&self, version: &str) -> PathBuf {
        self.versions_dir.join(version).join("ampctl")
    }

    /// Get the active ampctl binary symlink path
    pub fn active_ampctl_path(&self) -> PathBuf {
        self.bin_dir.join("ampctl")
    }

    /// Ensure all required directories exist
    pub fn ensure_dirs(&self) -> Result<()> {
        fs::create_dir_all(&self.amp_dir).context("Failed to create amp directory")?;
        fs::create_dir_all(&self.bin_dir).context("Failed to create bin directory")?;
        fs::create_dir_all(&self.versions_dir).context("Failed to create versions directory")?;
        Ok(())
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::new(None).expect("Failed to create default config")
    }
}

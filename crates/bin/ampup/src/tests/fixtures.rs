use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use fs_err as fs;
use tempfile::TempDir;

/// Temporary ampup installation directory for testing.
///
/// Creates an isolated `.amp` directory structure that is automatically
/// cleaned up when dropped (unless TESTS_KEEP_TEMP_DIRS=1 is set).
pub struct TempInstallDir {
    _temp_dir: TempDir,
    amp_dir: PathBuf,
}

impl TempInstallDir {
    /// Create a new temporary installation directory.
    pub fn new() -> Result<Self> {
        let temp_dir = TempDir::new().context("Failed to create temporary directory")?;
        let amp_dir = temp_dir.path().to_path_buf();

        // Create the basic directory structure
        let bin_dir = amp_dir.join("bin");
        let versions_dir = amp_dir.join("versions");

        fs::create_dir_all(&bin_dir).context("Failed to create bin directory")?;
        fs::create_dir_all(&versions_dir).context("Failed to create versions directory")?;

        Ok(Self {
            _temp_dir: temp_dir,
            amp_dir,
        })
    }

    /// Get the path to the temporary amp directory.
    pub fn path(&self) -> &Path {
        &self.amp_dir
    }

    /// Get the path to the bin directory.
    pub fn bin_dir(&self) -> PathBuf {
        self.amp_dir.join("bin")
    }

    /// Get the path to the versions directory.
    pub fn versions_dir(&self) -> PathBuf {
        self.amp_dir.join("versions")
    }

    /// Get the path to the ampup binary.
    pub fn ampup_binary(&self) -> PathBuf {
        self.bin_dir().join("ampup")
    }

    /// Get the path to the active amp binary symlink.
    pub fn active_binary(&self) -> PathBuf {
        self.bin_dir().join("ampd")
    }

    /// Get the path to the current version file.
    pub fn current_version_file(&self) -> PathBuf {
        self.amp_dir.join(".version")
    }

    /// Get the path to a specific version directory.
    pub fn version_dir(&self, version: &str) -> PathBuf {
        self.versions_dir().join(version)
    }

    /// Get the path to a specific version binary.
    pub fn version_binary(&self, version: &str) -> PathBuf {
        self.version_dir(version).join("ampd")
    }
}

/// Helper for creating mock ampd and ampctl binaries for testing.
pub struct MockBinary;

impl MockBinary {
    /// Create mock ampd and ampctl binaries for a specific version.
    pub fn create(temp: &TempInstallDir, version: &str) -> Result<()> {
        let version_dir = temp.version_dir(version);
        fs::create_dir_all(&version_dir)
            .with_context(|| format!("Failed to create version directory for {}", version))?;

        // Create mock ampd binary
        let ampd_path = version_dir.join("ampd");
        let ampd_script = format!("#!/bin/sh\necho 'ampd {}'", version);
        fs::write(&ampd_path, ampd_script)
            .with_context(|| format!("Failed to write mock ampd binary for {}", version))?;

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

        // Create mock ampctl binary
        let ampctl_path = version_dir.join("ampctl");
        let ampctl_script = format!("#!/bin/sh\necho 'ampctl {}'", version);
        fs::write(&ampctl_path, ampctl_script)
            .with_context(|| format!("Failed to write mock ampctl binary for {}", version))?;

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

        Ok(())
    }
}

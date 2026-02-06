//! Git helper functions for test environments.
//!
//! This module provides convenience functions for Git operations needed
//! in test scenarios, such as initializing submodules for Foundry contracts.

use std::{path::Path, process::Stdio};

use anyhow::{Result, anyhow};

/// Initialize git submodules in the specified directory.
///
/// Runs `git submodule update --init --recursive` to initialize submodules
/// such as forge-std for Foundry contracts.
pub async fn submodules_init(dir: &Path) -> Result<()> {
    tracing::debug!(
        dir = %dir.display(),
        "Initializing git submodules"
    );

    let output = tokio::process::Command::new("git")
        .args(["submodule", "update", "--init", "--recursive"])
        .current_dir(dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow!(
            "Failed to initialize git submodules in '{}': {}",
            dir.display(),
            stderr.trim()
        ));
    }

    tracing::debug!(
        dir = %dir.display(),
        "Git submodules initialized successfully"
    );

    Ok(())
}

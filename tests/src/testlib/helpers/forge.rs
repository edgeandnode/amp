//! Forge (Foundry) helper functions for test environments.
//!
//! This module provides convenience functions for compiling Solidity smart
//! contracts using Foundry's forge tool.

use std::{
    path::{Path, PathBuf},
    process::Stdio,
};

use anyhow::{Result, anyhow};

/// Compile Solidity contracts using Foundry's forge build.
///
/// Runs `forge build` in the specified contracts directory and returns paths
/// to the build output directories. Requires Foundry installed and git
/// submodules initialized (use `git::init_submodules` first).
pub async fn build(contracts_dir: &Path) -> Result<ForgeBuildOutput> {
    tracing::debug!(
        dir = %contracts_dir.display(),
        "Compiling Solidity contracts with forge build"
    );

    let output = tokio::process::Command::new("forge")
        .args(["build"])
        .current_dir(contracts_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Err(anyhow!(
            "Failed to compile contracts in '{}': {}\n{}",
            contracts_dir.display(),
            stderr.trim(),
            stdout.trim()
        ));
    }

    let out_dir = contracts_dir.join("out");
    let cache_dir = contracts_dir.join("cache");

    tracing::debug!(
        dir = %contracts_dir.display(),
        out_dir = %out_dir.display(),
        "Contracts compiled successfully"
    );

    Ok(ForgeBuildOutput { out_dir, cache_dir })
}

/// Clean compiled contract artifacts.
///
/// Runs `forge clean` to remove the `out/` and `cache/` directories,
/// forcing a fresh compilation on the next build.
pub async fn clean(contracts_dir: &Path) -> Result<()> {
    tracing::debug!(
        dir = %contracts_dir.display(),
        "Cleaning forge build artifacts"
    );

    let output = tokio::process::Command::new("forge")
        .args(["clean"])
        .current_dir(contracts_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow!(
            "Failed to clean contracts in '{}': {}",
            contracts_dir.display(),
            stderr.trim()
        ));
    }

    Ok(())
}

/// Output paths from a successful forge build.
#[derive(Debug, Clone)]
pub struct ForgeBuildOutput {
    /// Path to the `out/` directory containing compiled contract artifacts.
    ///
    /// Contract artifacts are organized as `out/<contract_file>/<contract_name>.json`.
    /// For example, `Counter.sol` produces `out/Counter.sol/Counter.json`.
    pub out_dir: PathBuf,

    /// Path to the `cache/` directory containing forge build cache.
    pub cache_dir: PathBuf,
}

impl ForgeBuildOutput {
    /// Get the path to a contract artifact JSON file.
    ///
    /// Constructs the path as `out/<contract_name>.sol/<contract_name>.json`.
    /// For example: `artifact_path("Counter")` returns `out/Counter.sol/Counter.json`.
    pub fn artifact_path(&self, contract_name: &str) -> PathBuf {
        self.out_dir
            .join(format!("{}.sol", contract_name))
            .join(format!("{}.json", contract_name))
    }
}

//! Test configuration fixtures and resource loading utilities.
//!
//! This module provides shared utilities for loading test fixtures from
//! predefined source directories. It includes:
//! - Constants defining fixture source directories
//! - Helper functions for resolving and copying fixture files/directories
//!
//! These utilities are used by various test fixtures to preload resources
//! like dataset manifests, provider configurations, and dataset snapshots.
//!
//! # Test Resource Preloading
//!
//! This module supports preloading test resources from predefined source directories:
//!
//! ## Dataset Manifests (.json files)
//! **Source directories searched (in order):**
//! - `tests/config/manifests/`
//! - `config/manifests/`
//!
//! Use `resolve_dataset_manifest_source_file()` to find manifest files, or use the
//! `read_manifest_fixture()` function to read manifest content directly.
//!
//! ## Provider Configurations (.toml files)
//! **Source directories searched (in order):**
//! - `tests/config/providers/`
//! - `config/providers/`
//!
//! Use `resolve_provider_config_source_file()` to find provider config files.
//!
//! ## Dataset Snapshot Reference Data (directory trees)
//! **Source directories searched (in order):**
//! - `tests/config/snapshots/`
//! - `config/snapshots/`
//!
//! Use `resolve_snapshot_source_dir()` to find dataset snapshot directories.
//!
//! ## Dataset Packages (directory trees)
//! **Source directories searched (in order):**
//! - `tests/config/packages/`
//! - `config/packages/`
//!
//! Use `resolve_package_source_dir()` to find dataset package directories.
//!
//! # Fixture Resolution Process
//!
//! When resolving fixtures, this module uses a search algorithm:
//! 1. **Search in order**: Each source directory is checked sequentially
//! 2. **First match wins**: The first existing file/directory is used
//! 3. **Canonicalization**: Paths are resolved to absolute, canonical paths
//! 4. **Type verification**: Files must exist as files, directories as directories
//! 5. **Error on missing**: If no match is found, `None` is returned
//!
//! This allows tests to work with different project layouts while maintaining
//! consistent resource loading behavior.

use std::path::{Path, PathBuf};

use anyhow::{Result, anyhow};

/// Source directories to search for dataset manifests
const DATASET_MANIFESTS_FIXTURE_DIRS: [&str; 2] = ["tests/config/manifests", "config/manifests"];

/// Source directories to search for provider configs
const PROVIDERS_FIXTURE_DIRS: [&str; 2] = ["tests/config/providers", "config/providers"];

/// Source directories to search for dataset snapshot data
const SNAPSHOTS_FIXTURE_DIRS: [&str; 2] = ["tests/config/snapshots", "config/snapshots"];

/// Source directories to search for dataset packages
const PACKAGES_FIXTURE_DIRS: [&str; 2] = ["tests/config/packages", "config/packages"];

/// Resolve dataset manifest source file from fixture directories.
///
/// Searches the predefined dataset manifest fixture directories for the specified file.
pub(super) fn resolve_dataset_manifest_source_file(name: &Path) -> Option<PathBuf> {
    resolve_fixture_source_file(&DATASET_MANIFESTS_FIXTURE_DIRS, name)
}

/// Resolve provider config source file from fixture directories.
///
/// Searches the predefined provider config fixture directories for the specified file.
pub(super) fn resolve_provider_config_source_file(name: &Path) -> Option<PathBuf> {
    resolve_fixture_source_file(&PROVIDERS_FIXTURE_DIRS, name)
}

/// Resolve dataset snapshot source directory from fixture directories.
///
/// Searches the predefined snapshot fixture directories for the specified directory.
pub(super) fn resolve_snapshot_source_dir(name: &Path) -> Option<PathBuf> {
    resolve_fixture_source_dir(&SNAPSHOTS_FIXTURE_DIRS, name)
}

/// Resolve dataset package source directory from fixture directories.
///
/// Searches the predefined package fixture directories for the specified package.
pub(super) fn resolve_package_source_dir(name: &Path) -> Option<PathBuf> {
    resolve_fixture_source_dir(&PACKAGES_FIXTURE_DIRS, name)
}

/// Resolves the absolute path to a file by searching through known fixture directories.
///
/// Searches through the provided fixture directories in order, looking for the specified file.
/// Returns the first canonicalized absolute path where the file exists as a regular file.
fn resolve_fixture_source_file(fixture_dirs: &[&str], name: &Path) -> Option<PathBuf> {
    fixture_dirs
        .iter()
        .map(|dir| Path::new(dir).join(name))
        .filter_map(|file_path| file_path.canonicalize().ok())
        .find(|dir| dir.is_file())
}

/// Resolves the absolute path to a directory by searching through known fixture directories.
///
/// Searches through the provided fixture directories in order, looking for the specified directory.
/// Returns the first canonicalized absolute path where the directory exists as a directory.
fn resolve_fixture_source_dir(fixture_dirs: &[&str], name: &Path) -> Option<PathBuf> {
    fixture_dirs
        .iter()
        .map(|dir| Path::new(dir).join(name))
        .filter_map(|dir_path| dir_path.canonicalize().ok())
        .find(|dir| dir.is_dir())
}

/// Read a dataset manifest from the fixture directories.
///
/// This function reads a manifest file from the predefined fixture directories
/// without copying it to any daemon state directory. It's useful for reading
/// manifest content to send to the Admin API for registration.
///
/// The `manifest_name` parameter should be a filename stem (without extension).
/// For example, passing `"eth_rpc"` will read `eth_rpc.json` from the fixture directories.
///
/// Returns the manifest content as a String.
///
/// # Errors
///
/// Returns an error if:
/// - The manifest file cannot be found in any of the predefined fixture directories
/// - The file cannot be read
pub(super) async fn read_manifest_fixture(manifest_name: &str) -> Result<String> {
    let mut path = PathBuf::from(manifest_name);
    path.set_extension("json");

    // Resolve source directory by searching known fixture locations
    let source_file_path = resolve_dataset_manifest_source_file(&path).ok_or_else(|| {
        anyhow!(
            "Could not find dataset manifest fixture '{}'",
            manifest_name
        )
    })?;

    tracing::debug!("Reading manifest fixture: {}", source_file_path.display());

    // Read and return the manifest content
    let content = tokio::fs::read_to_string(&source_file_path)
        .await
        .map_err(|err| {
            anyhow!(
                "Failed to read manifest fixture '{}': {}",
                source_file_path.display(),
                err
            )
        })?;

    Ok(content)
}

/// Read a provider configuration fixture from the predefined fixture directories.
///
/// This function searches for provider configuration files (`.toml`) in the predefined
/// fixture directories and returns the content as a String.
///
/// Returns the provider configuration content as a String.
///
/// # Errors
///
/// Returns an error if:
/// - The provider file cannot be found in any of the predefined fixture directories
/// - The file cannot be read
pub(super) async fn read_provider_fixture(provider_name: &str) -> Result<String> {
    let mut path = PathBuf::from(provider_name);
    path.set_extension("toml");

    // Resolve source directory by searching known fixture locations
    let source_file_path = resolve_provider_config_source_file(&path)
        .ok_or_else(|| anyhow!("Could not find provider config fixture '{}'", provider_name))?;

    tracing::debug!("Reading provider fixture: {}", source_file_path.display());

    // Read and return the provider content
    let content = tokio::fs::read_to_string(&source_file_path)
        .await
        .map_err(|err| {
            anyhow!(
                "Failed to read provider fixture '{}': {}",
                source_file_path.display(),
                err
            )
        })?;

    Ok(content)
}

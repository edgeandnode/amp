//! Tracing utilities for dataset handlers

use datasets_common::version::Version;

/// Format a dataset selector version for display purposes
///
/// Returns "latest" if version is None, otherwise returns the version string.
/// This is useful for logging and error messages where we want to be explicit
/// about using the latest version rather than showing None.
pub fn display_selector_version(version: &Option<Version>) -> String {
    version
        .as_ref()
        .map(|v| v.to_string())
        .unwrap_or_else(|| "latest".to_string())
}

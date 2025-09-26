//! Unified value representation for dataset manifests.
//!
//! This module provides the [`ManifestValue`] enum which abstracts over different
//! configuration formats (TOML and JSON) used in dataset manifests. This allows
//! dataset extractors and stores to work with configuration data regardless of
//! its original format.

/// A value from a dataset manifest that can be either TOML or JSON format.
///
/// This enum provides a unified interface for handling configuration values
/// from dataset manifests, allowing the same parsing logic to work with both
/// TOML and JSON sources.
///
/// `ManifestValue` is primarily used in dataset extractors and stores where
/// configuration needs to be parsed from manifest files. Each extractor
/// typically implements a `from_value` method that can handle both formats.
///
/// # Format Support
///
/// - **TOML**: Uses the `toml` crate's `Value` type for structured TOML data
/// - **JSON**: Uses `serde_json`'s `Value` type for structured JSON data
///
/// Both formats can represent the same logical configuration structure, and
/// the choice between them is typically based on user preference or tooling
/// requirements.
pub enum ManifestValue {
    /// A TOML configuration value.
    ///
    /// Contains parsed TOML data as a `toml::Value`, which provides access
    /// to structured configuration data from TOML format manifests.
    Toml(toml::Value),

    /// A JSON configuration value.
    ///
    /// Contains parsed JSON data as a `serde_json::Value`, which provides
    /// access to structured configuration data from JSON format manifests.
    Json(serde_json::Value),
}

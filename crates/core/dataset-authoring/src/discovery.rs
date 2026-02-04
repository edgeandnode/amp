//! Model file discovery for dbt-style dataset authoring.
//!
//! This module provides functionality to scan a models directory and discover
//! all SQL model files, deriving table names from file stems.
//!
//! # Model Discovery Rules
//!
//! - Scans the models directory recursively for `**/*.sql` files
//! - Model name is derived from the file stem (e.g., `transfers.sql` → `transfers`)
//! - Model names must be valid [`TableName`]s (SQL identifier rules)
//! - Duplicate model names across subdirectories are rejected
//! - Non-SQL files are ignored

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use datasets_common::table_name::{TableName, TableNameError};
use walkdir::WalkDir;

/// Errors that can occur during model discovery.
#[derive(Debug, thiserror::Error)]
pub enum DiscoveryError {
    /// Duplicate model name found in different paths.
    #[error("duplicate model name '{name}': {path1} and {path2}")]
    DuplicateModelName {
        /// The conflicting model name.
        name: String,
        /// First path where the model was found.
        path1: PathBuf,
        /// Second path where the model was found.
        path2: PathBuf,
    },

    /// Invalid model name derived from file stem.
    #[error("invalid model name from file '{}'", path.display())]
    InvalidModelName {
        /// The file path with the invalid name.
        path: PathBuf,
        /// The underlying table name error.
        #[source]
        source: TableNameError,
    },

    /// I/O error reading directory.
    #[error("failed to read models directory")]
    IoError(#[source] std::io::Error),

    /// Models directory not found.
    #[error("models directory not found: {}", path.display())]
    DirectoryNotFound {
        /// The missing directory path.
        path: PathBuf,
    },
}

/// Discovered model information.
#[derive(Debug, Clone)]
pub struct DiscoveredModel {
    /// The validated table name derived from the file stem.
    pub name: TableName,
    /// Path to the SQL file relative to the base directory.
    pub path: PathBuf,
}

/// Discover model SQL files in the given models directory.
///
/// Scans the models directory recursively for `**/*.sql` files and returns
/// a map of table names to their file paths. The table name is derived from
/// the file stem (without extension).
///
/// # Arguments
///
/// * `base_dir` - The base directory containing the amp.yaml
/// * `models_rel` - The models directory path relative to base_dir
///
/// # Errors
///
/// Returns [`DiscoveryError`] if:
/// - The models directory does not exist
/// - An I/O error occurs while reading the directory
/// - A file stem cannot be parsed as a valid [`TableName`]
/// - Duplicate model names are found across different subdirectories
///
/// # Example
///
/// ```ignore
/// use dataset_authoring::discovery::discover_models;
/// use std::path::Path;
///
/// let models = discover_models(Path::new("."), Path::new("models"))?;
/// for (name, path) in &models {
///     println!("Found model {} at {}", name, path.display());
/// }
/// ```
pub fn discover_models(
    base_dir: &Path,
    models_rel: &Path,
) -> Result<BTreeMap<TableName, PathBuf>, DiscoveryError> {
    let models_dir = base_dir.join(models_rel);

    // Check if models directory exists
    if !models_dir.is_dir() {
        return Err(DiscoveryError::DirectoryNotFound { path: models_dir });
    }

    let mut models: BTreeMap<TableName, PathBuf> = BTreeMap::new();

    // Walk the directory recursively
    for entry in WalkDir::new(&models_dir)
        .follow_links(false)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        let path = entry.path();

        // Skip directories and non-SQL files
        if !path.is_file() {
            continue;
        }

        let extension = path.extension().and_then(|e| e.to_str());
        if extension != Some("sql") {
            continue;
        }

        // Get the file stem as the model name
        let file_stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| DiscoveryError::IoError(std::io::Error::other("invalid file name")))?;

        // Parse as TableName to validate
        let table_name: TableName =
            file_stem
                .parse()
                .map_err(|source| DiscoveryError::InvalidModelName {
                    path: path.to_path_buf(),
                    source,
                })?;

        // Compute relative path from base_dir (not models_dir)
        let relative_path = path
            .strip_prefix(base_dir)
            .map_err(|e| DiscoveryError::IoError(std::io::Error::other(e.to_string())))?
            .to_path_buf();

        // Check for duplicate names
        if let Some(existing_path) = models.get(&table_name) {
            return Err(DiscoveryError::DuplicateModelName {
                name: table_name.to_string(),
                path1: existing_path.clone(),
                path2: relative_path,
            });
        }

        models.insert(table_name, relative_path);
    }

    Ok(models)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::TempDir;

    use super::*;

    fn create_test_dir() -> TempDir {
        tempfile::tempdir().expect("failed to create temp dir")
    }

    fn create_file(dir: &Path, relative_path: &str, content: &str) {
        let path = dir.join(relative_path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("failed to create parent dirs");
        }
        fs::write(&path, content).expect("failed to write file");
    }

    #[test]
    fn discover_models_finds_sql_files() {
        //* Given
        let temp_dir = create_test_dir();
        let base = temp_dir.path();
        create_file(base, "models/transfers.sql", "SELECT 1");

        //* When
        let result = discover_models(base, Path::new("models"));

        //* Then
        let models = result.expect("should discover models");
        assert_eq!(models.len(), 1);
        assert!(models.contains_key(&"transfers".parse::<TableName>().unwrap()));
    }

    #[test]
    fn discover_models_recursive() {
        //* Given
        let temp_dir = create_test_dir();
        let base = temp_dir.path();
        create_file(base, "models/transfers.sql", "SELECT 1");
        create_file(base, "models/subdir/balances.sql", "SELECT 2");
        create_file(base, "models/deep/nested/events.sql", "SELECT 3");

        //* When
        let result = discover_models(base, Path::new("models"));

        //* Then
        let models = result.expect("should discover models recursively");
        assert_eq!(models.len(), 3);
        assert!(models.contains_key(&"transfers".parse::<TableName>().unwrap()));
        assert!(models.contains_key(&"balances".parse::<TableName>().unwrap()));
        assert!(models.contains_key(&"events".parse::<TableName>().unwrap()));
    }

    #[test]
    fn discover_models_rejects_duplicate_names() {
        //* Given
        let temp_dir = create_test_dir();
        let base = temp_dir.path();
        create_file(base, "models/transfers.sql", "SELECT 1");
        create_file(base, "models/subdir/transfers.sql", "SELECT 2");

        //* When
        let result = discover_models(base, Path::new("models"));

        //* Then
        let err = result.expect_err("should reject duplicate model names");
        assert!(
            matches!(err, DiscoveryError::DuplicateModelName { name, .. } if name == "transfers")
        );
    }

    #[test]
    fn discover_models_returns_sorted() {
        //* Given
        let temp_dir = create_test_dir();
        let base = temp_dir.path();
        create_file(base, "models/zebra.sql", "SELECT 1");
        create_file(base, "models/apple.sql", "SELECT 2");
        create_file(base, "models/mango.sql", "SELECT 3");

        //* When
        let result = discover_models(base, Path::new("models"));

        //* Then
        let models = result.expect("should discover models");
        let names: Vec<&str> = models.keys().map(|n| n.as_str()).collect();
        assert_eq!(names, vec!["apple", "mango", "zebra"]);
    }

    #[test]
    fn discover_models_rejects_invalid_names() {
        //* Given
        let temp_dir = create_test_dir();
        let base = temp_dir.path();
        create_file(base, "models/123invalid.sql", "SELECT 1");

        //* When
        let result = discover_models(base, Path::new("models"));

        //* Then
        let err = result.expect_err("should reject invalid model name");
        assert!(matches!(err, DiscoveryError::InvalidModelName { .. }));
    }

    #[test]
    fn discover_models_ignores_non_sql_files() {
        //* Given
        let temp_dir = create_test_dir();
        let base = temp_dir.path();
        create_file(base, "models/transfers.sql", "SELECT 1");
        create_file(base, "models/readme.md", "# Documentation");
        create_file(base, "models/notes.txt", "Some notes");
        create_file(base, "models/config.json", "{}");

        //* When
        let result = discover_models(base, Path::new("models"));

        //* Then
        let models = result.expect("should discover only SQL files");
        assert_eq!(models.len(), 1);
        assert!(models.contains_key(&"transfers".parse::<TableName>().unwrap()));
    }

    #[test]
    fn discover_models_fails_if_directory_not_found() {
        //* Given
        let temp_dir = create_test_dir();
        let base = temp_dir.path();
        // Don't create the models directory

        //* When
        let result = discover_models(base, Path::new("models"));

        //* Then
        let err = result.expect_err("should fail if directory not found");
        assert!(matches!(err, DiscoveryError::DirectoryNotFound { .. }));
    }

    #[test]
    fn discover_models_handles_empty_directory() {
        //* Given
        let temp_dir = create_test_dir();
        let base = temp_dir.path();
        fs::create_dir_all(base.join("models")).expect("failed to create models dir");

        //* When
        let result = discover_models(base, Path::new("models"));

        //* Then
        let models = result.expect("should handle empty directory");
        assert!(models.is_empty());
    }

    #[test]
    fn discover_models_returns_relative_paths_from_base() {
        //* Given
        let temp_dir = create_test_dir();
        let base = temp_dir.path();
        create_file(base, "models/transfers.sql", "SELECT 1");
        create_file(base, "models/subdir/balances.sql", "SELECT 2");

        //* When
        let result = discover_models(base, Path::new("models"));

        //* Then
        let models = result.expect("should discover models");
        let transfers_path = models
            .get(&"transfers".parse::<TableName>().unwrap())
            .unwrap();
        let balances_path = models
            .get(&"balances".parse::<TableName>().unwrap())
            .unwrap();

        // Paths should be relative to base_dir (include models/ prefix)
        assert_eq!(transfers_path, &PathBuf::from("models/transfers.sql"));
        assert_eq!(balances_path, &PathBuf::from("models/subdir/balances.sql"));
    }
}

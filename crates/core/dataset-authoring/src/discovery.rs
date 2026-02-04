//! Table file discovery for dbt-style dataset authoring.
//!
//! This module provides functionality to scan a tables directory and discover
//! all SQL table files, deriving table names from file stems.
//!
//! # Table Discovery Rules
//!
//! - Scans the tables directory recursively for `**/*.sql` files
//! - Table name is derived from the file stem (e.g., `transfers.sql` → `transfers`)
//! - Table names must be valid [`TableName`]s (SQL identifier rules)
//! - Duplicate table names across subdirectories are rejected
//! - Non-SQL files are ignored

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use datasets_common::table_name::{TableName, TableNameError};
use walkdir::WalkDir;

/// Errors that can occur during table discovery.
#[derive(Debug, thiserror::Error)]
pub enum DiscoveryError {
    /// Duplicate table name found in different paths.
    #[error("duplicate table name '{name}': {path1} and {path2}")]
    DuplicateTableName {
        /// The conflicting table name.
        name: String,
        /// First path where the table was found.
        path1: PathBuf,
        /// Second path where the table was found.
        path2: PathBuf,
    },

    /// Invalid table name derived from file stem.
    #[error("invalid table name from file '{}'", path.display())]
    InvalidTableName {
        /// The file path with the invalid name.
        path: PathBuf,
        /// The underlying table name error.
        #[source]
        source: TableNameError,
    },

    /// I/O error reading directory.
    #[error("failed to read tables directory")]
    IoError(#[source] std::io::Error),

    /// Tables directory not found.
    #[error("tables directory not found: {}", path.display())]
    DirectoryNotFound {
        /// The missing directory path.
        path: PathBuf,
    },
}

/// Discovered table information.
#[derive(Debug, Clone)]
pub struct DiscoveredTable {
    /// The validated table name derived from the file stem.
    pub name: TableName,
    /// Path to the SQL file relative to the base directory.
    pub path: PathBuf,
}

/// Discover table SQL files in the given tables directory.
///
/// Scans the tables directory recursively for `**/*.sql` files and returns
/// a map of table names to their file paths. The table name is derived from
/// the file stem (without extension).
///
/// # Arguments
///
/// * `base_dir` - The base directory containing the amp.yaml
/// * `tables_rel` - The tables directory path relative to base_dir
///
/// # Errors
///
/// Returns [`DiscoveryError`] if:
/// - The tables directory does not exist
/// - An I/O error occurs while reading the directory
/// - A file stem cannot be parsed as a valid [`TableName`]
/// - Duplicate table names are found across different subdirectories
///
/// # Example
///
/// ```ignore
/// use dataset_authoring::discovery::discover_tables;
/// use std::path::Path;
///
/// let tables = discover_tables(Path::new("."), Path::new("tables"))?;
/// for (name, path) in &tables {
///     println!("Found table {} at {}", name, path.display());
/// }
/// ```
pub fn discover_tables(
    base_dir: &Path,
    tables_rel: &Path,
) -> Result<BTreeMap<TableName, PathBuf>, DiscoveryError> {
    let tables_dir = base_dir.join(tables_rel);

    // Check if tables directory exists
    if !tables_dir.is_dir() {
        return Err(DiscoveryError::DirectoryNotFound { path: tables_dir });
    }

    let mut tables: BTreeMap<TableName, PathBuf> = BTreeMap::new();

    // Walk the directory recursively
    for entry in WalkDir::new(&tables_dir)
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

        // Get the file stem as the table name
        let file_stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| DiscoveryError::IoError(std::io::Error::other("invalid file name")))?;

        // Parse as TableName to validate
        let table_name: TableName =
            file_stem
                .parse()
                .map_err(|source| DiscoveryError::InvalidTableName {
                    path: path.to_path_buf(),
                    source,
                })?;

        // Compute relative path from base_dir (not tables_dir)
        let relative_path = path
            .strip_prefix(base_dir)
            .map_err(|e| DiscoveryError::IoError(std::io::Error::other(e.to_string())))?
            .to_path_buf();

        // Check for duplicate names
        if let Some(existing_path) = tables.get(&table_name) {
            return Err(DiscoveryError::DuplicateTableName {
                name: table_name.to_string(),
                path1: existing_path.clone(),
                path2: relative_path,
            });
        }

        tables.insert(table_name, relative_path);
    }

    Ok(tables)
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
    fn discover_tables_finds_sql_files() {
        //* Given
        let temp_dir = create_test_dir();
        let base = temp_dir.path();
        create_file(base, "tables/transfers.sql", "SELECT 1");

        //* When
        let result = discover_tables(base, Path::new("tables"));

        //* Then
        let tables = result.expect("should discover tables");
        assert_eq!(tables.len(), 1);
        assert!(tables.contains_key(&"transfers".parse::<TableName>().unwrap()));
    }

    #[test]
    fn discover_tables_recursive() {
        //* Given
        let temp_dir = create_test_dir();
        let base = temp_dir.path();
        create_file(base, "tables/transfers.sql", "SELECT 1");
        create_file(base, "tables/subdir/balances.sql", "SELECT 2");
        create_file(base, "tables/deep/nested/events.sql", "SELECT 3");

        //* When
        let result = discover_tables(base, Path::new("tables"));

        //* Then
        let tables = result.expect("should discover tables recursively");
        assert_eq!(tables.len(), 3);
        assert!(tables.contains_key(&"transfers".parse::<TableName>().unwrap()));
        assert!(tables.contains_key(&"balances".parse::<TableName>().unwrap()));
        assert!(tables.contains_key(&"events".parse::<TableName>().unwrap()));
    }

    #[test]
    fn discover_tables_rejects_duplicate_names() {
        //* Given
        let temp_dir = create_test_dir();
        let base = temp_dir.path();
        create_file(base, "tables/transfers.sql", "SELECT 1");
        create_file(base, "tables/subdir/transfers.sql", "SELECT 2");

        //* When
        let result = discover_tables(base, Path::new("tables"));

        //* Then
        let err = result.expect_err("should reject duplicate table names");
        assert!(
            matches!(err, DiscoveryError::DuplicateTableName { name, .. } if name == "transfers")
        );
    }

    #[test]
    fn discover_tables_returns_sorted() {
        //* Given
        let temp_dir = create_test_dir();
        let base = temp_dir.path();
        create_file(base, "tables/zebra.sql", "SELECT 1");
        create_file(base, "tables/apple.sql", "SELECT 2");
        create_file(base, "tables/mango.sql", "SELECT 3");

        //* When
        let result = discover_tables(base, Path::new("tables"));

        //* Then
        let tables = result.expect("should discover tables");
        let names: Vec<&str> = tables.keys().map(|n| n.as_str()).collect();
        assert_eq!(names, vec!["apple", "mango", "zebra"]);
    }

    #[test]
    fn discover_tables_rejects_invalid_names() {
        //* Given
        let temp_dir = create_test_dir();
        let base = temp_dir.path();
        create_file(base, "tables/123invalid.sql", "SELECT 1");

        //* When
        let result = discover_tables(base, Path::new("tables"));

        //* Then
        let err = result.expect_err("should reject invalid table name");
        assert!(matches!(err, DiscoveryError::InvalidTableName { .. }));
    }

    #[test]
    fn discover_tables_ignores_non_sql_files() {
        //* Given
        let temp_dir = create_test_dir();
        let base = temp_dir.path();
        create_file(base, "tables/transfers.sql", "SELECT 1");
        create_file(base, "tables/readme.md", "# Documentation");
        create_file(base, "tables/notes.txt", "Some notes");
        create_file(base, "tables/config.json", "{}");

        //* When
        let result = discover_tables(base, Path::new("tables"));

        //* Then
        let tables = result.expect("should discover only SQL files");
        assert_eq!(tables.len(), 1);
        assert!(tables.contains_key(&"transfers".parse::<TableName>().unwrap()));
    }

    #[test]
    fn discover_tables_fails_if_directory_not_found() {
        //* Given
        let temp_dir = create_test_dir();
        let base = temp_dir.path();
        // Don't create the tables directory

        //* When
        let result = discover_tables(base, Path::new("tables"));

        //* Then
        let err = result.expect_err("should fail if directory not found");
        assert!(matches!(err, DiscoveryError::DirectoryNotFound { .. }));
    }

    #[test]
    fn discover_tables_handles_empty_directory() {
        //* Given
        let temp_dir = create_test_dir();
        let base = temp_dir.path();
        fs::create_dir_all(base.join("tables")).expect("failed to create tables dir");

        //* When
        let result = discover_tables(base, Path::new("tables"));

        //* Then
        let tables = result.expect("should handle empty directory");
        assert!(tables.is_empty());
    }

    #[test]
    fn discover_tables_returns_relative_paths_from_base() {
        //* Given
        let temp_dir = create_test_dir();
        let base = temp_dir.path();
        create_file(base, "tables/transfers.sql", "SELECT 1");
        create_file(base, "tables/subdir/balances.sql", "SELECT 2");

        //* When
        let result = discover_tables(base, Path::new("tables"));

        //* Then
        let tables = result.expect("should discover tables");
        let transfers_path = tables
            .get(&"transfers".parse::<TableName>().unwrap())
            .unwrap();
        let balances_path = tables
            .get(&"balances".parse::<TableName>().unwrap())
            .unwrap();

        // Paths should be relative to base_dir (include tables/ prefix)
        assert_eq!(transfers_path, &PathBuf::from("tables/transfers.sql"));
        assert_eq!(balances_path, &PathBuf::from("tables/subdir/balances.sql"));
    }
}

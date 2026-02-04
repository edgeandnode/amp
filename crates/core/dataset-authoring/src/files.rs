//! File hashing and path normalization utilities.
//!
//! This module provides utilities for working with files in the dataset authoring
//! workflow:
//!
//! - **File hashing**: Compute SHA-256 hashes of file contents
//! - **Path normalization**: Convert paths to POSIX-style relative paths
//! - **File references**: Combine path + hash for manifest entries
//!
//! # Path Normalization
//!
//! Paths in manifests must follow these rules:
//! - Relative to the package root (no absolute paths)
//! - POSIX separators (`/`) only
//! - No `./` prefix
//! - No `..` components
//!
//! # Example
//!
//! ```no_run
//! use std::path::Path;
//!
//! use dataset_authoring::files::{FileRef, hash_file, normalize_path};
//!
//! let base = Path::new("/project");
//! let file = Path::new("/project/sql/users.sql");
//!
//! // Compute file hash
//! let hash = hash_file(file).unwrap();
//!
//! // Normalize path relative to base
//! let path = normalize_path(file, base).unwrap();
//! assert_eq!(path, "sql/users.sql");
//!
//! // Create file reference for manifest
//! let file_ref = FileRef { path, hash };
//! ```

use std::{fs, io, path::Path};

use datasets_common::hash::Hash;
use serde::{Deserialize, Serialize};

/// A reference to a file with its normalized path and content hash.
///
/// File references are used in manifests to identify specific file contents
/// by their content-addressable hash. The path is relative to the package
/// root using POSIX separators.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileRef {
    /// Relative path to the file using POSIX separators.
    ///
    /// Must not start with `./` or contain `..` components.
    pub path: String,

    /// SHA-256 hash of the file contents.
    pub hash: Hash,
}

impl FileRef {
    /// Creates a new file reference from a path and hash.
    pub fn new(path: String, hash: Hash) -> Self {
        Self { path, hash }
    }

    /// Creates a file reference by reading and hashing a file.
    ///
    /// # Arguments
    ///
    /// * `file_path` - Absolute path to the file to hash
    /// * `base_path` - Base directory for computing relative paths
    ///
    /// # Errors
    ///
    /// Returns [`FileRefError::HashFile`] if the file cannot be read.
    /// Returns [`FileRefError::NormalizePath`] if the path cannot be normalized.
    pub fn from_file(file_path: &Path, base_path: &Path) -> Result<Self, FileRefError> {
        let hash = hash_file(file_path).map_err(|err| FileRefError::HashFile {
            path: file_path.to_path_buf(),
            source: err,
        })?;
        let path = normalize_path(file_path, base_path).map_err(FileRefError::NormalizePath)?;
        Ok(Self { path, hash })
    }
}

/// Errors that occur when creating a file reference.
#[derive(Debug, thiserror::Error)]
pub enum FileRefError {
    /// Failed to read and hash file contents.
    #[error("failed to hash file '{}'", path.display())]
    HashFile {
        /// Path to the file that failed to hash.
        path: std::path::PathBuf,
        /// Underlying I/O error.
        #[source]
        source: io::Error,
    },

    /// Failed to normalize the file path.
    #[error("failed to normalize path")]
    NormalizePath(#[source] PathError),
}

/// Computes the SHA-256 hash of a file's contents.
///
/// Reads the entire file into memory and computes its hash using the
/// `datasets_common::hash::hash` function.
///
/// # Arguments
///
/// * `path` - Path to the file to hash
///
/// # Errors
///
/// Returns an I/O error if the file cannot be read.
///
/// # Example
///
/// ```no_run
/// use std::path::Path;
///
/// use dataset_authoring::files::hash_file;
///
/// let hash = hash_file(Path::new("sql/users.sql")).unwrap();
/// println!("File hash: {}", hash);
/// ```
pub fn hash_file(path: &Path) -> Result<Hash, io::Error> {
    let contents = fs::read(path)?;
    Ok(datasets_common::hash::hash(&contents))
}

/// Errors that occur during path normalization.
#[derive(Debug, thiserror::Error)]
pub enum PathError {
    /// Path is absolute when a relative path is required.
    #[error("path must be relative, got absolute path '{}'", path.display())]
    AbsolutePath {
        /// The absolute path that was provided.
        path: std::path::PathBuf,
    },

    /// Path is not within the base directory.
    ///
    /// This occurs when the path escapes the base directory via `..` components
    /// or when the path is not a descendant of the base.
    #[error("path '{}' is not within base directory '{}'", path.display(), base.display())]
    NotWithinBase {
        /// The path that is not within the base.
        path: std::path::PathBuf,
        /// The base directory.
        base: std::path::PathBuf,
    },

    /// Path contains disallowed `..` component.
    #[error("path contains disallowed '..' component: '{}'", path.display())]
    ContainsParentRef {
        /// The path containing `..`.
        path: std::path::PathBuf,
    },

    /// Path starts with `./` which is not allowed.
    #[error("path must not start with './': '{}'", path.display())]
    StartsWithCurrentDir {
        /// The path starting with `./`.
        path: std::path::PathBuf,
    },

    /// Failed to strip the base prefix from the path.
    #[error("failed to compute relative path from '{}' to '{}'", base.display(), path.display())]
    StripPrefixFailed {
        /// The path that could not be made relative.
        path: std::path::PathBuf,
        /// The base directory.
        base: std::path::PathBuf,
    },
}

/// Normalizes a file path to a POSIX-style relative path.
///
/// This function converts a path to a normalized form suitable for manifests:
/// - Computes the path relative to the base directory
/// - Uses POSIX separators (`/`) regardless of platform
/// - Rejects paths that start with `./`
/// - Rejects paths containing `..` components
/// - Rejects absolute paths in the result
///
/// # Arguments
///
/// * `path` - The path to normalize (can be absolute or relative)
/// * `base` - The base directory for computing relative paths
///
/// # Errors
///
/// Returns [`PathError`] if the path cannot be normalized:
/// - [`PathError::NotWithinBase`] if the path is not under the base directory
/// - [`PathError::ContainsParentRef`] if the relative path contains `..`
/// - [`PathError::StartsWithCurrentDir`] if the relative path starts with `./`
/// - [`PathError::AbsolutePath`] if the result would be an absolute path
///
/// # Example
///
/// ```
/// use std::path::Path;
///
/// use dataset_authoring::files::normalize_path;
///
/// let base = Path::new("/project");
/// let file = Path::new("/project/sql/users.sql");
///
/// let normalized = normalize_path(file, base).unwrap();
/// assert_eq!(normalized, "sql/users.sql");
/// ```
pub fn normalize_path(path: &Path, base: &Path) -> Result<String, PathError> {
    // Get canonical forms to handle symlinks and relative components
    // We use dunce::canonicalize on Windows to avoid \\?\ prefixes, but
    // for now we just use the paths as-is since we're dealing with logical paths

    // Strip the base prefix to get the relative path
    let relative = path
        .strip_prefix(base)
        .map_err(|_| PathError::NotWithinBase {
            path: path.to_path_buf(),
            base: base.to_path_buf(),
        })?;

    // Convert to string with POSIX separators
    let mut normalized = String::new();
    let mut first = true;

    for component in relative.components() {
        use std::path::Component;

        match component {
            Component::Normal(s) => {
                if !first {
                    normalized.push('/');
                }
                // Convert OsStr to str, rejecting non-UTF8 paths
                let s = s.to_str().ok_or_else(|| PathError::NotWithinBase {
                    path: path.to_path_buf(),
                    base: base.to_path_buf(),
                })?;
                normalized.push_str(s);
                first = false;
            }
            Component::ParentDir => {
                return Err(PathError::ContainsParentRef {
                    path: path.to_path_buf(),
                });
            }
            Component::CurDir => {
                // Skip `.` components but check if it would result in `./` prefix
                if first {
                    return Err(PathError::StartsWithCurrentDir {
                        path: path.to_path_buf(),
                    });
                }
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(PathError::AbsolutePath {
                    path: path.to_path_buf(),
                });
            }
        }
    }

    // Reject empty paths (path == base)
    if normalized.is_empty() {
        return Err(PathError::NotWithinBase {
            path: path.to_path_buf(),
            base: base.to_path_buf(),
        });
    }

    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    // ============================================================
    // hash_file tests
    // ============================================================

    #[test]
    fn hash_file_with_known_content_produces_expected_hash() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");
        let file_path = dir.path().join("test.txt");
        std::fs::write(&file_path, b"hello world").expect("should write file");

        // SHA-256 of "hello world"
        let expected = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9";

        //* When
        let result = hash_file(&file_path);

        //* Then
        let hash = result.expect("hashing should succeed");
        assert_eq!(hash.as_str(), expected);
    }

    #[test]
    fn hash_file_with_empty_file_produces_empty_hash() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");
        let file_path = dir.path().join("empty.txt");
        std::fs::write(&file_path, b"").expect("should write file");

        // SHA-256 of empty string
        let expected = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

        //* When
        let result = hash_file(&file_path);

        //* Then
        let hash = result.expect("hashing should succeed");
        assert_eq!(hash.as_str(), expected);
    }

    #[test]
    fn hash_file_with_nonexistent_file_returns_error() {
        //* Given
        let path = Path::new("/nonexistent/path/to/file.txt");

        //* When
        let result = hash_file(path);

        //* Then
        assert!(result.is_err(), "should fail for nonexistent file");
        let err = result.expect_err("should return error");
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn hash_file_is_deterministic() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");
        let file_path = dir.path().join("test.txt");
        std::fs::write(&file_path, b"deterministic content").expect("should write file");

        //* When
        let hash1 = hash_file(&file_path).expect("first hash should succeed");
        let hash2 = hash_file(&file_path).expect("second hash should succeed");

        //* Then
        assert_eq!(hash1, hash2, "same file should produce same hash");
    }

    #[test]
    fn hash_file_with_binary_content_succeeds() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");
        let file_path = dir.path().join("binary.bin");
        let binary_content: Vec<u8> = (0..=255).collect();
        std::fs::write(&file_path, &binary_content).expect("should write file");

        //* When
        let result = hash_file(&file_path);

        //* Then
        assert!(result.is_ok(), "should hash binary content");
    }

    // ============================================================
    // normalize_path tests
    // ============================================================

    #[test]
    fn normalize_path_with_simple_relative_path_succeeds() {
        //* Given
        let base = Path::new("/project");
        let path = Path::new("/project/sql/users.sql");

        //* When
        let result = normalize_path(path, base);

        //* Then
        let normalized = result.expect("normalization should succeed");
        assert_eq!(normalized, "sql/users.sql");
    }

    #[test]
    fn normalize_path_with_nested_path_uses_posix_separators() {
        //* Given
        let base = Path::new("/project");
        let path = Path::new("/project/sql/tables/users.sql");

        //* When
        let result = normalize_path(path, base);

        //* Then
        let normalized = result.expect("normalization should succeed");
        assert_eq!(normalized, "sql/tables/users.sql");
        assert!(!normalized.contains('\\'), "should not contain backslashes");
    }

    #[test]
    fn normalize_path_with_single_file_succeeds() {
        //* Given
        let base = Path::new("/project");
        let path = Path::new("/project/manifest.json");

        //* When
        let result = normalize_path(path, base);

        //* Then
        let normalized = result.expect("normalization should succeed");
        assert_eq!(normalized, "manifest.json");
    }

    #[test]
    fn normalize_path_with_path_outside_base_fails() {
        //* Given
        let base = Path::new("/project");
        let path = Path::new("/other/file.sql");

        //* When
        let result = normalize_path(path, base);

        //* Then
        assert!(result.is_err(), "should fail for path outside base");
        let err = result.expect_err("should return error");
        assert!(
            matches!(err, PathError::NotWithinBase { .. }),
            "should be NotWithinBase error"
        );
    }

    #[test]
    fn normalize_path_with_path_equal_to_base_fails() {
        //* Given
        let base = Path::new("/project");
        let path = Path::new("/project");

        //* When
        let result = normalize_path(path, base);

        //* Then
        assert!(result.is_err(), "should fail when path equals base");
        let err = result.expect_err("should return error");
        assert!(
            matches!(err, PathError::NotWithinBase { .. }),
            "should be NotWithinBase error"
        );
    }

    #[test]
    fn normalize_path_rejects_dotdot_components() {
        //* Given
        let base = Path::new("/project");
        // This path technically resolves within base, but contains ..
        let path = Path::new("/project/sql/../sql/users.sql");

        //* When
        let result = normalize_path(path, base);

        //* Then
        // The path contains .. so it should be rejected
        assert!(result.is_err(), "should reject paths with ..");
    }

    #[test]
    fn normalize_path_result_does_not_start_with_dot_slash() {
        //* Given
        let base = Path::new("/project");
        let path = Path::new("/project/file.sql");

        //* When
        let result = normalize_path(path, base);

        //* Then
        let normalized = result.expect("normalization should succeed");
        assert!(
            !normalized.starts_with("./"),
            "result should not start with ./"
        );
        assert!(
            !normalized.starts_with('.'),
            "result should not start with ."
        );
    }

    #[test]
    fn normalize_path_result_is_not_absolute() {
        //* Given
        let base = Path::new("/project");
        let path = Path::new("/project/sql/users.sql");

        //* When
        let result = normalize_path(path, base);

        //* Then
        let normalized = result.expect("normalization should succeed");
        assert!(
            !normalized.starts_with('/'),
            "result should not be absolute"
        );
    }

    // ============================================================
    // FileRef tests
    // ============================================================

    #[test]
    fn file_ref_new_creates_instance() {
        //* Given
        let path = "sql/users.sql".to_string();
        let hash: Hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
            .parse()
            .expect("should parse hash");

        //* When
        let file_ref = FileRef::new(path.clone(), hash.clone());

        //* Then
        assert_eq!(file_ref.path, path);
        assert_eq!(file_ref.hash, hash);
    }

    #[test]
    fn file_ref_from_file_creates_instance() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");
        let sql_dir = dir.path().join("sql");
        std::fs::create_dir(&sql_dir).expect("should create sql dir");
        let file_path = sql_dir.join("users.sql");
        std::fs::write(&file_path, b"SELECT * FROM users").expect("should write file");

        //* When
        let result = FileRef::from_file(&file_path, dir.path());

        //* Then
        let file_ref = result.expect("should create file ref");
        assert_eq!(file_ref.path, "sql/users.sql");
        // Verify hash is computed correctly
        let expected_hash = hash_file(&file_path).expect("should hash file");
        assert_eq!(file_ref.hash, expected_hash);
    }

    #[test]
    fn file_ref_from_file_with_nonexistent_file_returns_error() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");
        let file_path = dir.path().join("nonexistent.sql");

        //* When
        let result = FileRef::from_file(&file_path, dir.path());

        //* Then
        assert!(result.is_err(), "should fail for nonexistent file");
        let err = result.expect_err("should return error");
        assert!(
            matches!(err, FileRefError::HashFile { .. }),
            "should be HashFile error"
        );
    }

    #[test]
    fn file_ref_serializes_to_json() {
        //* Given
        let file_ref = FileRef {
            path: "sql/users.sql".to_string(),
            hash: "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
                .parse()
                .expect("should parse hash"),
        };

        //* When
        let json = serde_json::to_string(&file_ref);

        //* Then
        let json_str = json.expect("serialization should succeed");
        assert!(json_str.contains("sql/users.sql"));
        assert!(
            json_str.contains("b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9")
        );
    }

    #[test]
    fn file_ref_deserializes_from_json() {
        //* Given
        let json = r#"{"path":"sql/users.sql","hash":"b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"}"#;

        //* When
        let result: Result<FileRef, _> = serde_json::from_str(json);

        //* Then
        let file_ref = result.expect("deserialization should succeed");
        assert_eq!(file_ref.path, "sql/users.sql");
        assert_eq!(
            file_ref.hash.as_str(),
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    #[test]
    fn file_ref_roundtrip_serialization() {
        //* Given
        let original = FileRef {
            path: "functions/decode.js".to_string(),
            hash: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                .parse()
                .expect("should parse hash"),
        };

        //* When
        let json = serde_json::to_string(&original).expect("serialization should succeed");
        let deserialized: FileRef =
            serde_json::from_str(&json).expect("deserialization should succeed");

        //* Then
        assert_eq!(deserialized, original);
    }
}

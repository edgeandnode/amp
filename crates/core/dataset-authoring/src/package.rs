//! Package assembly for dataset authoring.
//!
//! Creates deterministic `dataset.tgz` archives containing:
//! - `manifest.json` (canonical)
//! - `sql/<table>.sql` (rendered, post-Jinja)
//! - `sql/<table>.schema.json`
//! - `functions/<name>.js` (if any)
//!
//! # Determinism
//!
//! Archives are deterministic for identical inputs:
//! - Entries sorted alphabetically by path
//! - mtime set to Unix epoch (0)
//! - Consistent permissions (0o644 for files)
//! - No ownership metadata (uid/gid = 0)
//!
//! # Example
//!
//! ```ignore
//! use std::path::Path;
//!
//! use dataset_authoring::package::PackageBuilder;
//!
//! // Build from a directory containing manifest.json, sql/, and optional functions/
//! let builder = PackageBuilder::from_directory(Path::new("build"))?;
//! builder.write_to(Path::new("dataset.tgz"))?;
//! ```

use std::{
    fs::{self, File},
    io::{self, Read},
    path::Path,
};

use datasets_common::hash::Hash;
use flate2::{Compression, write::GzEncoder};

/// Default file permissions for archive entries (rw-r--r--).
const FILE_MODE: u32 = 0o644;

/// Default directory permissions for archive entries (rwxr-xr-x).
const DIR_MODE: u32 = 0o755;

/// Unix epoch timestamp for deterministic archives.
const MTIME: u64 = 0;

/// Errors that occur during package assembly.
#[derive(Debug, thiserror::Error)]
pub enum PackageError {
    /// Failed to read a file to include in the package.
    #[error("failed to read file '{}'", path.display())]
    ReadFile {
        /// Path to the file that could not be read.
        path: std::path::PathBuf,
        /// Underlying I/O error.
        #[source]
        source: io::Error,
    },

    /// Failed to write the package archive.
    #[error("failed to write package archive")]
    WriteArchive(#[source] io::Error),

    /// Failed to create the output file.
    #[error("failed to create output file '{}'", path.display())]
    CreateOutput {
        /// Path to the output file.
        path: std::path::PathBuf,
        /// Underlying I/O error.
        #[source]
        source: io::Error,
    },

    /// Failed to finish writing the archive.
    #[error("failed to finalize archive")]
    FinalizeArchive(#[source] io::Error),

    /// Required manifest.json not found.
    #[error("manifest.json not found in package directory")]
    MissingManifest,

    /// Failed to read the source directory.
    #[error("failed to read directory '{}'", path.display())]
    ReadDirectory {
        /// Path to the directory.
        path: std::path::PathBuf,
        /// Underlying I/O error.
        #[source]
        source: io::Error,
    },
}

/// An entry to include in the package archive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageEntry {
    /// Archive path (POSIX style, no leading slash).
    pub path: String,
    /// File contents.
    pub contents: Vec<u8>,
}

impl PackageEntry {
    /// Creates a new package entry.
    pub fn new(path: impl Into<String>, contents: impl Into<Vec<u8>>) -> Self {
        Self {
            path: path.into(),
            contents: contents.into(),
        }
    }

    /// Creates a package entry by reading from a file.
    ///
    /// # Arguments
    ///
    /// * `file_path` - Absolute path to the file to read
    /// * `archive_path` - Path within the archive (POSIX style)
    ///
    /// # Errors
    ///
    /// Returns [`PackageError::ReadFile`] if the file cannot be read.
    pub fn from_file(
        file_path: &Path,
        archive_path: impl Into<String>,
    ) -> Result<Self, PackageError> {
        let contents = fs::read(file_path).map_err(|err| PackageError::ReadFile {
            path: file_path.to_path_buf(),
            source: err,
        })?;

        Ok(Self {
            path: archive_path.into(),
            contents,
        })
    }
}

/// Builder for creating deterministic package archives.
///
/// Collects entries and writes them to a gzipped tar archive with
/// deterministic metadata (sorted entries, fixed timestamps, etc.).
#[derive(Debug, Default)]
pub struct PackageBuilder {
    entries: Vec<PackageEntry>,
}

impl PackageBuilder {
    /// Creates a new empty package builder.
    pub fn new() -> Self {
        Self { entries: vec![] }
    }

    /// Creates a package builder from a directory containing the build output.
    ///
    /// Expects the directory to contain:
    /// - `manifest.json` (required)
    /// - `sql/*.sql` and `sql/*.schema.json` (table files)
    /// - `functions/*.js` (optional function files)
    ///
    /// # Errors
    ///
    /// Returns [`PackageError::MissingManifest`] if manifest.json is not found.
    /// Returns [`PackageError::ReadDirectory`] if a directory cannot be read.
    /// Returns [`PackageError::ReadFile`] if a file cannot be read.
    pub fn from_directory(dir: &Path) -> Result<Self, PackageError> {
        let mut builder = Self::new();

        // Add manifest.json (required)
        let manifest_path = dir.join("manifest.json");
        if !manifest_path.exists() {
            return Err(PackageError::MissingManifest);
        }
        builder.add_entry(PackageEntry::from_file(&manifest_path, "manifest.json")?);

        // Add sql/ directory contents
        let sql_dir = dir.join("sql");
        if sql_dir.exists() {
            builder.add_directory_contents(&sql_dir, "sql")?;
        }

        // Add functions/ directory contents
        let functions_dir = dir.join("functions");
        if functions_dir.exists() {
            builder.add_directory_contents(&functions_dir, "functions")?;
        }

        Ok(builder)
    }

    /// Adds a single entry to the package.
    pub fn add_entry(&mut self, entry: PackageEntry) -> &mut Self {
        self.entries.push(entry);
        self
    }

    /// Adds all files from a directory to the package.
    ///
    /// Files are added with paths relative to the given prefix.
    ///
    /// # Arguments
    ///
    /// * `dir` - Directory to read files from
    /// * `prefix` - Archive path prefix for the files
    fn add_directory_contents(
        &mut self,
        dir: &Path,
        prefix: &str,
    ) -> Result<&mut Self, PackageError> {
        let entries = fs::read_dir(dir).map_err(|err| PackageError::ReadDirectory {
            path: dir.to_path_buf(),
            source: err,
        })?;

        for entry in entries {
            let entry = entry.map_err(|err| PackageError::ReadDirectory {
                path: dir.to_path_buf(),
                source: err,
            })?;

            let path = entry.path();
            if path.is_file() {
                let file_name = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("unknown");
                let archive_path = format!("{}/{}", prefix, file_name);
                self.add_entry(PackageEntry::from_file(&path, archive_path)?);
            }
        }

        Ok(self)
    }

    /// Returns the entries that will be included in the package.
    ///
    /// Entries are sorted alphabetically by path for determinism.
    pub fn entries(&self) -> Vec<&PackageEntry> {
        let mut entries: Vec<_> = self.entries.iter().collect();
        entries.sort_by(|a, b| a.path.cmp(&b.path));
        entries
    }

    /// Writes the package archive to a file.
    ///
    /// # Arguments
    ///
    /// * `output` - Path to write the archive to
    ///
    /// # Errors
    ///
    /// Returns [`PackageError::CreateOutput`] if the output file cannot be created.
    /// Returns [`PackageError::WriteArchive`] if writing fails.
    /// Returns [`PackageError::FinalizeArchive`] if finalizing the archive fails.
    pub fn write_to(&self, output: &Path) -> Result<(), PackageError> {
        let file = File::create(output).map_err(|err| PackageError::CreateOutput {
            path: output.to_path_buf(),
            source: err,
        })?;

        self.write(file)
    }

    /// Writes the package archive to a writer.
    ///
    /// This is useful for testing or writing to memory.
    ///
    /// # Errors
    ///
    /// Returns [`PackageError::WriteArchive`] if writing fails.
    /// Returns [`PackageError::FinalizeArchive`] if finalizing the archive fails.
    pub fn write<W: io::Write>(&self, writer: W) -> Result<(), PackageError> {
        let gz = GzEncoder::new(writer, Compression::default());
        let mut tar = tar::Builder::new(gz);

        // Sort entries alphabetically for determinism
        let mut sorted_entries: Vec<_> = self.entries.iter().collect();
        sorted_entries.sort_by(|a, b| a.path.cmp(&b.path));

        // Collect unique directories that need to be created
        let mut directories: Vec<String> = vec![];
        for entry in &sorted_entries {
            if let Some(parent) = Path::new(&entry.path).parent() {
                let parent_str = parent.to_string_lossy();
                if !parent_str.is_empty() && !directories.contains(&parent_str.to_string()) {
                    directories.push(parent_str.to_string());
                }
            }
        }
        directories.sort();

        // Add directory entries first
        for dir_path in &directories {
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(tar::EntryType::Directory);
            header.set_size(0);
            header.set_mode(DIR_MODE);
            header.set_mtime(MTIME);
            header.set_uid(0);
            header.set_gid(0);
            header.set_cksum();

            let dir_with_slash = format!("{}/", dir_path);
            tar.append_data(&mut header, &dir_with_slash, io::empty())
                .map_err(PackageError::WriteArchive)?;
        }

        // Add file entries
        for entry in sorted_entries {
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(tar::EntryType::Regular);
            header.set_size(entry.contents.len() as u64);
            header.set_mode(FILE_MODE);
            header.set_mtime(MTIME);
            header.set_uid(0);
            header.set_gid(0);
            header.set_cksum();

            tar.append_data(&mut header, &entry.path, entry.contents.as_slice())
                .map_err(PackageError::WriteArchive)?;
        }

        // Finish writing
        let gz = tar.into_inner().map_err(PackageError::WriteArchive)?;
        gz.finish().map_err(PackageError::FinalizeArchive)?;

        Ok(())
    }

    /// Writes the package archive to a byte vector.
    ///
    /// Useful for computing the hash of the archive or for testing.
    ///
    /// # Errors
    ///
    /// Returns [`PackageError::WriteArchive`] if writing fails.
    /// Returns [`PackageError::FinalizeArchive`] if finalizing the archive fails.
    pub fn to_bytes(&self) -> Result<Vec<u8>, PackageError> {
        let mut buffer = Vec::new();
        self.write(&mut buffer)?;
        Ok(buffer)
    }

    /// Computes the SHA-256 hash of the package archive.
    ///
    /// This can be used to verify the package was built correctly
    /// or to check for reproducibility.
    ///
    /// # Errors
    ///
    /// Returns [`PackageError::WriteArchive`] if writing fails.
    /// Returns [`PackageError::FinalizeArchive`] if finalizing the archive fails.
    pub fn hash(&self) -> Result<Hash, PackageError> {
        let bytes = self.to_bytes()?;
        Ok(datasets_common::hash::hash(&bytes))
    }
}

/// Extracts a package archive to a directory.
///
/// This is useful for testing or inspecting package contents.
///
/// # Arguments
///
/// * `archive_path` - Path to the `.tgz` archive
/// * `output_dir` - Directory to extract to (must exist)
///
/// # Errors
///
/// Returns [`PackageError::ReadFile`] if the archive cannot be read.
/// Returns [`PackageError::WriteArchive`] if extraction fails.
pub fn extract_to_directory(archive_path: &Path, output_dir: &Path) -> Result<(), PackageError> {
    let file = File::open(archive_path).map_err(|err| PackageError::ReadFile {
        path: archive_path.to_path_buf(),
        source: err,
    })?;

    let gz = flate2::read::GzDecoder::new(file);
    let mut archive = tar::Archive::new(gz);

    archive
        .unpack(output_dir)
        .map_err(PackageError::WriteArchive)?;

    Ok(())
}

/// Reads the contents of a package archive into memory.
///
/// Returns a list of entries with their paths and contents.
///
/// # Arguments
///
/// * `archive_path` - Path to the `.tgz` archive
///
/// # Errors
///
/// Returns [`PackageError::ReadFile`] if the archive cannot be read.
/// Returns [`PackageError::WriteArchive`] if parsing fails.
pub fn read_archive(archive_path: &Path) -> Result<Vec<PackageEntry>, PackageError> {
    let file = File::open(archive_path).map_err(|err| PackageError::ReadFile {
        path: archive_path.to_path_buf(),
        source: err,
    })?;

    read_archive_from_reader(file)
}

/// Reads the contents of a package archive from a reader.
///
/// Returns a list of entries with their paths and contents.
///
/// # Errors
///
/// Returns [`PackageError::WriteArchive`] if parsing fails.
pub fn read_archive_from_reader<R: Read>(reader: R) -> Result<Vec<PackageEntry>, PackageError> {
    let gz = flate2::read::GzDecoder::new(reader);
    let mut archive = tar::Archive::new(gz);

    let mut entries = Vec::new();

    for entry in archive.entries().map_err(PackageError::WriteArchive)? {
        let mut entry = entry.map_err(PackageError::WriteArchive)?;

        // Skip directories
        if entry.header().entry_type() == tar::EntryType::Directory {
            continue;
        }

        let path = entry
            .path()
            .map_err(PackageError::WriteArchive)?
            .to_string_lossy()
            .into_owned();

        let mut contents = Vec::new();
        entry
            .read_to_end(&mut contents)
            .map_err(PackageError::WriteArchive)?;

        entries.push(PackageEntry { path, contents });
    }

    // Sort for consistent ordering
    entries.sort_by(|a, b| a.path.cmp(&b.path));

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    // ============================================================
    // PackageEntry tests
    // ============================================================

    #[test]
    fn package_entry_new_creates_entry() {
        //* Given
        let path = "manifest.json";
        let contents = b"{}".to_vec();

        //* When
        let entry = PackageEntry::new(path, contents.clone());

        //* Then
        assert_eq!(entry.path, path);
        assert_eq!(entry.contents, contents);
    }

    #[test]
    fn package_entry_from_file_reads_content() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");
        let file_path = dir.path().join("test.txt");
        fs::write(&file_path, b"hello world").expect("should write file");

        //* When
        let result = PackageEntry::from_file(&file_path, "test.txt");

        //* Then
        let entry = result.expect("should create entry");
        assert_eq!(entry.path, "test.txt");
        assert_eq!(entry.contents, b"hello world");
    }

    #[test]
    fn package_entry_from_nonexistent_file_fails() {
        //* Given
        let path = Path::new("/nonexistent/file.txt");

        //* When
        let result = PackageEntry::from_file(path, "file.txt");

        //* Then
        assert!(result.is_err());
        let err = result.expect_err("should fail");
        assert!(matches!(err, PackageError::ReadFile { .. }));
    }

    // ============================================================
    // PackageBuilder tests
    // ============================================================

    #[test]
    fn package_builder_new_creates_empty() {
        //* Given / When
        let builder = PackageBuilder::new();

        //* Then
        assert!(builder.entries.is_empty());
    }

    #[test]
    fn package_builder_add_entry_adds_entry() {
        //* Given
        let mut builder = PackageBuilder::new();
        let entry = PackageEntry::new("manifest.json", b"{}".to_vec());

        //* When
        builder.add_entry(entry);

        //* Then
        assert_eq!(builder.entries.len(), 1);
    }

    #[test]
    fn package_builder_entries_returns_sorted() {
        //* Given
        let mut builder = PackageBuilder::new();
        builder.add_entry(PackageEntry::new("sql/z.sql", b"z"));
        builder.add_entry(PackageEntry::new("manifest.json", b"{}"));
        builder.add_entry(PackageEntry::new("sql/a.sql", b"a"));

        //* When
        let entries = builder.entries();

        //* Then
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].path, "manifest.json");
        assert_eq!(entries[1].path, "sql/a.sql");
        assert_eq!(entries[2].path, "sql/z.sql");
    }

    #[test]
    fn package_builder_to_bytes_produces_valid_archive() {
        //* Given
        let mut builder = PackageBuilder::new();
        builder.add_entry(PackageEntry::new("manifest.json", b"{}".to_vec()));

        //* When
        let result = builder.to_bytes();

        //* Then
        let bytes = result.expect("should produce bytes");
        assert!(!bytes.is_empty());
        // Should start with gzip magic number
        assert_eq!(&bytes[0..2], &[0x1f, 0x8b]);
    }

    #[test]
    fn package_builder_produces_deterministic_output() {
        //* Given
        let mut builder1 = PackageBuilder::new();
        builder1.add_entry(PackageEntry::new("manifest.json", b"{}".to_vec()));
        builder1.add_entry(PackageEntry::new("sql/test.sql", b"SELECT 1".to_vec()));

        let mut builder2 = PackageBuilder::new();
        // Add in different order
        builder2.add_entry(PackageEntry::new("sql/test.sql", b"SELECT 1".to_vec()));
        builder2.add_entry(PackageEntry::new("manifest.json", b"{}".to_vec()));

        //* When
        let bytes1 = builder1.to_bytes().expect("should produce bytes");
        let bytes2 = builder2.to_bytes().expect("should produce bytes");

        //* Then
        assert_eq!(
            bytes1, bytes2,
            "archives should be identical regardless of entry order"
        );
    }

    #[test]
    fn package_builder_hash_is_deterministic() {
        //* Given
        let mut builder = PackageBuilder::new();
        builder.add_entry(PackageEntry::new("manifest.json", b"{}".to_vec()));

        //* When
        let hash1 = builder.hash().expect("should compute hash");
        let hash2 = builder.hash().expect("should compute hash");

        //* Then
        assert_eq!(hash1, hash2, "hash should be deterministic");
    }

    #[test]
    fn package_builder_from_directory_requires_manifest() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");
        // Don't create manifest.json

        //* When
        let result = PackageBuilder::from_directory(dir.path());

        //* Then
        assert!(result.is_err());
        let err = result.expect_err("should fail");
        assert!(matches!(err, PackageError::MissingManifest));
    }

    #[test]
    fn package_builder_from_directory_includes_all_files() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");

        // Create manifest.json
        fs::write(dir.path().join("manifest.json"), b"{}").expect("should write");

        // Create sql/ directory with files
        fs::create_dir(dir.path().join("sql")).expect("should create sql dir");
        fs::write(dir.path().join("sql/users.sql"), b"SELECT * FROM users").expect("should write");
        fs::write(dir.path().join("sql/users.schema.json"), b"{\"fields\":[]}")
            .expect("should write");

        // Create functions/ directory with file
        fs::create_dir(dir.path().join("functions")).expect("should create functions dir");
        fs::write(
            dir.path().join("functions/decode.js"),
            b"export function decode() {}",
        )
        .expect("should write");

        //* When
        let result = PackageBuilder::from_directory(dir.path());

        //* Then
        let builder = result.expect("should build from directory");
        let entries = builder.entries();

        // Should have 4 files
        assert_eq!(entries.len(), 4);

        // Verify paths (sorted)
        let paths: Vec<_> = entries.iter().map(|e| e.path.as_str()).collect();
        assert!(paths.contains(&"manifest.json"));
        assert!(paths.contains(&"sql/users.sql"));
        assert!(paths.contains(&"sql/users.schema.json"));
        assert!(paths.contains(&"functions/decode.js"));
    }

    #[test]
    fn package_builder_write_to_creates_file() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");
        let output_path = dir.path().join("dataset.tgz");

        let mut builder = PackageBuilder::new();
        builder.add_entry(PackageEntry::new("manifest.json", b"{}".to_vec()));

        //* When
        let result = builder.write_to(&output_path);

        //* Then
        assert!(result.is_ok());
        assert!(output_path.exists());
    }

    // ============================================================
    // Archive reading tests
    // ============================================================

    #[test]
    fn roundtrip_write_and_read_archive() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");
        let archive_path = dir.path().join("test.tgz");

        let mut builder = PackageBuilder::new();
        builder.add_entry(PackageEntry::new("manifest.json", b"{\"name\":\"test\"}"));
        builder.add_entry(PackageEntry::new("sql/users.sql", b"SELECT * FROM users"));

        builder.write_to(&archive_path).expect("should write");

        //* When
        let result = read_archive(&archive_path);

        //* Then
        let entries = result.expect("should read archive");
        assert_eq!(entries.len(), 2);

        assert_eq!(entries[0].path, "manifest.json");
        assert_eq!(entries[0].contents, b"{\"name\":\"test\"}");

        assert_eq!(entries[1].path, "sql/users.sql");
        assert_eq!(entries[1].contents, b"SELECT * FROM users");
    }

    #[test]
    fn extract_to_directory_creates_files() {
        //* Given
        let dir = TempDir::new().expect("should create temp dir");
        let archive_path = dir.path().join("test.tgz");
        let output_dir = dir.path().join("extracted");
        fs::create_dir(&output_dir).expect("should create output dir");

        let mut builder = PackageBuilder::new();
        builder.add_entry(PackageEntry::new("manifest.json", b"{}"));
        builder.add_entry(PackageEntry::new("sql/users.sql", b"SELECT 1"));
        builder.write_to(&archive_path).expect("should write");

        //* When
        let result = extract_to_directory(&archive_path, &output_dir);

        //* Then
        assert!(result.is_ok());
        assert!(output_dir.join("manifest.json").exists());
        assert!(output_dir.join("sql/users.sql").exists());
    }

    // ============================================================
    // Determinism verification tests
    // ============================================================

    #[test]
    fn multiple_builds_produce_identical_archives() {
        //* Given
        let entries = vec![
            PackageEntry::new("manifest.json", b"{}".to_vec()),
            PackageEntry::new("sql/a.sql", b"SELECT 1".to_vec()),
            PackageEntry::new("sql/b.sql", b"SELECT 2".to_vec()),
            PackageEntry::new("functions/f.js", b"export function f() {}".to_vec()),
        ];

        //* When
        let mut hashes = Vec::new();
        for _ in 0..3 {
            let mut builder = PackageBuilder::new();
            for entry in &entries {
                builder.add_entry(entry.clone());
            }
            hashes.push(builder.hash().expect("should compute hash"));
        }

        //* Then
        assert_eq!(hashes[0], hashes[1]);
        assert_eq!(hashes[1], hashes[2]);
    }
}

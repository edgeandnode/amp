use datasets_common::{name::Name, namespace::Namespace, table_name::TableName};
use object_store::path::Path;
use url::Url;
use uuid::Uuid;

/// Physical table URL _new-type_ wrapper
///
/// Represents a base directory URL in the object store containing all parquet files for a table.
/// Individual file URLs are constructed by appending the filename to this base URL.
///
/// ## URL Format
///
/// `<store_base_url>/<dataset_name>/<table_name>/<revision_id>/`
///
/// Where:
/// - `store_base_url`: Object store base URL, may include path prefix after bucket
///   (e.g., `s3://bucket/prefix`, `file:///data/subdir`)
/// - `dataset_name`: Dataset name (without namespace)
/// - `table_name`: Table name
/// - `revision_id`: Unique identifier for this table revision (typically UUIDv7)
///
/// ## Example
///
/// ```text
/// s3://my-bucket/prefix/ethereum_mainnet/logs/01234567-89ab-cdef-0123-456789abcdef/
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PhyTableUrl(Url);

impl PhyTableUrl {
    /// Constructs a table URL from a base URL and revision path.
    ///
    /// The URL follows the structure: `<base_url>/<revision_path>/`
    ///
    /// Where:
    /// - `base_url`: Object store root URL (e.g., `s3://bucket/prefix/`, `file:///data/`)
    /// - `revision_path`: Complete path to the table revision (dataset_name/table_name/revision_uuid)
    pub fn new(base_url: &Url, revision_path: &PhyTableRevisionPath) -> Self {
        // SAFETY: Path components (Name, TableName, Uuid) contain only URL-safe characters
        let raw_url = base_url
            .join(&format!("{}/", revision_path))
            .expect("path is URL-safe");
        PhyTableUrl(raw_url)
    }

    /// Get the URL as a string slice
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// Get a reference to the inner [`Url`]
    pub fn inner(&self) -> &Url {
        &self.0
    }
}

impl std::str::FromStr for PhyTableUrl {
    type Err = PhyTableUrlParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let url = s.parse().map_err(|err| PhyTableUrlParseError {
            url: s.to_string(),
            source: err,
        })?;
        Ok(PhyTableUrl(url))
    }
}

impl std::fmt::Display for PhyTableUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.as_str())
    }
}

/// Path to a table directory in object storage (without revision).
///
/// Represents the parent directory containing all revisions of a table.
/// Format: `<dataset_name>/<table_name>`
///
/// **NOTE**: The underlying [`object_store::Path`] type automatically strips leading and
/// trailing slashes, so the string representation will not contain a trailing slash.
///
/// ## Example
///
/// ```text
/// ethereum_mainnet/logs
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhyTablePath(Path);

impl PhyTablePath {
    /// Constructs the path to a table directory (without revision).
    pub fn new(dataset_name: impl AsRef<Name>, table_name: impl AsRef<TableName>) -> Self {
        Self(format!("{}/{}", dataset_name.as_ref(), table_name.as_ref()).into())
    }

    /// Create a revision path by appending the given revision ID to this table path.
    pub fn with_revision(&self, revision_id: impl AsRef<Uuid>) -> PhyTableRevisionPath {
        let path = self.0.child(revision_id.as_ref().to_string());
        PhyTableRevisionPath(path)
    }

    /// Get a reference to the underlying [`object_store::path::Path`]
    pub fn as_object_store_path(&self) -> &Path {
        &self.0
    }

    /// Get the path as a string slice
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }
}

impl std::fmt::Display for PhyTablePath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::ops::Deref for PhyTablePath {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Path to a table revision directory in object storage.
///
/// Represents a specific revision of a table, identified by a UUID.
/// Format: `<dataset_name>/<table_name>/<revision_uuid>`
///
/// **NOTE**: The underlying [`object_store::Path`] type automatically strips leading and
/// trailing slashes, so the string representation will not contain a trailing slash.
///
/// ## Example
///
/// ```text
/// ethereum_mainnet/logs/01234567-89ab-cdef-0123-456789abcdef
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhyTableRevisionPath(Path);

impl PhyTableRevisionPath {
    /// Constructs the path to a table revision directory.
    pub fn new(
        namespace: impl AsRef<Namespace>,
        dataset_name: impl AsRef<Name>,
        table_name: impl AsRef<TableName>,
        revision_id: impl AsRef<Uuid>,
    ) -> Self {
        Self(
            format!(
                "{}/{}/{}/{}",
                namespace.as_ref(),
                dataset_name.as_ref(),
                table_name.as_ref(),
                revision_id.as_ref()
            )
            .into(),
        )
    }

    /// Creates a path from a pre-validated [`Path`].
    ///
    /// This is an internal constructor used after validation has been performed
    /// (e.g., via [`FromStr`] implementation). The caller must ensure the path
    /// is non-empty and contains valid object store path characters.
    fn new_unchecked(path: Path) -> Self {
        Self(path)
    }

    /// Get a reference to the underlying [`Path`]
    pub fn as_object_store_path(&self) -> &Path {
        &self.0
    }

    /// Get the path as a string slice
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }
}

/// Parses a string into a [`PhyTableRevisionPath`].
///
/// Validates that the input is:
/// - Non-empty
/// - A valid [`object_store::path::Path`] (no invalid characters or empty segments)
///
/// This is the primary constructor for paths from external input (e.g., API requests,
/// database records).
impl std::str::FromStr for PhyTableRevisionPath {
    type Err = PhyTableRevisionPathParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(PhyTableRevisionPathParseError::Empty);
        }
        let path =
            object_store::path::Path::parse(s).map_err(PhyTableRevisionPathParseError::Invalid)?;

        Ok(Self::new_unchecked(path))
    }
}

impl std::fmt::Display for PhyTableRevisionPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::ops::Deref for PhyTableRevisionPath {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Errors that occur when parsing a string into a [`PhyTableRevisionPath`]
///
/// This error type is used by the `FromStr` implementation for `PhyTableRevisionPath`.
#[derive(Debug, thiserror::Error)]
pub enum PhyTableRevisionPathParseError {
    /// The provided path string is empty
    ///
    /// A physical table revision path must contain at least the path components
    /// `namespace/dataset/table/revision_id`. An empty string cannot represent
    /// a valid revision path.
    #[error("physical table revision path cannot be empty")]
    Empty,

    /// The path contains invalid characters or malformed segments
    ///
    /// This error occurs when the path string fails object store path validation.
    ///
    /// Common causes:
    /// - Empty segments (consecutive slashes like `foo//bar`)
    /// - Invalid characters in path segments
    /// - Malformed path encoding
    #[error("invalid physical table revision path: {0}")]
    Invalid(object_store::path::Error),
}

/// Error type for PhyTableUrl parsing
#[derive(Debug, thiserror::Error)]
#[error("invalid object store URL '{url}'")]
pub struct PhyTableUrlParseError {
    url: String,
    #[source]
    source: url::ParseError,
}

impl From<PhyTableRevisionPath> for metadata_db::physical_table::TablePathOwned {
    fn from(value: PhyTableRevisionPath) -> Self {
        metadata_db::physical_table::TablePath::from_owned_unchecked(value.as_str().to_owned())
    }
}

impl<'a> From<&'a PhyTableRevisionPath> for metadata_db::physical_table::TablePath<'a> {
    fn from(value: &'a PhyTableRevisionPath) -> Self {
        metadata_db::physical_table::TablePath::from_ref_unchecked(value.as_str())
    }
}

impl From<metadata_db::physical_table::TablePathOwned> for PhyTableRevisionPath {
    fn from(value: metadata_db::physical_table::TablePathOwned) -> Self {
        value
            .as_str()
            .parse()
            .expect("database path should be a valid revision path")
    }
}

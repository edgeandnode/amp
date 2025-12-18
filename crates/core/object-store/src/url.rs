//! Validated URL wrapper and provider types for object store URLs.

use std::path::PathBuf;

use fs_err as fs;
use object_store::ObjectStoreScheme as DatafusionObjectStoreScheme;
use url::Url;

/// A validated URL wrapper that ensures the URL is a valid object store URL.
#[derive(Clone, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct ObjectStoreUrl(Url);

impl ObjectStoreUrl {
    /// Creates a new ObjectStoreUrl from a string that can be either a URL or filesystem path.
    pub fn new(location: impl Into<String>) -> Result<Self, ObjectStoreUrlError> {
        Self::new_with_base(location, None)
    }

    /// Creates a new [`ObjectStoreUrl`] from a string that can be either a URL or filesystem path.
    ///
    /// If `data_location` is a relative filesystem path, then `base` will be used as the prefix.
    pub fn new_with_base(
        location: impl Into<String>,
        base: Option<&std::path::Path>,
    ) -> Result<Self, ObjectStoreUrlError> {
        let location = location.into();

        // If the location is fails to parse as a URL, we assume it's a filesystem path.
        Self::try_from_url(&location).or_else(|_| Self::try_from_filesystem_path(&location, base))
    }

    /// Parses a string as a URL and validates it for object store use.
    ///
    /// The URL must have a supported object store scheme (`file://`, `s3://`, `gs://`, etc.).
    fn try_from_url(url: impl AsRef<str>) -> Result<Self, ObjectStoreUrlError> {
        let inner = Url::parse(url.as_ref()).map_err(ObjectStoreUrlError::UrlParseError)?;

        // Validate the scheme to ensure it's a supported object store scheme
        let _: ObjectStoreProvider = inner.scheme().parse()?;

        Ok(Self(inner))
    }

    /// Parses a string as a filesystem path and converts it to a `file://` URL.
    ///
    /// If the path is relative, `base` will be used as the prefix, otherwise
    /// `base` is ignored.
    ///
    /// The path must exist and must be canonicalized to an absolute path.
    fn try_from_filesystem_path(
        location: impl AsRef<str>,
        base: Option<&std::path::Path>,
    ) -> Result<Self, ObjectStoreUrlError> {
        let mut path = PathBuf::from(location.as_ref());
        if !path.is_absolute()
            && let Some(base) = base
        {
            path = PathBuf::from(base).join(path);
        }

        let canonical_path = fs::canonicalize(&path)
            .map_err(|err| ObjectStoreUrlError::InvalidFilesystemPath { path, source: err })?;

        let url = Url::from_directory_path(&canonical_path).map_err(|_| {
            ObjectStoreUrlError::InvalidFilesystemPath {
                path: canonical_path.clone(),
                source: std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "absolute path could not be resolved",
                ),
            }
        })?;

        Ok(Self(url))
    }

    /// Creates a new [`ObjectStoreUrl`] without validation.
    ///
    /// # Safety
    /// The caller must ensure that the URL is a valid object store URL.
    pub fn new_unchecked(url: Url) -> Self {
        Self(url)
    }

    /// Consumes self and returns the inner [`URL`].
    pub fn into_url(self) -> Url {
        self.0
    }

    /// Returns the [`ObjectStoreProvider`] of the URL.
    pub fn provider(&self) -> ObjectStoreProvider {
        match self.0.scheme() {
            s if ObjectStoreProvider::is_local_filesystem(s) => ObjectStoreProvider::Local,
            s if ObjectStoreProvider::is_amazon_s3(s) => ObjectStoreProvider::AmazonS3,
            s if ObjectStoreProvider::is_google_cloud_storage(s) => {
                ObjectStoreProvider::GoogleCloudStorage
            }
            s if ObjectStoreProvider::is_microsoft_azure(s) => ObjectStoreProvider::MicrosoftAzure,
            _ => unreachable!(), // We validate the scheme previously.
        }
    }
}

impl AsRef<ObjectStoreUrl> for ObjectStoreUrl {
    fn as_ref(&self) -> &ObjectStoreUrl {
        self
    }
}

impl std::ops::Deref for ObjectStoreUrl {
    type Target = Url;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<Url> for ObjectStoreUrl {
    type Error = ObjectStoreUrlError;

    fn try_from(value: Url) -> Result<Self, Self::Error> {
        Self::try_from_url(value)
    }
}

impl std::str::FromStr for ObjectStoreUrl {
    type Err = ObjectStoreUrlError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Url::parse(s)
            .map_err(ObjectStoreUrlError::UrlParseError)?
            .try_into()
    }
}

impl std::fmt::Display for ObjectStoreUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::fmt::Debug for ObjectStoreUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl From<&ObjectStoreUrl> for String {
    fn from(value: &ObjectStoreUrl) -> Self {
        value.to_string()
    }
}

/// Supported object store providers.
///
/// This is a subset of the object_store crate's ObjectStoreScheme that only includes
/// the providers we actually support.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ObjectStoreProvider {
    /// Local filesystem (`file://`)
    Local,

    /// Amazon S3 (`s3://`, `s3a://`)
    AmazonS3,

    /// Google Cloud Storage (`gs://`)
    GoogleCloudStorage,

    /// Microsoft Azure (`az://`, `adl://`, `azure://`, `abfs://`, `abfss://`)
    MicrosoftAzure,
}

impl ObjectStoreProvider {
    /// Returns `true` if the scheme is one of:
    /// - `s3://`
    /// - `s3a://`
    ///
    /// This is used for Amazon S3 and S3-compatible object stores.
    #[inline]
    fn is_amazon_s3(s: impl AsRef<str>) -> bool {
        let scheme = s.as_ref();
        scheme.eq_ignore_ascii_case("s3") || scheme.eq_ignore_ascii_case("s3a")
    }

    /// Returns `true` if the scheme is `gs://`.
    ///
    /// This is used for Google Cloud Storage.
    #[inline]
    fn is_google_cloud_storage(s: impl AsRef<str>) -> bool {
        s.as_ref().eq_ignore_ascii_case("gs")
    }

    /// Returns `true` if the scheme is one of:
    /// - `az://`
    /// - `adl://`
    /// - `azure://`
    /// - `abfs://`
    /// - `abfss://`
    ///
    /// This is used for Microsoft Azure Blob Storage.
    #[inline]
    fn is_microsoft_azure(s: impl AsRef<str>) -> bool {
        let scheme = s.as_ref();
        scheme.eq_ignore_ascii_case("az")
            || scheme.eq_ignore_ascii_case("adl")
            || scheme.eq_ignore_ascii_case("azure")
            || scheme.eq_ignore_ascii_case("abfs")
            || scheme.eq_ignore_ascii_case("abfss")
    }

    /// Returns `true` if the scheme is `file://`.
    ///
    /// This is used for local filesystem paths.
    #[inline]
    fn is_local_filesystem(s: impl AsRef<str>) -> bool {
        s.as_ref().eq_ignore_ascii_case("file")
    }
}

impl From<ObjectStoreProvider> for DatafusionObjectStoreScheme {
    fn from(value: ObjectStoreProvider) -> Self {
        match value {
            ObjectStoreProvider::Local => DatafusionObjectStoreScheme::Local,
            ObjectStoreProvider::AmazonS3 => DatafusionObjectStoreScheme::AmazonS3,
            ObjectStoreProvider::GoogleCloudStorage => {
                DatafusionObjectStoreScheme::GoogleCloudStorage
            }
            ObjectStoreProvider::MicrosoftAzure => DatafusionObjectStoreScheme::MicrosoftAzure,
        }
    }
}

impl std::str::FromStr for ObjectStoreProvider {
    type Err = ObjectStoreUrlError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            s if ObjectStoreProvider::is_local_filesystem(s) => Ok(ObjectStoreProvider::Local),
            s if ObjectStoreProvider::is_amazon_s3(s) => Ok(ObjectStoreProvider::AmazonS3),
            s if ObjectStoreProvider::is_google_cloud_storage(s) => {
                Ok(ObjectStoreProvider::GoogleCloudStorage)
            }
            s if ObjectStoreProvider::is_microsoft_azure(s) => {
                Ok(ObjectStoreProvider::MicrosoftAzure)
            }
            _ => Err(ObjectStoreUrlError::UnsupportedScheme {
                scheme: s.to_string(),
            }),
        }
    }
}

/// Error type for invalid object store URLs.
///
/// This error is returned when attempting to validate a URL for use with object stores.
/// Valid object store schemes are: `gs`, `s3`, `azure`, and `file`.
///
/// Note: If you are attempting to configure an S3-compatible object store with HTTP/HTTPS,
/// please use the `s3://` scheme and configure AWS_ENDPOINT instead.
#[derive(Debug, thiserror::Error)]
pub enum ObjectStoreUrlError {
    /// URL string has invalid syntax and cannot be parsed
    ///
    /// This error occurs when the provided string cannot be parsed as a valid URL according
    /// to the URL specification (RFC 3986). The string is malformed and does not conform to
    /// the expected URL structure.
    ///
    /// Common causes:
    /// - Missing scheme separator (e.g., `s3//bucket` instead of `s3://bucket`)
    /// - Invalid characters in the URL components
    /// - Malformed port numbers
    /// - Empty or whitespace-only strings
    /// - Invalid Unicode characters that cannot be percent-encoded
    ///
    /// The underlying `url::ParseError` provides specific details about what went wrong
    /// during parsing.
    #[error(transparent)]
    UrlParseError(url::ParseError),

    /// URL scheme is not supported by object stores
    ///
    /// This error occurs when attempting to create an object store URL with a scheme that
    /// is not supported by the object store implementations. While the URL syntax itself
    /// may be valid, the scheme does not correspond to any supported storage backend.
    ///
    /// Supported schemes:
    /// - `file://` - Local filesystem
    /// - `s3://` or `s3a://` - Amazon S3 and S3-compatible stores
    /// - `gs://` - Google Cloud Storage
    /// - `az://`, `adl://`, `azure://`, `abfs://`, `abfss://` - Microsoft Azure Blob Storage
    ///
    /// Unsupported schemes (common mistakes):
    /// - `http://` or `https://` - Use `s3://` with `AWS_ENDPOINT` environment variable instead
    /// - `ftp://` - Not supported
    /// - `memory://` - Not supported
    /// - Custom schemes - Not supported
    ///
    /// If you need to use an S3-compatible object store with a custom HTTP/HTTPS endpoint,
    /// configure the `AWS_ENDPOINT` environment variable and use the `s3://` scheme.
    #[error("unsupported object store scheme: {scheme}")]
    UnsupportedScheme { scheme: String },

    /// Filesystem path does not exist or cannot be accessed
    ///
    /// This error occurs when attempting to create an object store URL from a filesystem
    /// path that either does not exist or cannot be canonicalized to an absolute path.
    /// The path must exist and be accessible for the object store to operate on it.
    ///
    /// Common causes:
    /// - Path does not exist on the filesystem
    /// - Insufficient permissions to read the path
    /// - Path contains symbolic links that cannot be resolved
    /// - Path is on an unmounted or inaccessible filesystem
    /// - Path exceeds system limits (e.g., too long)
    /// - Path contains invalid characters for the operating system
    ///
    /// The path must be created (e.g., via `std::fs::create_dir_all()`) before attempting
    /// to create an object store URL from it.
    #[error("invalid filesystem path: {path}")]
    InvalidFilesystemPath {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_supported_object_store_providers() {
        //* Given
        let file_scheme = "file";
        let s3_scheme = "s3";
        let s3a_scheme = "s3a";
        let gs_scheme = "gs";
        let az_scheme = "az";
        let adl_scheme = "adl";
        let azure_scheme = "azure";
        let abfs_scheme = "abfs";
        let abfss_scheme = "abfss";

        //* When
        let file_result = file_scheme
            .parse::<ObjectStoreProvider>()
            .expect("Failed to parse file scheme");
        let s3_result = s3_scheme
            .parse::<ObjectStoreProvider>()
            .expect("Failed to parse s3 scheme");
        let s3a_result = s3a_scheme
            .parse::<ObjectStoreProvider>()
            .expect("Failed to parse s3a scheme");
        let gs_result = gs_scheme
            .parse::<ObjectStoreProvider>()
            .expect("Failed to parse gs scheme");
        let az_result = az_scheme
            .parse::<ObjectStoreProvider>()
            .expect("Failed to parse az scheme");
        let adl_result = adl_scheme
            .parse::<ObjectStoreProvider>()
            .expect("Failed to parse adl scheme");
        let azure_result = azure_scheme
            .parse::<ObjectStoreProvider>()
            .expect("Failed to parse azure scheme");
        let abfs_result = abfs_scheme
            .parse::<ObjectStoreProvider>()
            .expect("Failed to parse abfs scheme");
        let abfss_result = abfss_scheme
            .parse::<ObjectStoreProvider>()
            .expect("Failed to parse abfss scheme");

        //* Then
        assert_eq!(file_result, ObjectStoreProvider::Local);
        assert_eq!(s3_result, ObjectStoreProvider::AmazonS3);
        assert_eq!(s3a_result, ObjectStoreProvider::AmazonS3);
        assert_eq!(gs_result, ObjectStoreProvider::GoogleCloudStorage);
        assert_eq!(az_result, ObjectStoreProvider::MicrosoftAzure);
        assert_eq!(adl_result, ObjectStoreProvider::MicrosoftAzure);
        assert_eq!(azure_result, ObjectStoreProvider::MicrosoftAzure);
        assert_eq!(abfs_result, ObjectStoreProvider::MicrosoftAzure);
        assert_eq!(abfss_result, ObjectStoreProvider::MicrosoftAzure);
    }

    #[test]
    fn parse_unsupported_object_store_schemes() {
        //* Given
        let http_scheme = "http";
        let https_scheme = "https";
        let ftp_scheme = "ftp";
        let memory_scheme = "memory";

        //* When
        let http_result = http_scheme
            .parse::<ObjectStoreProvider>()
            .expect_err("Expected http scheme to fail");
        let https_result = https_scheme
            .parse::<ObjectStoreProvider>()
            .expect_err("Expected https scheme to fail");
        let ftp_result = ftp_scheme
            .parse::<ObjectStoreProvider>()
            .expect_err("Expected ftp scheme to fail");
        let memory_result = memory_scheme
            .parse::<ObjectStoreProvider>()
            .expect_err("Expected memory scheme to fail");

        //* Then
        assert!(matches!(
            http_result,
            ObjectStoreUrlError::UnsupportedScheme { .. }
        ));
        assert!(matches!(
            https_result,
            ObjectStoreUrlError::UnsupportedScheme { .. }
        ));
        assert!(matches!(
            ftp_result,
            ObjectStoreUrlError::UnsupportedScheme { .. }
        ));
        assert!(matches!(
            memory_result,
            ObjectStoreUrlError::UnsupportedScheme { .. }
        ));
    }

    #[test]
    fn create_object_store_url_from_s3_url() {
        //* Given
        let s3_url = "s3://my-bucket/path/";

        //* When
        let store_url =
            ObjectStoreUrl::new(s3_url).expect("Failed to create ObjectStoreUrl from S3 URL");

        //* Then
        assert_eq!(store_url.provider(), ObjectStoreProvider::AmazonS3);
        assert_eq!(store_url.scheme(), "s3");
        assert_eq!(store_url.host_str(), Some("my-bucket"));
        assert_eq!(store_url.path(), "/path/");
    }

    #[test]
    fn create_object_store_url_from_gs_url() {
        //* Given
        let gs_url = "gs://my-bucket/data/";

        //* When
        let store_url =
            ObjectStoreUrl::new(gs_url).expect("Failed to create ObjectStoreUrl from GS URL");

        //* Then
        assert_eq!(
            store_url.provider(),
            ObjectStoreProvider::GoogleCloudStorage
        );
        assert_eq!(store_url.scheme(), "gs");
        assert_eq!(store_url.host_str(), Some("my-bucket"));
        assert_eq!(store_url.path(), "/data/");
    }

    #[test]
    fn create_object_store_url_from_azure_url() {
        //* Given
        let azure_url = "az://container/path/";

        //* When
        let store_url =
            ObjectStoreUrl::new(azure_url).expect("Failed to create ObjectStoreUrl from Azure URL");

        //* Then
        assert_eq!(store_url.provider(), ObjectStoreProvider::MicrosoftAzure);
        assert_eq!(store_url.scheme(), "az");
        assert_eq!(store_url.host_str(), Some("container"));
        assert_eq!(store_url.path(), "/path/");
    }

    #[test]
    fn create_object_store_url_preserves_path_without_trailing_slash() {
        //* Given
        let s3_url_without_slash = "s3://my-bucket/path";

        //* When
        let store_url =
            ObjectStoreUrl::new(s3_url_without_slash).expect("Failed to create ObjectStoreUrl");

        //* Then
        assert_eq!(store_url.path(), "/path");
    }

    #[test]
    fn create_object_store_url_from_case_insensitive_scheme() {
        //* Given
        let uppercase_s3_url = "S3://my-bucket/path/";
        let mixed_case_gs_url = "Gs://my-bucket/data/";

        //* When
        let s3_store_url = ObjectStoreUrl::new(uppercase_s3_url)
            .expect("Failed to create ObjectStoreUrl from uppercase S3 URL");
        let gs_store_url = ObjectStoreUrl::new(mixed_case_gs_url)
            .expect("Failed to create ObjectStoreUrl from mixed case GS URL");

        //* Then
        assert_eq!(s3_store_url.provider(), ObjectStoreProvider::AmazonS3);
        assert_eq!(
            gs_store_url.provider(),
            ObjectStoreProvider::GoogleCloudStorage
        );
    }

    #[test]
    fn try_from_url_for_object_store_url() {
        //* Given
        let url = Url::parse("s3://test-bucket/prefix/").expect("Failed to parse test URL");

        //* When
        let store_url =
            ObjectStoreUrl::try_from(url.clone()).expect("Failed to convert URL to ObjectStoreUrl");

        //* Then
        assert_eq!(&*store_url, &url);
        assert_eq!(store_url.provider(), ObjectStoreProvider::AmazonS3);
    }

    #[test]
    fn from_str_for_object_store_url() {
        //* Given
        let url_str = "gs://my-bucket/data/";

        //* When
        let store_url = url_str
            .parse::<ObjectStoreUrl>()
            .expect("Failed to parse ObjectStoreUrl from string");

        //* Then
        assert_eq!(
            store_url.provider(),
            ObjectStoreProvider::GoogleCloudStorage
        );
        assert_eq!(store_url.to_string(), url_str);
    }

    #[test]
    fn create_object_store_url_from_file_url() {
        //* Given
        let temp_dir = tempfile::tempdir().expect("Failed to create temp directory");
        let file_url = format!("file://{}/", temp_dir.path().display());

        //* When
        let store_url = ObjectStoreUrl::new(file_url.clone())
            .expect("Failed to create ObjectStoreUrl from file URL");

        //* Then
        assert_eq!(store_url.provider(), ObjectStoreProvider::Local);
        assert_eq!(store_url.scheme(), "file");
        assert_eq!(store_url.to_string(), file_url);
    }

    #[test]
    fn create_object_store_url_from_absolute_path() {
        //* Given
        let temp_dir = tempfile::tempdir().expect("Failed to create temp directory");
        let absolute_path = temp_dir
            .path()
            .to_str()
            .expect("Failed to convert path to string");

        //* When
        let store_url = ObjectStoreUrl::new(absolute_path)
            .expect("Failed to create ObjectStoreUrl from absolute path");

        //* Then
        assert_eq!(store_url.provider(), ObjectStoreProvider::Local);
        assert_eq!(store_url.scheme(), "file");
        assert!(store_url.path().contains(absolute_path));
    }

    #[test]
    fn create_object_store_url_from_relative_path_without_base() {
        //* Given
        let temp_dir = tempfile::tempdir().expect("Failed to create temp directory");
        let subdir_name = "test-subdir";
        let subdir_path = temp_dir.path().join(subdir_name);
        std::fs::create_dir(&subdir_path).expect("Failed to create subdirectory");

        // Change to temp directory so relative path resolves correctly
        let original_dir = std::env::current_dir().expect("Failed to get current directory");
        std::env::set_current_dir(temp_dir.path()).expect("Failed to change directory");

        //* When
        let store_url = ObjectStoreUrl::new(subdir_name)
            .expect("Failed to create ObjectStoreUrl from relative path");

        //* Then
        assert_eq!(store_url.provider(), ObjectStoreProvider::Local);
        assert_eq!(store_url.scheme(), "file");

        //* Cleanup
        // Restore original directory
        std::env::set_current_dir(original_dir).expect("Failed to restore directory");
    }

    #[test]
    fn create_object_store_url_from_relative_path_with_base() {
        //* Given
        let temp_dir = tempfile::tempdir().expect("Failed to create temp directory");
        let subdir_name = "test-subdir";
        let subdir_path = temp_dir.path().join(subdir_name);
        std::fs::create_dir(&subdir_path).expect("Failed to create subdirectory");
        let base_path = temp_dir.path();

        //* When
        let store_url = ObjectStoreUrl::new_with_base(subdir_name, Some(base_path))
            .expect("Failed to create ObjectStoreUrl from relative path with base");

        //* Then
        assert_eq!(store_url.provider(), ObjectStoreProvider::Local);
        assert_eq!(store_url.scheme(), "file");
        assert!(store_url.path().contains(subdir_name));
    }

    #[test]
    fn create_object_store_url_from_nested_relative_path_with_base() {
        //* Given
        let temp_dir = tempfile::tempdir().expect("Failed to create temp directory");
        let nested_path = "level1/level2";
        let full_nested_path = temp_dir.path().join(nested_path);
        std::fs::create_dir_all(&full_nested_path).expect("Failed to create nested directories");
        let base_path = temp_dir.path();

        //* When
        let store_url = ObjectStoreUrl::new_with_base(nested_path, Some(base_path))
            .expect("Failed to create ObjectStoreUrl from nested relative path");

        //* Then
        assert_eq!(store_url.provider(), ObjectStoreProvider::Local);
        assert_eq!(store_url.scheme(), "file");
        assert!(store_url.path().contains("level1"));
        assert!(store_url.path().contains("level2"));
    }

    #[test]
    fn create_object_store_url_from_nonexistent_path_fails() {
        //* Given
        let nonexistent_path = "/this/path/does/not/exist/hopefully";

        //* When
        let result =
            ObjectStoreUrl::new(nonexistent_path).expect_err("Expected nonexistent path to fail");

        //* Then
        assert!(matches!(
            result,
            ObjectStoreUrlError::InvalidFilesystemPath { .. }
        ));
    }

    #[test]
    fn create_object_store_url_from_relative_nonexistent_path_with_base_fails() {
        //* Given
        let temp_dir = tempfile::tempdir().expect("Failed to create temp directory");
        let nonexistent_relative = "does/not/exist";
        let base_path = temp_dir.path();

        //* When
        let result = ObjectStoreUrl::new_with_base(nonexistent_relative, Some(base_path))
            .expect_err("Expected nonexistent relative path to fail");

        //* Then
        assert!(matches!(
            result,
            ObjectStoreUrlError::InvalidFilesystemPath { .. }
        ));
    }

    #[test]
    fn create_object_store_url_from_filesystem_path() {
        //* Given
        let temp_dir = tempfile::tempdir().expect("Failed to create temp directory");
        let path_without_slash = temp_dir
            .path()
            .to_str()
            .expect("Failed to convert path to string")
            .trim_end_matches('/');

        //* When
        let store_url = ObjectStoreUrl::new(path_without_slash)
            .expect("Failed to create ObjectStoreUrl from path without slash");

        //* Then
        assert_eq!(store_url.provider(), ObjectStoreProvider::Local);
        // Note: Url::from_directory_path automatically adds a trailing slash for directories
        assert!(store_url.path().ends_with('/'));
    }
}

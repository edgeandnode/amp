use std::{future::Future, path::PathBuf, sync::Arc};

use bytes::Bytes;
use fs_err as fs;
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use object_store::{
    ObjectMeta, ObjectStore, ObjectStoreScheme,
    aws::{AmazonS3Builder, AmazonS3ConfigKey},
    azure::{AzureConfigKey, MicrosoftAzureBuilder},
    gcp::{GoogleCloudStorageBuilder, GoogleConfigKey},
    local::LocalFileSystem,
    path::Path,
    prefix::PrefixStore,
};
use url::Url;

/// A wrapper around an `ObjectStore`. There are a few things it helps us with over a plain
/// `ObjectStore`:
/// - Keeps track of the URL of the store, in case we need it.
/// - Tries to better handle various cases of relative paths and path prefixes.
/// - Can be extended with helper functions.
#[derive(Debug, Clone)]
pub struct Store {
    url: ObjectStoreUrl,
    prefix: String,
    store: Arc<PrefixStore<Arc<dyn ObjectStore>>>,
    unprefixed: Arc<dyn ObjectStore>,

    /// `bucket` is `None` for local filesystem stores.
    bucket: Option<String>,
}

impl Store {
    /// Creates a store for an object store URL (or filesystem directory).
    ///
    /// Examples of valid formats for `data_location`:
    /// - Filesystem path: `relative/path/to/data/`
    /// - GCS: `gs://bucket-name`
    /// - S3: `s3://bucket-name`
    /// - Prefixed: `s3://bucket-name/my_prefix/`
    ///
    /// If `data_location` is a relative filesystem path, then `base` will be used as the prefix.
    pub fn new(url: ObjectStoreUrl) -> Result<Self, StoreError> {
        let (unprefixed, bucket) =
            object_store(&url).map_err(|err| StoreError::ObjectStoreCreation {
                url: url.to_string(),
                err,
            })?;
        let prefix = url.path().to_string();
        let store = Arc::new(PrefixStore::new(unprefixed.clone(), prefix.as_str()));
        Ok(Self {
            url,
            prefix,
            store,
            unprefixed,
            bucket,
        })
    }

    pub fn url(&self) -> &Url {
        &self.url
    }

    /// A store that will search for paths relative to the location provided in the constructor, as
    /// would be expected.
    pub fn prefixed_store(&self) -> Arc<dyn ObjectStore> {
        self.store.clone()
    }

    /// Returns the unprefixed store. Use this when you have a URL which already contains the prefix.
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.unprefixed.clone()
    }

    pub async fn get_bytes(&self, location: impl Into<Path>) -> Result<Bytes, StoreError> {
        self.store.get_bytes(location).await
    }

    pub async fn get_string(&self, location: impl Into<Path>) -> Result<String, StoreError> {
        self.store.get_string(location).await
    }

    pub fn list(&self, prefix: impl Into<Path>) -> BoxStream<'_, Result<ObjectMeta, StoreError>> {
        self.store
            .list(Some(&prefix.into()))
            .map_err(|e| e.into())
            .boxed()
    }

    pub async fn list_all_shallow(&self) -> Result<Vec<ObjectMeta>, StoreError> {
        Ok(self.store.list_with_delimiter(None).await?.objects)
    }

    pub fn bucket(&self) -> Option<&str> {
        self.bucket.as_deref()
    }
}

impl std::fmt::Display for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "store with prefix {}", self.prefix)
    }
}

/// Creates an object store instance from a validated URL.
///
/// This function takes an [`ObjectStoreUrl`] and returns the appropriate object store
/// implementation based on the URL scheme. The returned tuple contains:
/// - An `Arc<dyn ObjectStore>` - the object store implementation
/// - An `Option<String>` - the bucket/container name, or `None` for local filesystem
///
/// See [`ObjectStoreUrl`] and [`ObjectStoreProvider`] for more details on supported schemes.
///
/// # Providers configuration
///
/// Cloud providers are configured via environment variables:
/// - **AWS S3**: Uses `AWS_*` environment variables (e.g., `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
/// - **Google Cloud**: Uses `GOOGLE_*` environment variables or service account credentials
/// - **Azure**: Uses `AZURE_*` environment variables (e.g., `AZURE_STORAGE_ACCOUNT_NAME`)
pub fn object_store(
    url: &ObjectStoreUrl,
) -> Result<(Arc<dyn ObjectStore>, Option<String>), ObjectStoreCreationError> {
    match url.provider() {
        ObjectStoreProvider::GoogleCloudStorage => {
            let builder = GoogleCloudStorageBuilder::from_env().with_url(url);
            let bucket = builder.get_config_value(&GoogleConfigKey::Bucket);
            let store = builder.build()?;
            Ok((Arc::new(store), bucket))
        }
        ObjectStoreProvider::AmazonS3 => {
            let builder = AmazonS3Builder::from_env().with_url(url);
            let bucket = builder.get_config_value(&AmazonS3ConfigKey::Bucket);
            let store = builder.build()?;
            Ok((Arc::new(store), bucket))
        }
        ObjectStoreProvider::MicrosoftAzure => {
            let builder = MicrosoftAzureBuilder::from_env().with_url(url);
            let bucket = builder.get_config_value(&AzureConfigKey::ContainerName);
            let store = builder.build()?;
            Ok((Arc::new(store), bucket))
        }
        ObjectStoreProvider::Local => {
            let store = LocalFileSystem::new();
            Ok((Arc::new(store), None))
        }
    }
}

/// Error type for object store creation failures.
///
/// This error occurs when attempting to create an object store instance from a valid URL.
/// Common causes include missing credentials, network issues, or configuration problems.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct ObjectStoreCreationError(#[from] object_store::Error);

/// Extension trait for `ObjectStore` that provides convenient methods for common operations.
///
/// This trait adds helper methods to any type implementing `ObjectStore`, providing
/// more ergonomic APIs for reading data from object stores.
pub trait ObjectStoreExt {
    /// Reads the entire contents of an object as `Bytes`.
    ///
    /// This is a convenience method that combines `ObjectStore::get()` and reading
    /// all bytes from the returned stream.
    fn get_bytes(
        &self,
        location: impl Into<Path>,
    ) -> impl Future<Output = Result<Bytes, StoreError>>;

    /// Reads the entire contents of an object as a UTF-8 string.
    ///
    /// This method reads the object's contents and attempts to decode them as UTF-8.
    /// Returns `StoreError::NotUtf8` if the contents are not valid UTF-8.
    fn get_string(
        &self,
        location: impl Into<Path>,
    ) -> impl Future<Output = Result<String, StoreError>>;
}

impl<T> ObjectStoreExt for T
where
    T: ObjectStore,
{
    async fn get_bytes(&self, location: impl Into<Path>) -> Result<Bytes, StoreError> {
        Ok(self.get(&location.into()).await?.bytes().await?)
    }

    async fn get_string(&self, location: impl Into<Path>) -> Result<String, StoreError> {
        let path = location.into();
        let bytes = self.get_bytes(path.clone()).await?;
        String::from_utf8(bytes.to_vec()).map_err(|err| StoreError::NotUtf8 {
            path: path.to_string(),
            err,
        })
    }
}
/// Error type for store operations.
///
/// This error encompasses all failures that can occur when working with object stores,
/// from creation to runtime operations like reading and writing files.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// Failed to create the object store instance.
    ///
    /// This wraps [`ObjectStoreCreationError`] and occurs during store initialization.
    /// Common causes: missing credentials, invalid configuration, or network issues.
    #[error("failed to create object store for {url}: {err}")]
    ObjectStoreCreation {
        url: String,
        #[source]
        err: ObjectStoreCreationError,
    },

    /// File contents are not valid UTF-8 text.
    ///
    /// This occurs when calling `get_string()` on a file that contains binary data
    /// or text in a non-UTF-8 encoding.
    #[error("object is not an utf8 text file: {path}")]
    NotUtf8 {
        /// The path of the file that is not valid UTF-8.
        path: String,
        #[source]
        err: std::string::FromUtf8Error,
    },

    /// Runtime error from the underlying object store implementation.
    ///
    /// Common causes: network timeouts, permission denied, file not found,
    /// service unavailable, or authentication token expiry.
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
}

impl StoreError {
    pub fn is_not_found(&self) -> bool {
        matches!(
            self,
            StoreError::ObjectStore(object_store::Error::NotFound { .. })
        )
    }
}

/// A validated URL wrapper that ensures the URL is a valid object store URL.
#[derive(Clone, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct ObjectStoreUrl(Url);

impl ObjectStoreUrl {
    /// Creates a new ObjectStoreUrl from a string that can be either a URL or filesystem path.
    pub fn new(location: impl Into<String>) -> Result<Self, InvalidObjectStoreUrlError> {
        Self::new_with_base(location, None)
    }

    /// Creates a new [`ObjectStoreUrl`] from a string that can be either a URL or filesystem path.
    ///
    /// If `data_location` is a relative filesystem path, then `base` will be used as the prefix.
    pub fn new_with_base(
        location: impl Into<String>,
        base: Option<&std::path::Path>,
    ) -> Result<Self, InvalidObjectStoreUrlError> {
        let mut location = location.into();
        if !location.ends_with('/') {
            location.push('/');
        }

        // If the location is fails to parse as a URL, we assume it's a filesystem path.
        Self::try_from_url(&location).or_else(|_| Self::try_from_filesystem_path(&location, base))
    }

    /// Parses a string as a URL and validates it for object store use.
    ///
    /// The URL must have a supported object store scheme (`file://`, `s3://`, `gs://`, etc.).
    /// The input URL should have a trailing slash to indicate it's a directory/container.
    fn try_from_url(url: impl AsRef<str>) -> Result<Self, InvalidObjectStoreUrlError> {
        let inner = Url::parse(url.as_ref())?;

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
    /// The input location should have a trailing slash to indicate it's a directory.
    fn try_from_filesystem_path(
        location: impl AsRef<str>,
        base: Option<&std::path::Path>,
    ) -> Result<Self, InvalidObjectStoreUrlError> {
        let mut path = PathBuf::from(location.as_ref());
        if !path.is_absolute()
            && let Some(base) = base
        {
            path = PathBuf::from(base).join(path);
        }

        let canonical_path = fs::canonicalize(&path)
            .map_err(|err| InvalidObjectStoreUrlError::InvalidFilesystemPath { path, err })?;

        let url = Url::from_directory_path(&canonical_path).map_err(|_| {
            InvalidObjectStoreUrlError::InvalidFilesystemPath {
                path: canonical_path.clone(),
                err: std::io::Error::new(
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
    pub unsafe fn new_unchecked(url: Url) -> Self {
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

impl std::ops::Deref for ObjectStoreUrl {
    type Target = Url;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<Url> for ObjectStoreUrl {
    type Error = InvalidObjectStoreUrlError;

    fn try_from(value: Url) -> Result<Self, Self::Error> {
        Self::try_from_url(value)
    }
}

impl std::str::FromStr for ObjectStoreUrl {
    type Err = InvalidObjectStoreUrlError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Url::parse(s)?.try_into()
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

impl From<ObjectStoreProvider> for ObjectStoreScheme {
    fn from(value: ObjectStoreProvider) -> Self {
        match value {
            ObjectStoreProvider::Local => ObjectStoreScheme::Local,
            ObjectStoreProvider::AmazonS3 => ObjectStoreScheme::AmazonS3,
            ObjectStoreProvider::GoogleCloudStorage => ObjectStoreScheme::GoogleCloudStorage,
            ObjectStoreProvider::MicrosoftAzure => ObjectStoreScheme::MicrosoftAzure,
        }
    }
}

impl std::str::FromStr for ObjectStoreProvider {
    type Err = InvalidObjectStoreUrlError;

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
            _ => Err(InvalidObjectStoreUrlError::UnsupportedScheme {
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
pub enum InvalidObjectStoreUrlError {
    /// The URL format is invalid.
    #[error(transparent)]
    UrlParseError(#[from] url::ParseError),

    /// The URL scheme is not supported by object stores.
    #[error("unsupported object store scheme: {scheme}")]
    UnsupportedScheme { scheme: String },

    /// Invalid filesystem path.
    #[error("invalid filesystem path '{path}': {err}")]
    InvalidFilesystemPath {
        path: PathBuf,
        #[source]
        err: std::io::Error,
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
            InvalidObjectStoreUrlError::UnsupportedScheme { .. }
        ));
        assert!(matches!(
            https_result,
            InvalidObjectStoreUrlError::UnsupportedScheme { .. }
        ));
        assert!(matches!(
            ftp_result,
            InvalidObjectStoreUrlError::UnsupportedScheme { .. }
        ));
        assert!(matches!(
            memory_result,
            InvalidObjectStoreUrlError::UnsupportedScheme { .. }
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
    fn create_object_store_url_automatically_adds_trailing_slash() {
        //* Given
        let s3_url_without_slash = "s3://my-bucket/path";

        //* When
        let store_url =
            ObjectStoreUrl::new(s3_url_without_slash).expect("Failed to create ObjectStoreUrl");

        //* Then
        assert_eq!(store_url.path(), "/path/");
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
            InvalidObjectStoreUrlError::InvalidFilesystemPath { .. }
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
            InvalidObjectStoreUrlError::InvalidFilesystemPath { .. }
        ));
    }

    #[test]
    fn create_object_store_url_filesystem_paths_automatically_add_trailing_slash() {
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
        assert!(store_url.path().ends_with('/'));
    }
}

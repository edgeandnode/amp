use std::{ops::Range, path::PathBuf, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use fs_err as fs;
use futures::stream::BoxStream;
use object_store::{
    aws::AmazonS3Builder, gcp::GoogleCloudStorageBuilder, local::LocalFileSystem, path::Path,
    prefix::PrefixStore, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult,
};
use url::Url;

use crate::BoxError;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("object store error: {0}")]
    ObjectStore(#[from] object_store::Error),
    #[error("object is not an utf8 text file: {0}")]
    NotUtf8(String),
}

/// A wrapper around an `ObjectStore`. There are a few things it helps us with over a plain
/// `ObjectStore`:
/// - Keeps track of the URL of the store, in case we need it.
/// - When constructed with: `gs://bucket/subidr/` will preserve the `subdir` when doing any
///   requests.
/// - Can be extended with helper functions.
#[derive(Debug, Clone)]
pub struct Store {
    url: Url,
    prefix: String,
    store: Arc<PrefixStore<Box<dyn ObjectStore>>>,
}

impl Store {
    /// Creates a store for an object store path or filesystem directory. Examples of valid formats
    /// for `data_location`:
    /// - Filesystem path: `relative/path/to/data/`
    /// - GCS: `gs://bucket-name/`
    /// - S3: `s3://bucket-name/`
    ///
    /// If `data_location` is not a URL, but a relative path, then `base` will be used as the prefix.
    pub fn new(data_location: String, base: Option<&std::path::Path>) -> Result<Self, BoxError> {
        let (url, object_store) = infer_object_store(data_location, base)?;

        let prefix = match url.scheme() {
            // Don't set a prefix for file URLs, as DataFusion will preserve the full path.
            "file" => String::new(),

            // For object store URLs, set the prefix to the path part of the URL.
            _ => url.path().to_string(),
        };

        let store = Arc::new(PrefixStore::new(object_store, prefix.as_str()));
        Ok(Self { url, prefix, store })
    }

    pub fn in_memory() -> Self {
        let url = Url::parse("memory://in_memory_store/").unwrap();
        let store = Box::new(object_store::memory::InMemory::new());
        Self {
            url,
            prefix: "".to_string(),
            store: Arc::new(PrefixStore::new(store, "")),
        }
    }

    pub fn url(&self) -> &Url {
        &self.url
    }

    pub async fn get_bytes(&self, location: impl Into<Path>) -> Result<Bytes, StoreError> {
        Ok(self.store.get(&location.into()).await?.bytes().await?)
    }

    pub async fn get_string(&self, location: impl Into<Path>) -> Result<String, StoreError> {
        let path = location.into();
        let bytes = self.get_bytes(path.clone()).await?;
        String::from_utf8(bytes.to_vec()).map_err(|_| StoreError::NotUtf8(path.to_string()))
    }
}

impl std::fmt::Display for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "store with prefix {}", self.prefix)
    }
}

/// Delegated impl to the internal store.
#[async_trait]
impl ObjectStore for Store {
    async fn put(&self, location: &Path, payload: PutPayload) -> object_store::Result<PutResult> {
        self.store.put(location, payload).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.store.put_opts(location, payload, opts).await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.store.put_multipart(location).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.store.put_multipart_opts(location, opts).await
    }

    async fn get(&self, location: &Path) -> object_store::Result<GetResult> {
        self.store.get(location).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.store.get_opts(location, options).await
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> object_store::Result<Bytes> {
        self.store.get_range(location, range).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<usize>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.store.get_ranges(location, ranges).await
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        self.store.head(location).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.store.delete(location).await
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, object_store::Result<Path>>,
    ) -> BoxStream<'a, object_store::Result<Path>> {
        self.store.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        self.store.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        self.store.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.store.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.store.copy(from, to).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.store.rename(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.store.copy_if_not_exists(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.store.rename_if_not_exists(from, to).await
    }
}

/// The location must be a directory. Examples of valid formats for `data_location`:
/// - Filesystem path: `relative/path/to/data/`
/// - GCS: `gs://bucket-name/`
/// - S3: `s3://bucket-name/`
///
/// `base` is used as the base directory for relative filesystem paths.
fn infer_object_store(
    mut data_location: String,
    base: Option<&std::path::Path>,
) -> Result<(Url, Box<dyn ObjectStore>), BoxError> {
    // Make sure there is a trailing slash so it's recognized as a directory.
    if !data_location.ends_with('/') {
        data_location.push('/');
    }

    // TODO: Use `ObjectStoreScheme` once `https://github.com/apache/arrow-rs/pull/5912` is merged
    // and released.
    if data_location.starts_with("gs://") {
        let bucket = {
            let segment = data_location.trim_start_matches("gs://").split('/').next();
            segment.ok_or("invalid GCS url")?
        };

        let store = Box::new(
            GoogleCloudStorageBuilder::from_env()
                .with_bucket_name(bucket)
                .build()?,
        );
        Ok((Url::parse(&data_location)?, store))
    } else if data_location.starts_with("s3://") {
        let bucket = {
            let segment = data_location.trim_start_matches("s3://").split('/').next();
            segment.ok_or("invalid S3 url")?
        };

        let store = Box::new(
            AmazonS3Builder::from_env()
                .with_bucket_name(bucket)
                .build()?,
        );
        Ok((Url::parse(&data_location)?, store))
    } else {
        let mut path = PathBuf::from(&data_location);
        if !path.is_absolute() {
            if let Some(base) = base {
                path = PathBuf::from(base).join(path);
            }
        }

        // Error if the directory does not exist.
        let path = fs::canonicalize(path)?;

        let store = Box::new(LocalFileSystem::new_with_prefix(&path)?);
        let url = Url::from_directory_path(path).unwrap();
        Ok((url, store))
    }
}

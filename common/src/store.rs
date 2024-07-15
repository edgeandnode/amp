use std::{path::PathBuf, sync::Arc};

use bytes::Bytes;
use datafusion::prelude::SessionContext;
use fs_err as fs;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use object_store::{
    aws::AmazonS3Builder, gcp::GoogleCloudStorageBuilder, local::LocalFileSystem, path::Path,
    prefix::PrefixStore, ObjectMeta, ObjectStore,
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
/// - Tries to better handle various cases of relative paths and path prefixes.
/// - Can be extended with helper functions.
#[derive(Debug, Clone)]
pub struct Store {
    url: Url,
    prefix: String,
    store: Arc<PrefixStore<Arc<dyn ObjectStore>>>,
    unprefixed: Arc<dyn ObjectStore>,
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
        let (url, unprefixed) = infer_object_store(data_location, base)?;
        let prefix = url.path().to_string();
        let store = Arc::new(PrefixStore::new(unprefixed.clone(), prefix.as_str()));
        Ok(Self {
            url,
            prefix,
            store,
            unprefixed,
        })
    }

    pub fn in_memory() -> Self {
        let url = Url::parse("memory://in_memory_store/").unwrap();
        let store = Arc::new(object_store::memory::InMemory::new());
        Self {
            url,
            prefix: "".to_string(),
            store: Arc::new(PrefixStore::new(store.clone(), "")),
            unprefixed: store,
        }
    }

    pub fn url(&self) -> &Url {
        &self.url
    }

    /// A store that will search for paths relative to the location provided in the constructor, as
    /// would be expected.
    pub fn prefixed_store(&self) -> Arc<dyn ObjectStore> {
        self.store.clone()
    }

    /// Registers the store in a datafusion context so that it can be used in queries.
    pub fn register_in(&self, ctx: &SessionContext) {
        // It's important to register the unprefixed store, as we give absolute URLs to the
        // `CreateExternalTable` command, which will do its own prefixing. Using the prefixed store
        // here would result in double prefixing.
        ctx.register_object_store(self.url(), self.unprefixed.clone());
    }

    pub async fn get_bytes(&self, location: impl Into<Path>) -> Result<Bytes, StoreError> {
        Ok(self.store.get(&location.into()).await?.bytes().await?)
    }

    pub async fn get_string(&self, location: impl Into<Path>) -> Result<String, StoreError> {
        let path = location.into();
        let bytes = self.get_bytes(path.clone()).await?;
        String::from_utf8(bytes.to_vec()).map_err(|_| StoreError::NotUtf8(path.to_string()))
    }

    pub fn list(&self, prefix: impl Into<Path>) -> BoxStream<'_, Result<ObjectMeta, StoreError>> {
        self.store
            .list(Some(&prefix.into()))
            .map_err(|e| e.into())
            .boxed()
    }
}

impl std::fmt::Display for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "store with prefix {}", self.prefix)
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
) -> Result<(Url, Arc<dyn ObjectStore>), BoxError> {
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

        let store = Arc::new(
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

        let store = Arc::new(
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

        let url = Url::from_directory_path(path).unwrap();
        let store = Arc::new(LocalFileSystem::new());
        Ok((url, store))
    }
}

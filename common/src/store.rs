use std::{future::Future, path::PathBuf, sync::Arc};

use bytes::Bytes;
use fs_err as fs;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use object_store::{
    aws::{AmazonS3Builder, AmazonS3ConfigKey},
    azure::{AzureConfigKey, MicrosoftAzureBuilder},
    gcp::{GoogleCloudStorageBuilder, GoogleConfigKey},
    local::LocalFileSystem,
    path::Path,
    prefix::PrefixStore,
    ObjectMeta, ObjectStore, ObjectStoreScheme,
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

impl StoreError {
    pub fn is_not_found(&self) -> bool {
        matches!(
            self,
            StoreError::ObjectStore(object_store::Error::NotFound { .. })
        )
    }
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

    /// `bucket` is `None` for local filesystem stores.
    bucket: Option<String>,
}

impl Store {
    /// Creates a store for an object store url or filesystem directory.
    ///
    /// Examples of valid formats for `data_location`:
    /// - Filesystem path: `relative/path/to/data/`
    /// - GCS: `gs://bucket-name`
    /// - S3: `s3://bucket-name`
    /// - Prefixed: `s3://bucket-name/my_prefix/`
    ///
    /// If `data_location` is a relative filesystem path, then `base` will be used as the prefix.
    pub fn new(data_location: String, base: Option<&std::path::Path>) -> Result<Self, BoxError> {
        let url = infer_url(data_location, base)?;
        let (unprefixed, bucket) = infer_object_store(&url)?;
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

    pub fn in_memory() -> Self {
        let url = Url::parse("memory://in_memory_store/").unwrap();
        let store = Arc::new(object_store::memory::InMemory::new());
        Self {
            url,
            prefix: "".to_string(),
            store: Arc::new(PrefixStore::new(store.clone(), "")),
            unprefixed: store,
            bucket: None,
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

    /// Returns the unprefixed store. Use this when you have an URL which already contains the prefix.
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.unprefixed.clone()
    }

    pub async fn get_bytes(&self, location: impl Into<Path>) -> Result<Bytes, StoreError> {
        self.store.get_bytes(location).await
    }

    pub async fn get_string(&self, location: impl Into<Path>) -> Result<String, StoreError> {
        self.store.get_string(location).await
    }

    pub async fn put_string(&self, location: impl Into<Path>, s: String) -> Result<(), StoreError> {
        self.store.put(&location.into(), s.into()).await?;
        Ok(())
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

/// Returns a tuple of (object_store, bucket).
/// `bucket` is `None` for local filesystem stores.
pub fn infer_object_store(url: &Url) -> Result<(Arc<dyn ObjectStore>, Option<String>), BoxError> {
    let (object_store_scheme, _) = ObjectStoreScheme::parse(&url)?;

    match object_store_scheme {
        ObjectStoreScheme::GoogleCloudStorage => {
            let builder = GoogleCloudStorageBuilder::from_env().with_url(url.to_string());
            let bucket = builder.get_config_value(&GoogleConfigKey::Bucket);
            let store = Arc::new(builder.build()?);
            Ok((store, bucket))
        }
        ObjectStoreScheme::AmazonS3 => {
            let builder = AmazonS3Builder::from_env().with_url(url.to_string());
            let bucket = builder.get_config_value(&AmazonS3ConfigKey::Bucket);
            let store = Arc::new(builder.build()?);
            Ok((store, bucket))
        }
        ObjectStoreScheme::MicrosoftAzure => {
            let builder = MicrosoftAzureBuilder::from_env().with_url(url.to_string());
            let bucket = builder.get_config_value(&AzureConfigKey::ContainerName);
            let store = Arc::new(builder.build()?);
            Ok((store, bucket))
        }
        ObjectStoreScheme::Local => {
            let store = Arc::new(LocalFileSystem::new());
            Ok((store, None))
        }
        ObjectStoreScheme::Http => {
            let err_msg = format!(
        "unsupported object store url: {}. If you are attempting to configure an S3-compatible object store, \
         please use the `s3://` scheme and configure AWS_ENDPOINT. See the documentation for more details.",
            url
        );
            Err(err_msg.into())
        }
        ObjectStoreScheme::Memory | _ => {
            Err(format!("unsupported object store scheme: {:?}", object_store_scheme).into())
        }
    }
}

fn infer_url(mut data_location: String, base: Option<&std::path::Path>) -> Result<Url, BoxError> {
    if !data_location.ends_with('/') {
        data_location.push('/');
    }
    let url = match Url::parse(&data_location) {
        Ok(url) => url,

        // If the location is not an URL, we can still try to parse it as a relative filesystem path.
        Err(_) => {
            let mut path = PathBuf::from(&data_location);
            if !path.is_absolute() {
                if let Some(base) = base {
                    path = PathBuf::from(base).join(path);
                }
            }

            // Error if the directory does not exist.
            let path = fs::canonicalize(path)?;

            Url::from_directory_path(path).unwrap()
        }
    };
    Ok(url)
}

pub trait ObjectStoreExt {
    fn get_bytes(
        &self,
        location: impl Into<Path>,
    ) -> impl Future<Output = Result<Bytes, StoreError>>;
    fn get_string(
        &self,
        location: impl Into<Path>,
    ) -> impl Future<Output = Result<String, StoreError>>;
}

impl<T: ObjectStore> ObjectStoreExt for T {
    fn get_bytes(
        &self,
        location: impl Into<Path>,
    ) -> impl Future<Output = Result<Bytes, StoreError>> {
        async move { Ok(self.get(&location.into()).await?.bytes().await?) }
    }

    fn get_string(
        &self,
        location: impl Into<Path>,
    ) -> impl Future<Output = Result<String, StoreError>> {
        async move {
            let path = location.into();
            let bytes = self.get_bytes(path.clone()).await?;
            String::from_utf8(bytes.to_vec()).map_err(|_| StoreError::NotUtf8(path.to_string()))
        }
    }
}

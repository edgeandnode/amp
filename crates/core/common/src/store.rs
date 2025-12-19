//! Object store abstraction layer.
//!
//! This module provides the [`Store`] wrapper.

use std::sync::Arc;

use amp_object_store::{ObjectStoreCreationError, url::ObjectStoreUrl};
use object_store::ObjectStore;

/// Data store.
///
/// A wrapper around [`ObjectStore`] for managing datasets' data.
///
/// There are a few things it helps us with over a plain `ObjectStore`:
/// - Keeps track of the URL of the store, in case we need it.
/// - Tries to better handle various cases of relative paths and path prefixes.
/// - Can be extended with helper functions.
#[derive(Debug, Clone)]
pub struct Store {
    url: Arc<ObjectStoreUrl>,
    inner: Arc<dyn ObjectStore>,
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
    pub fn new(url: ObjectStoreUrl) -> Result<Self, ObjectStoreCreationError> {
        let inner: Arc<dyn ObjectStore> = amp_object_store::new_with_prefix(&url, url.path())?;
        Ok(Self {
            url: Arc::new(url),
            inner,
        })
    }

    pub fn url(&self) -> &url::Url {
        &self.url
    }

    /// Returns a reference to the inner object store for this location.
    ///
    /// All operations are relative to the URL path provided in the constructor.
    pub fn as_inner(&self) -> &Arc<dyn ObjectStore> {
        &self.inner
    }
}

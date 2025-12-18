//! Extension trait for ObjectStore.

use std::future::Future;

use bytes::Bytes;
use object_store::{ObjectStore, path::Path};

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
    ) -> impl Future<Output = Result<Bytes, ObjectStoreExtError>>;

    /// Reads the entire contents of an object as a UTF-8 string.
    ///
    /// This method reads the object's contents and attempts to decode them as UTF-8.
    /// Returns `ObjectStoreExtError::NotUtf8` if the contents are not valid UTF-8.
    fn get_string(
        &self,
        location: impl Into<Path>,
    ) -> impl Future<Output = Result<String, ObjectStoreExtError>>;
}

impl<T> ObjectStoreExt for T
where
    T: ObjectStore,
{
    async fn get_bytes(&self, location: impl Into<Path>) -> Result<Bytes, ObjectStoreExtError> {
        self.get(&location.into())
            .await
            .map_err(ObjectStoreExtError::ObjectStoreGet)?
            .bytes()
            .await
            .map_err(ObjectStoreExtError::ObjectStoreBytes)
    }

    async fn get_string(&self, location: impl Into<Path>) -> Result<String, ObjectStoreExtError> {
        let path = location.into();
        let bytes = self.get_bytes(path.clone()).await?;
        String::from_utf8(bytes.to_vec()).map_err(|err| ObjectStoreExtError::NotUtf8 {
            path: path.to_string(),
            source: err,
        })
    }
}

/// Error type for object store read operations.
///
/// This error encompasses failures that can occur when reading from object stores.
/// Used by [`ObjectStoreExt`] methods.
#[derive(Debug, thiserror::Error)]
pub enum ObjectStoreExtError {
    /// File contents are not valid UTF-8 text.
    ///
    /// This occurs when calling `get_string()` on a file that contains binary data
    /// or text in a non-UTF-8 encoding.
    #[error("object is not an utf8 text file: {path}")]
    NotUtf8 {
        /// The path of the file that is not valid UTF-8.
        path: String,
        #[source]
        source: std::string::FromUtf8Error,
    },

    /// Error getting object metadata or initiating object retrieval.
    ///
    /// This error occurs when calling `.get()` on the object store.
    /// Common causes: network timeouts, permission denied, file not found,
    /// service unavailable, or authentication token expiry.
    #[error("failed to get object: {0}")]
    ObjectStoreGet(#[source] object_store::Error),

    /// Error reading object bytes after successful retrieval.
    ///
    /// This error occurs when calling `.bytes()` on a retrieved object.
    /// Common causes: network interruption during download, corrupted data,
    /// or timeout while streaming the object content.
    #[error("failed to read object bytes: {0}")]
    ObjectStoreBytes(#[source] object_store::Error),
}

impl ObjectStoreExtError {
    pub fn is_not_found(&self) -> bool {
        matches!(
            self,
            ObjectStoreExtError::ObjectStoreGet(object_store::Error::NotFound { .. })
                | ObjectStoreExtError::ObjectStoreBytes(object_store::Error::NotFound { .. })
        )
    }
}

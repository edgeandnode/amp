//! Object store abstraction layer for cloud storage providers.
//!
//! This crate provides a unified interface for working with various object storage backends
//! including local filesystem, AWS S3, Google Cloud Storage, and Azure Blob Storage.

use std::sync::Arc;

use object_store::{
    ObjectStore, aws::AmazonS3Builder, azure::MicrosoftAzureBuilder,
    gcp::GoogleCloudStorageBuilder, local::LocalFileSystem, path::Path, prefix::PrefixStore,
};

pub mod ext;
pub mod url;

use self::url::{ObjectStoreProvider, ObjectStoreUrl};

/// Creates an object store at the bucket/container root level.
///
/// The URL's path component is ignored - the returned store operates at the bucket/container root.
/// Use [`new_with_prefix`] for path-scoped access where operations are relative to a prefix.
///
/// This function returns the appropriate object store implementation based on the URL scheme.
/// See [`ObjectStoreUrl`] and [`ObjectStoreProvider`] for supported schemes.
///
/// # Providers configuration
///
/// Cloud providers are configured via environment variables:
/// - **AWS S3**: Uses `AWS_*` environment variables (e.g., `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
/// - **Google Cloud**: Uses `GOOGLE_*` environment variables or service account credentials
/// - **Azure**: Uses `AZURE_*` environment variables (e.g., `AZURE_STORAGE_ACCOUNT_NAME`)
pub fn new(
    url: impl AsRef<ObjectStoreUrl>,
) -> Result<Arc<dyn ObjectStore>, ObjectStoreCreationError> {
    let url = url.as_ref();

    match url.provider() {
        ObjectStoreProvider::GoogleCloudStorage => {
            let store = GoogleCloudStorageBuilder::from_env()
                .with_url(url)
                .build()
                .map_err(|err| ObjectStoreCreationError {
                    url: url.to_string(),
                    source: err,
                })?;
            Ok(Arc::new(store))
        }
        ObjectStoreProvider::AmazonS3 => {
            let store = AmazonS3Builder::from_env()
                .with_url(url)
                .build()
                .map_err(|err| ObjectStoreCreationError {
                    url: url.to_string(),
                    source: err,
                })?;
            Ok(Arc::new(store))
        }
        ObjectStoreProvider::MicrosoftAzure => {
            let store = MicrosoftAzureBuilder::from_env()
                .with_url(url)
                .build()
                .map_err(|err| ObjectStoreCreationError {
                    url: url.to_string(),
                    source: err,
                })?;
            Ok(Arc::new(store))
        }
        ObjectStoreProvider::Local => Ok(Arc::new(LocalFileSystem::new())),
    }
}

/// Creates a path-scoped object store where operations are relative to the given prefix.
///
/// All operations on the returned store (get, put, list, etc.) are performed relative to the prefix.
/// The prefix is prepended to all paths used in store operations.
///
/// See [`new`] for provider configuration details and supported schemes.
pub fn new_with_prefix(
    url: impl AsRef<ObjectStoreUrl>,
    prefix: impl Into<Path>,
) -> Result<Arc<dyn ObjectStore>, ObjectStoreCreationError> {
    let url = url.as_ref();

    match url.provider() {
        ObjectStoreProvider::GoogleCloudStorage => {
            let store = GoogleCloudStorageBuilder::from_env()
                .with_url(url)
                .build()
                .map_err(|err| ObjectStoreCreationError {
                    url: url.to_string(),
                    source: err,
                })?;
            Ok(Arc::new(PrefixStore::new(store, prefix)))
        }
        ObjectStoreProvider::AmazonS3 => {
            let store = AmazonS3Builder::from_env()
                .with_url(url)
                .build()
                .map_err(|err| ObjectStoreCreationError {
                    url: url.to_string(),
                    source: err,
                })?;
            Ok(Arc::new(PrefixStore::new(store, prefix)))
        }
        ObjectStoreProvider::MicrosoftAzure => {
            let store = MicrosoftAzureBuilder::from_env()
                .with_url(url)
                .build()
                .map_err(|err| ObjectStoreCreationError {
                    url: url.to_string(),
                    source: err,
                })?;
            Ok(Arc::new(PrefixStore::new(store, prefix)))
        }
        ObjectStoreProvider::Local => {
            let store = LocalFileSystem::new();
            Ok(Arc::new(PrefixStore::new(store, prefix)))
        }
    }
}

/// Failed to create object store instance.
///
/// This error occurs when attempting to create an object store from a URL with valid scheme
/// but the underlying provider cannot be initialized. The URL itself has been validated,
/// but the object store backend failed to instantiate.
///
/// Common causes:
/// - **Missing credentials**: Required environment variables not set (e.g., `AWS_ACCESS_KEY_ID`, `GOOGLE_APPLICATION_CREDENTIALS`)
/// - **Invalid credentials**: Environment variables set but contain invalid or expired credentials
/// - **Network issues**: Cannot reach cloud provider's authentication endpoint
/// - **Configuration problems**: Malformed configuration values or unsupported regions
/// - **Permission denied**: Credentials lack required permissions for the bucket/container
/// - **Bucket/container not found**: Referenced bucket or container does not exist
///
/// This error is returned by both [`new`] and [`new_with_prefix`] functions.
#[derive(Debug, thiserror::Error)]
#[error("failed to create object store for {url}")]
pub struct ObjectStoreCreationError {
    url: String,
    #[source]
    source: object_store::Error,
}

//! Object store abstraction layer.
//!
//! This module provides the [`Store`] wrapper.

use std::sync::Arc;

use amp_object_store::{ObjectStoreCreationError, url::ObjectStoreUrl};
use bytes::Bytes;
use datafusion::{
    arrow::datatypes::SchemaRef,
    common::Statistics,
    datasource::physical_plan::parquet::metadata::DFParquetMetadata,
    error::DataFusionError,
    parquet::{
        arrow::async_reader::ParquetObjectReader,
        errors::ParquetError,
        file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader},
    },
};
use datasets_common::{hash::Hash, hash_reference::HashReference, table_name::TableName};
use foyer::Cache;
use futures::{Stream, StreamExt as _, TryStreamExt as _, stream::BoxStream};
use metadata_db::{LocationId, MetadataDb, PhysicalTable, files::FileId};
use object_store::{ObjectMeta, ObjectStore, buffered::BufWriter, path::Path};
use url::Url;

use crate::{
    catalog::physical::{PhyTablePath, PhyTableRevisionPath},
    metadata::FileName,
};

/// Data store.
///
/// A wrapper around [`ObjectStore`] for managing datasets' data.
///
/// There are a few things it helps us with over a plain `ObjectStore`:
/// - Keeps track of the URL of the store, in case we need it.
/// - Tries to better handle various cases of relative paths and path prefixes.
/// - Can be extended with helper functions.
/// - Provides integrated metadata database operations.
#[derive(Debug, Clone)]
pub struct Store {
    metadata_db: MetadataDb,
    object_store: Arc<dyn ObjectStore>,
    url: Arc<ObjectStoreUrl>,
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
    pub fn new(
        metadata_db: MetadataDb,
        url: ObjectStoreUrl,
    ) -> Result<Self, ObjectStoreCreationError> {
        let object_store: Arc<dyn ObjectStore> =
            amp_object_store::new_with_prefix(&url, url.path())?;
        Ok(Self {
            metadata_db,
            object_store,
            url: Arc::new(url),
        })
    }

    pub fn url(&self) -> &Url {
        &self.url
    }

    /// Returns a reference to the metadata database.
    ///
    /// Prefer using Store methods when possible. Direct database access should
    /// be used only for operations not yet available through Store's API.
    pub fn metadata_db(&self) -> &MetadataDb {
        &self.metadata_db
    }

    /// Returns a reference to the inner object store for DataFusion integration.
    ///
    /// **Note**: Prefer using Store methods for object store operations. This method
    /// is intended for registering the store with DataFusion's SessionContext.
    ///
    /// All operations are relative to the URL path provided in the constructor.
    pub fn as_datafusion_object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.object_store
    }
}

/// Physical table revision management
impl Store {
    /// Registers a new physical table revision and marks it as active.
    ///
    /// This atomically registers the revision location in the metadata database
    /// and marks it as active while deactivating all other revisions for this table.
    pub async fn register_table_revision(
        &self,
        dataset: &HashReference,
        table_name: &TableName,
        path: &PhyTableRevisionPath,
    ) -> Result<LocationId, RegisterTableRevisionError> {
        let mut tx = self
            .metadata_db
            .begin_txn()
            .await
            .map_err(RegisterTableRevisionError::TransactionBegin)?;

        let location_id = metadata_db::physical_table::register(
            &mut tx,
            dataset.namespace(),
            dataset.name(),
            dataset.hash(),
            table_name,
            path,
            false,
        )
        .await
        .map_err(RegisterTableRevisionError::RegisterPhysicalTable)?;

        metadata_db::physical_table::mark_inactive_by_table_id(&mut tx, dataset.hash(), table_name)
            .await
            .map_err(RegisterTableRevisionError::MarkInactive)?;

        metadata_db::physical_table::mark_active_by_id(
            &mut tx,
            location_id,
            dataset.hash(),
            table_name,
        )
        .await
        .map_err(RegisterTableRevisionError::MarkActive)?;

        tx.commit()
            .await
            .map_err(RegisterTableRevisionError::TransactionCommit)?;

        Ok(location_id)
    }

    /// Gets the active revision of a table from the metadata database.
    ///
    /// Returns the database row if an active revision exists, or None if no active
    /// revision is found.
    pub async fn get_active_table_revision(
        &self,
        manifest_hash: &Hash,
        table_name: &TableName,
    ) -> Result<Option<PhysicalTable>, GetActiveTableRevisionError> {
        metadata_db::physical_table::get_active(&self.metadata_db, manifest_hash, table_name)
            .await
            .map_err(GetActiveTableRevisionError)
    }

    /// Finds the latest table revision by lexicographic comparison of revision IDs.
    ///
    /// This function performs a `list_with_delimiter` query on the provided table path,
    /// iterates through the returned subdirectories, parses each as a `PhyTableRevisionPath`,
    /// and finds the maximum by lexicographic comparison.
    ///
    /// Lexicographic comparison works correctly for UUIDv7 revisions since they are
    /// time-ordered by design. The latest UUID will sort last lexicographically.
    pub async fn find_latest_table_revision_in_object_store(
        &self,
        path: &PhyTablePath,
    ) -> Result<Option<PhyTableRevisionPath>, FindLatestTableRevisionInObjectStoreError> {
        let list_result = self
            .object_store
            .list_with_delimiter(Some(path.as_object_store_path()))
            .await
            .map_err(FindLatestTableRevisionInObjectStoreError)?;

        let latest_revision_path = list_result
            .common_prefixes
            .into_iter()
            .filter_map(|rev| rev.as_ref().parse::<PhyTableRevisionPath>().ok())
            .max_by(|a, b| a.as_str().cmp(b.as_str()));

        Ok(latest_revision_path)
    }
}

/// Physical table files management
impl Store {
    /// Registers a file in the metadata database.
    ///
    /// Associates the file with a specific location ID and stores its Parquet metadata.
    #[expect(clippy::too_many_arguments)]
    pub async fn register_file(
        &self,
        location_id: LocationId,
        url: &Url,
        file_name: &FileName,
        object_size: u64,
        object_e_tag: Option<String>,
        object_version: Option<String>,
        parquet_meta_json: serde_json::Value,
        footer: &Vec<u8>,
    ) -> Result<(), RegisterFileError> {
        metadata_db::files::register(
            &self.metadata_db,
            location_id,
            url,
            file_name,
            object_size,
            object_e_tag,
            object_version,
            parquet_meta_json,
            footer,
        )
        .await
        .map_err(RegisterFileError)
    }

    /// Streams file metadata for a specific location.
    pub fn stream_file_metadata(
        &self,
        location_id: LocationId,
    ) -> impl Stream<
        Item = Result<metadata_db::files::FileMetadataWithDetails, StreamFileMetadataError>,
    > + '_ {
        metadata_db::files::stream_by_location_id_with_details(&self.metadata_db, location_id)
            .map(|r| r.map_err(StreamFileMetadataError))
    }

    /// Gets the footer bytes for a file by its ID.
    pub async fn get_file_footer(&self, file_id: FileId) -> Result<Vec<u8>, GetFileFooterError> {
        metadata_db::files::get_footer_bytes(&self.metadata_db, file_id)
            .await
            .map_err(GetFileFooterError)
    }

    /// Deletes multiple files from object storage.
    ///
    /// Validates that the number of successfully deleted files matches the expected count.
    pub async fn delete_files_in_object_store(
        &self,
        paths: Vec<Path>,
    ) -> Result<Vec<Path>, DeleteFilesInObjectStoreError> {
        let expected = paths.len();
        let locations = Box::pin(futures::stream::iter(paths.into_iter().map(Ok)));
        let deleted_paths: Vec<Path> = self
            .object_store
            .delete_stream(locations)
            .try_collect()
            .await
            .map_err(DeleteFilesInObjectStoreError::ObjectStore)?;

        let deleted = deleted_paths.len();
        if deleted != expected {
            return Err(DeleteFilesInObjectStoreError::IncompleteDelete { expected, deleted });
        }

        Ok(deleted_paths)
    }

    /// Lists all files in a physical table revision directory.
    ///
    /// Returns all file metadata for files in the revision directory, sorted
    /// by location path for deterministic ordering.
    pub async fn list_revision_files_in_object_store(
        &self,
        path: &PhyTableRevisionPath,
    ) -> Result<Vec<ObjectMeta>, ListRevisionFilesInObjectStoreError> {
        let mut files = self
            .object_store
            .list(Some(path.as_object_store_path()))
            .try_collect::<Vec<_>>()
            .await
            .map_err(ListRevisionFilesInObjectStoreError)?;
        files.sort_unstable_by(|a, b| a.location.cmp(&b.location));
        Ok(files)
    }

    /// Streams all files in a physical table revision directory.
    ///
    /// Returns a stream of object metadata for each file in the revision directory.
    pub fn stream_revision_files_in_object_store(
        &self,
        path: &PhyTableRevisionPath,
    ) -> BoxStream<'_, Result<ObjectMeta, StreamRevisionFilesInObjectStoreError>> {
        let stream = self
            .object_store
            .list(Some(path.as_object_store_path()))
            .map(|result| result.map_err(StreamRevisionFilesInObjectStoreError));
        Box::pin(stream)
    }

    /// Gets object metadata (size, e_tag, version) for a file in a table revision.
    pub async fn head_revision_file_in_object_store(
        &self,
        path: &PhyTableRevisionPath,
        filename: &FileName,
    ) -> Result<ObjectMeta, HeadInObjectStoreError> {
        let file_path = path.as_object_store_path().child(filename.as_str());
        self.object_store
            .head(&file_path)
            .await
            .map_err(HeadInObjectStoreError)
    }

    /// Deletes a single file from object storage.
    pub async fn delete_file_in_object_store(
        &self,
        path: &Path,
    ) -> Result<(), DeleteFileInObjectStoreError> {
        self.object_store
            .delete(path)
            .await
            .map_err(DeleteFileInObjectStoreError)
    }

    /// Creates a delete stream for bulk file deletion with individual results.
    ///
    /// Unlike `delete_files_in_object_store`, this returns a stream that yields
    /// each deletion result individually, allowing callers to handle per-file
    /// success/failure (e.g., for metrics tracking).
    pub fn delete_files_stream<'a>(
        &'a self,
        paths: BoxStream<'a, Result<Path, object_store::Error>>,
    ) -> BoxStream<'a, Result<Path, DeleteFilesStreamError>> {
        Box::pin(
            self.object_store
                .delete_stream(paths)
                .map(|r| r.map_err(DeleteFilesStreamError)),
        )
    }
}

/// Object store file readers and writers
impl Store {
    /// Creates a buffered writer for writing a file to a table revision.
    pub fn create_revision_file_writer(
        &self,
        path: &PhyTableRevisionPath,
        filename: &FileName,
    ) -> BufWriter {
        let file_path = path.as_object_store_path().child(filename.as_str());
        BufWriter::new(self.object_store.clone(), file_path)
    }

    /// Creates a ParquetObjectReader for reading parquet files.
    pub fn create_revision_file_reader(&self, path: Path) -> ParquetObjectReader {
        ParquetObjectReader::new(self.object_store.clone(), path)
    }
}

/// Errors that occur when registering and activating a physical table revision
///
/// This error type is used by `Store::register_table_revision()`.
#[derive(Debug, thiserror::Error)]
pub enum RegisterTableRevisionError {
    /// Failed to begin transaction
    ///
    /// This error occurs when the database connection fails to start a transaction,
    /// typically due to connection issues, database unavailability, or permission problems.
    ///
    /// Common causes:
    /// - Database connection pool exhausted
    /// - Database server unreachable
    /// - Network connectivity issues
    /// - Insufficient permissions
    #[error("Failed to begin transaction")]
    TransactionBegin(#[source] metadata_db::Error),

    /// Failed to register physical table location in metadata database
    ///
    /// This error occurs when inserting the new physical table revision record fails.
    /// The transaction is still open and will be rolled back.
    ///
    /// Common causes:
    /// - Database constraint violation
    /// - Database connection lost during operation
    /// - Invalid data format or encoding issues
    #[error("Failed to register physical table in metadata database")]
    RegisterPhysicalTable(#[source] metadata_db::Error),

    /// Failed to mark existing active revisions as inactive
    ///
    /// This error occurs when updating the status of previously active revisions fails.
    /// The transaction is still open and will be rolled back, so the new revision
    /// registration will not be persisted.
    ///
    /// Common causes:
    /// - Database connection lost during update
    /// - Lock timeout on physical_tables rows
    /// - Database constraint violation
    #[error("Failed to mark existing physical tables as inactive")]
    MarkInactive(#[source] metadata_db::Error),

    /// Failed to mark new physical table revision as active
    ///
    /// This error occurs when updating the newly registered revision to active status fails.
    /// The transaction is still open and will be rolled back, so neither the new revision
    /// registration nor the inactive status updates will be persisted.
    ///
    /// Common causes:
    /// - Database connection lost during update
    /// - Lock timeout on physical_tables row
    /// - Database constraint violation
    #[error("Failed to mark new physical table as active")]
    MarkActive(#[source] metadata_db::Error),

    /// Failed to commit transaction after successful database operations
    ///
    /// When a commit fails, PostgreSQL guarantees that all changes are rolled back.
    /// None of the operations in the transaction (registering revision, marking inactive,
    /// marking active) were persisted to the database.
    ///
    /// Possible causes:
    /// - Database connection lost during commit
    /// - Transaction conflict with concurrent operations (serialization failure)
    /// - Database constraint violation detected at commit time
    /// - Database running out of disk space or resources
    ///
    /// The operation is safe to retry from the beginning as no partial state was persisted.
    #[error("Failed to commit transaction")]
    TransactionCommit(#[source] metadata_db::Error),
}

/// Failed to retrieve active physical table revision from metadata database
///
/// This error occurs when querying the metadata database for the currently active
/// physical table revision fails. This typically happens during table initialization
/// or when checking for existing revisions.
///
/// Common causes:
/// - Database connection lost during query
/// - Database server unreachable
/// - Network connectivity issues
/// - Invalid manifest hash or table name format
#[derive(Debug, thiserror::Error)]
#[error("Failed to get active revision from metadata database")]
pub struct GetActiveTableRevisionError(#[source] pub metadata_db::Error);

/// Failed to find the latest table revision in object store
///
/// This error occurs when the object store fails to list revision directories
/// while attempting to find the most recent revision by lexicographic comparison.
/// This typically happens during table restoration or when checking for the
/// latest available revision.
///
/// Common causes:
/// - Object store unavailable or unreachable
/// - Network connectivity issues
/// - Permission denied for listing objects
/// - Invalid credentials or authentication failure
#[derive(Debug, thiserror::Error)]
#[error("Failed to find latest revision in object store")]
pub struct FindLatestTableRevisionInObjectStoreError(#[source] pub object_store::Error);

/// Failed to register file metadata in the metadata database
///
/// This error occurs when inserting file metadata (Parquet footer, object metadata,
/// and statistics) into the metadata database fails. This typically happens during
/// table dumping or restoration operations.
///
/// Common causes:
/// - Database connection lost during insert
/// - Database constraint violation (e.g., duplicate file entry)
/// - Invalid Parquet metadata JSON format
/// - Database running out of disk space
/// - Transaction conflict with concurrent operations
#[derive(Debug, thiserror::Error)]
#[error("Failed to register file in metadata database")]
pub struct RegisterFileError(#[source] pub metadata_db::Error);

/// Failed to stream file metadata from metadata database
///
/// This error occurs when streaming file metadata records for a specific location
/// fails. This typically happens during query execution or table truncation operations.
///
/// Common causes:
/// - Database connection lost during streaming
/// - Database server unreachable
/// - Network connectivity issues
/// - Invalid location ID
#[derive(Debug, thiserror::Error)]
#[error("Failed to stream file metadata from metadata database")]
pub struct StreamFileMetadataError(#[source] pub metadata_db::Error);

/// Failed to retrieve file footer bytes from metadata database
///
/// This error occurs when querying the Parquet file footer bytes by file ID fails.
/// This typically happens during query planning or file metadata operations.
///
/// Common causes:
/// - Database connection lost during query
/// - File ID does not exist in the database
/// - Database server unreachable
/// - Network connectivity issues
#[derive(Debug, thiserror::Error)]
#[error("Failed to get file footer from metadata database")]
pub struct GetFileFooterError(#[source] pub metadata_db::Error);

/// Errors that occur when deleting multiple files from object storage
///
/// This error type is used by `Store::delete_files_in_object_store()`.
#[derive(Debug, thiserror::Error)]
pub enum DeleteFilesInObjectStoreError {
    /// Failed to delete files from object store
    ///
    /// This error occurs when the object store delete operation fails.
    /// This typically happens during garbage collection or table truncation operations.
    ///
    /// Common causes:
    /// - Object store unavailable or unreachable
    /// - Network connectivity issues
    /// - Permission denied for deleting objects
    /// - Invalid credentials or authentication failure
    /// - Object already deleted (idempotency issue)
    #[error("Object store operation failed")]
    ObjectStore(#[source] object_store::Error),

    /// Incomplete deletion - not all files were successfully deleted
    ///
    /// This error occurs when the number of successfully deleted files does not match
    /// the expected count. This indicates a partial deletion occurred, which may leave
    /// orphaned files in the object store.
    ///
    /// This should be treated as a critical error requiring investigation, as it may
    /// indicate object store inconsistencies or concurrent deletion operations.
    #[error("Expected to delete {expected} files, but deleted {deleted}")]
    IncompleteDelete {
        /// Number of files expected to be deleted
        expected: usize,
        /// Number of files actually deleted
        deleted: usize,
    },
}

/// Failed to list revision files in object store
///
/// This error occurs when the object store fails to list files within
/// a specific table revision directory. This typically happens during
/// table restoration operations.
///
/// Common causes:
/// - Object store unavailable or unreachable
/// - Network connectivity issues
/// - Permission denied for listing objects
/// - Invalid credentials or authentication failure
#[derive(Debug, thiserror::Error)]
#[error("Failed to list revision files in object store")]
pub struct ListRevisionFilesInObjectStoreError(#[source] pub object_store::Error);

/// Failed to stream revision files in object store
///
/// This error occurs when streaming file listings from the object store fails.
/// This typically happens during table discovery, garbage collection, or
/// file enumeration operations.
///
/// Common causes:
/// - Object store unavailable or unreachable
/// - Network connectivity issues
/// - Permission denied for listing objects
/// - Invalid credentials or authentication failure
/// - Invalid path prefix
#[derive(Debug, thiserror::Error)]
#[error("Failed to stream revision files in object store")]
pub struct StreamRevisionFilesInObjectStoreError(#[source] pub object_store::Error);

/// Failed to get object metadata from object store
///
/// This error occurs when the object store fails to retrieve metadata for a file.
/// This typically happens when checking file attributes after writing or during
/// validation operations.
///
/// Common causes:
/// - Object store unavailable or unreachable
/// - Network connectivity issues
/// - File does not exist
/// - Permission denied for accessing object metadata
/// - Invalid credentials or authentication failure
#[derive(Debug, thiserror::Error)]
#[error("Failed to get object metadata from object store")]
pub struct HeadInObjectStoreError(#[source] pub object_store::Error);

/// Failed to delete a single file from object store
///
/// This error occurs when attempting to delete a single file from object storage fails.
/// This typically happens during cleanup operations, such as removing orphaned files.
///
/// Common causes:
/// - Object store unavailable or unreachable
/// - Network connectivity issues
/// - Permission denied for deleting objects
/// - Invalid credentials or authentication failure
/// - File already deleted (usually safe to ignore)
#[derive(Debug, thiserror::Error)]
#[error("Failed to delete file from object store")]
pub struct DeleteFileInObjectStoreError(#[source] pub object_store::Error);

/// Failed to delete file during streaming deletion
///
/// This error occurs when a file deletion fails during a streaming delete operation.
/// Each deletion in the stream is processed individually, allowing callers to handle
/// per-file success/failure (e.g., for metrics tracking).
///
/// Common causes:
/// - Object store unavailable or unreachable
/// - Network connectivity issues
/// - Permission denied for deleting objects
/// - Invalid credentials or authentication failure
/// - File already deleted (usually safe to ignore)
#[derive(Debug, thiserror::Error)]
#[error("Failed to delete file during streaming deletion")]
pub struct DeleteFilesStreamError(#[source] pub object_store::Error);

/// Cached data store.
///
/// A wrapper around [`Store`] that adds parquet metadata caching for query execution.
///
/// This type provides:
/// - All [`Store`] functionality via [`Deref`]
/// - Cached access to parquet file metadata via `get_cached_parquet_metadata()`
/// - Automatic cache management with configurable size
///
/// Use this type in query contexts where repeated metadata access benefits from caching.
/// For non-query contexts (dump, GC, etc.), use [`Store`] directly.
#[derive(Debug, Clone)]
pub struct CachedStore {
    store: Store,
    parquet_footer_cache: Cache<FileId, CachedParquetData>,
}

impl CachedStore {
    /// Creates a cached store with the specified cache size.
    ///
    /// The cache uses a memory-weighted eviction policy based on the size of cached
    /// parquet metadata. The `cache_size_mb` parameter controls the maximum memory
    /// footprint of the cache.
    pub fn new(store: Store, cache_size_mb: u64) -> Self {
        let cache_size_bytes = cache_size_mb * 1024 * 1024;
        let parquet_footer_cache = foyer::CacheBuilder::new(cache_size_bytes as usize)
            .with_weighter(|_k, v: &CachedParquetData| v.metadata.memory_size())
            .build();

        Self {
            store,
            parquet_footer_cache,
        }
    }

    /// Creates a cached store from existing store and cache instances.
    ///
    /// Use this when you already have a cache instance that should be shared
    /// across multiple components.
    pub fn from_parts(
        store: Store,
        parquet_footer_cache: Cache<FileId, CachedParquetData>,
    ) -> Self {
        Self {
            store,
            parquet_footer_cache,
        }
    }

    /// Returns a clone of the inner `Store`.
    ///
    /// Use this when you need to pass a `Store` to code that doesn't support `CachedStore`.
    /// Since `Store` is cheap to clone (uses `Arc` internally), this operation is efficient.
    pub fn as_store(&self) -> Store {
        self.store.clone()
    }

    /// Gets cached parquet metadata for a file, fetching from database on cache miss.
    ///
    /// This method encapsulates the cache lookup and database fetch logic. On cache miss,
    /// it retrieves the footer bytes from the metadata database, parses the parquet metadata,
    /// computes statistics, and stores the result in the cache.
    ///
    /// The `schema` parameter is required to compute DataFusion statistics from the parquet
    /// metadata.
    pub async fn get_cached_parquet_metadata(
        &self,
        file_id: FileId,
        schema: SchemaRef,
    ) -> Result<CachedParquetData, GetCachedMetadataError> {
        let cache = self.parquet_footer_cache.clone();
        let metadata_db = self.store.metadata_db.clone();
        let file_id1 = file_id;

        cache
            .get_or_fetch(&file_id1, || async move {
                // Cache miss, fetch from database
                let footer = metadata_db::files::get_footer_bytes(&metadata_db, file_id1)
                    .await
                    .map_err(GetCachedMetadataError::FetchFooter)?;

                let metadata = Arc::new(
                    ParquetMetaDataReader::new()
                        .with_page_index_policy(PageIndexPolicy::Required)
                        .parse_and_finish(&Bytes::from_owner(footer))
                        .map_err(GetCachedMetadataError::ParseMetadata)?,
                );

                let statistics = Arc::new(
                    DFParquetMetadata::statistics_from_parquet_metadata(&metadata, &schema)
                        .map_err(GetCachedMetadataError::ComputeStatistics)?,
                );

                Ok::<CachedParquetData, GetCachedMetadataError>(CachedParquetData {
                    metadata,
                    statistics,
                })
            })
            .await
            .map(|entry| entry.value().clone())
            .map_err(GetCachedMetadataError::CacheError)
    }
}

impl std::ops::Deref for CachedStore {
    type Target = Store;

    fn deref(&self) -> &Self::Target {
        &self.store
    }
}

/// Cached parquet data including metadata and computed statistics.
#[derive(Clone)]
pub struct CachedParquetData {
    pub metadata: Arc<ParquetMetaData>,
    pub statistics: Arc<Statistics>,
}

/// Errors that occur when fetching cached parquet metadata
#[derive(Debug, thiserror::Error)]
pub enum GetCachedMetadataError {
    /// Failed to fetch file footer bytes from metadata database
    ///
    /// This occurs when the database query to retrieve the parquet file footer fails.
    /// The footer contains critical metadata including schema, row group information,
    /// and page index data required for query planning and execution.
    ///
    /// Possible causes:
    /// - Database connection lost or network interruption
    /// - File ID not found in the metadata database
    /// - Database query timeout
    /// - Insufficient database permissions
    /// - Database server error or unavailability
    #[error("failed to fetch file footer from metadata database")]
    FetchFooter(#[source] metadata_db::Error),

    /// Failed to parse parquet metadata from footer bytes
    ///
    /// This occurs when the parquet library cannot parse the footer bytes into
    /// valid ParquetMetaData. This typically indicates corrupted or invalid parquet
    /// file data.
    ///
    /// Possible causes:
    /// - Corrupted parquet file footer data in the database
    /// - Incompatible parquet format version
    /// - Truncated or incomplete footer bytes
    /// - File was not a valid parquet file
    /// - Parquet file written with incompatible compression or encoding
    ///
    /// This error is not recoverable through retry as it indicates data corruption.
    #[error("failed to parse parquet metadata")]
    ParseMetadata(#[source] ParquetError),

    /// Failed to compute statistics from parquet metadata
    ///
    /// This occurs when DataFusion cannot extract statistics (min/max values,
    /// null counts, etc.) from the parquet metadata. These statistics are used
    /// for query optimization and predicate pushdown.
    ///
    /// Possible causes:
    /// - Schema mismatch between parquet file and expected schema
    /// - Invalid or missing statistics in the parquet metadata
    /// - Incompatible data types between parquet schema and arrow schema
    /// - Corrupted row group metadata
    /// - Unsupported parquet logical types
    ///
    /// This error typically indicates a schema evolution issue or incompatible
    /// parquet file format.
    #[error("failed to compute statistics from parquet metadata")]
    ComputeStatistics(#[source] DataFusionError),

    /// Cache layer error during get or fetch operation
    ///
    /// This occurs when the foyer cache fails during retrieval or storage operations.
    /// The cache provides both in-memory and disk-based caching of parquet metadata
    /// to avoid repeated database queries and parsing overhead.
    ///
    /// Possible causes:
    /// - Disk cache I/O errors (disk full, permissions, hardware failure)
    /// - Memory allocation failure in the in-memory cache
    /// - Cache corruption or checksum mismatch
    /// - Concurrent cache operations conflict
    /// - Cache storage backend unavailable
    ///
    /// This error may be transient and safe to retry. If it persists, it indicates
    /// a problem with the cache storage layer rather than the underlying data.
    #[error("cache layer error")]
    CacheError(#[source] foyer::Error),
}

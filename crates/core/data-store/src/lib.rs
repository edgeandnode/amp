//! Object store abstraction layer.
//!
//! This module provides the [`DataStore`] wrapper.

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
use datasets_common::{hash_reference::HashReference, table_name::TableName};
use foyer::Cache;
// Re-export foyer::Cache for use by downstream crates
pub use foyer::Cache as FoyerCache;
use futures::{Stream, StreamExt as _, TryStreamExt as _, stream::BoxStream};
use metadata_db::{
    MetadataDb,
    files::FileId,
    physical_table_revision::{LocationId, PhysicalTableRevision, RevisionMetadataOwned},
};
use object_store::{ObjectMeta, ObjectStore, buffered::BufWriter, path::Path};
use url::Url;
use uuid::Uuid;

pub mod file_name;
pub mod physical_table;

use self::{
    file_name::FileName,
    physical_table::{PhyTablePath, PhyTableRevisionPath, PhyTableUrl},
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
pub struct DataStore {
    metadata_db: MetadataDb,
    object_store: Arc<dyn ObjectStore>,
    url: Arc<ObjectStoreUrl>,
    parquet_footer_cache: Cache<FileId, CachedParquetData>,
}

impl DataStore {
    /// Creates a store for an object store URL (or filesystem directory).
    ///
    /// Examples of valid formats for `data_location`:
    /// - Filesystem path: `relative/path/to/data/`
    /// - GCS: `gs://bucket-name`
    /// - S3: `s3://bucket-name`
    /// - Prefixed: `s3://bucket-name/my_prefix/`
    ///
    /// If `data_location` is a relative filesystem path, then `base` will be used as the prefix.
    ///
    /// The `cache_size_mb` parameter controls the maximum memory footprint of the parquet
    /// metadata cache. The cache uses a memory-weighted eviction policy.
    pub fn new(
        metadata_db: MetadataDb,
        url: ObjectStoreUrl,
        cache_size_mb: u64,
    ) -> Result<Self, ObjectStoreCreationError> {
        let object_store: Arc<dyn ObjectStore> =
            amp_object_store::new_with_prefix(&url, url.path())?;

        let cache_size_bytes = cache_size_mb * 1024 * 1024;
        let parquet_footer_cache = foyer::CacheBuilder::new(cache_size_bytes as usize)
            .with_weighter(|_k, v: &CachedParquetData| v.metadata.memory_size())
            .build();

        Ok(Self {
            metadata_db,
            object_store,
            url: Arc::new(url),
            parquet_footer_cache,
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
impl DataStore {
    /// Creates and activates a new physical table revision.
    ///
    /// This atomically registers the revision location in the metadata database
    /// and marks it as active while deactivating all other revisions for this table.
    pub async fn create_and_activate_table_revision(
        &self,
        dataset: &HashReference,
        table_name: &TableName,
        path: &PhyTableRevisionPath,
    ) -> Result<LocationId, CreateAndActivateTableRevisionError> {
        let mut tx = self
            .metadata_db
            .begin_txn()
            .await
            .map_err(CreateAndActivateTableRevisionError::TransactionBegin)?;

        let metadata = build_revision_metadata(dataset, table_name);
        let location_id = metadata_db::physical_table_revision::register(&mut tx, path, metadata)
            .await
            .map_err(CreateAndActivateTableRevisionError::RegisterPhysicalTable)?;

        metadata_db::physical_table::register(
            &mut tx,
            dataset.namespace(),
            dataset.name(),
            dataset.hash(),
            table_name,
        )
        .await
        .map_err(CreateAndActivateTableRevisionError::RegisterPhysicalTable)?;

        metadata_db::physical_table::mark_inactive_by_table_name(
            &mut tx,
            dataset.namespace(),
            dataset.name(),
            dataset.hash(),
            table_name,
        )
        .await
        .map_err(CreateAndActivateTableRevisionError::MarkInactive)?;

        metadata_db::physical_table::mark_active_by_id(
            &mut tx,
            location_id,
            dataset.namespace(),
            dataset.name(),
            dataset.hash(),
            table_name,
        )
        .await
        .map_err(CreateAndActivateTableRevisionError::MarkActive)?;

        tx.commit()
            .await
            .map_err(CreateAndActivateTableRevisionError::TransactionCommit)?;

        Ok(location_id)
    }

    /// Creates a new table revision with a unique UUIDv7 identifier.
    ///
    /// Atomically:
    /// 1. Generates a UUIDv7-based path
    /// 2. Registers in metadata database
    /// 3. Marks as active (deactivating others)
    pub async fn create_new_table_revision(
        &self,
        dataset: &HashReference,
        table_name: &TableName,
    ) -> Result<PhyTableRevision, CreateNewTableRevisionError> {
        let revision_id = Uuid::now_v7();
        let path =
            PhyTableRevisionPath::new(dataset.namespace(), dataset.name(), table_name, revision_id);
        let url = PhyTableUrl::new(self.url(), &path);

        let location_id = self
            .create_and_activate_table_revision(dataset, table_name, &path)
            .await
            .map_err(CreateNewTableRevisionError)?;

        Ok(PhyTableRevision {
            location_id,
            path,
            url,
        })
    }

    /// Registers a physical table revision from an existing path.
    ///
    /// This is a low-level, idempotent operation that only inserts a record into
    /// `physical_table_revisions`. It does NOT create `physical_tables` entries,
    /// nor does it activate the revision.
    pub async fn register_table_revision(
        &self,
        dataset: &HashReference,
        table_name: &TableName,
        path: &PhyTableRevisionPath,
    ) -> Result<LocationId, RegisterTableRevisionError> {
        let metadata = build_revision_metadata(dataset, table_name);
        metadata_db::physical_table_revision::register(&self.metadata_db, path, metadata)
            .await
            .map_err(RegisterTableRevisionError)
    }

    /// Locks table revisions for a writer job.
    ///
    /// This updates the `writer` field for all specified revisions, establishing
    /// a relationship between the job and the physical table locations it manages.
    pub async fn lock_revisions_for_writer<'a>(
        &self,
        revisions: impl IntoIterator<Item = &'a PhyTableRevision>,
        writer: impl Into<metadata_db::jobs::JobId> + std::fmt::Debug,
    ) -> Result<(), LockRevisionsForWriterError> {
        let location_ids: Vec<_> = revisions.into_iter().map(|r| r.location_id).collect();
        metadata_db::physical_table_revision::assign_job_writer(
            &self.metadata_db,
            &location_ids,
            writer,
        )
        .await
        .map_err(LockRevisionsForWriterError)
    }

    /// Restores the latest table revision from object storage.
    ///
    /// 1. Finds latest revision by UUIDv7 ordering
    /// 2. Registers in metadata database
    /// 3. Marks as active
    ///
    /// Returns `None` if no revisions exist.
    ///
    /// **Note:** Does NOT register files. Caller must use `register_revision_files()`.
    pub async fn restore_latest_table_revision(
        &self,
        dataset: &HashReference,
        table_name: &TableName,
    ) -> Result<Option<PhyTableRevision>, RestoreLatestTableRevisionError> {
        let table_path = PhyTablePath::new(dataset.namespace(), dataset.name(), table_name);

        let Some(path) = self
            .find_latest_table_revision_in_object_store(&table_path)
            .await
            .map_err(RestoreLatestTableRevisionError::FindLatestRevision)?
        else {
            return Ok(None);
        };

        let url = PhyTableUrl::new(self.url(), &path);

        let location_id = self
            .create_and_activate_table_revision(dataset, table_name, &path)
            .await
            .map_err(RestoreLatestTableRevisionError::RegisterRevision)?;

        Ok(Some(PhyTableRevision {
            location_id,
            path,
            url,
        }))
    }

    /// Gets a physical table revision by its location ID from the metadata database.
    ///
    /// Returns the revision info if found, or `None` if no revision exists with the given ID.
    pub async fn get_revision_by_location_id(
        &self,
        location_id: LocationId,
    ) -> Result<Option<PhysicalTableRevision>, GetRevisionByLocationIdError> {
        metadata_db::physical_table_revision::get_by_location_id(&self.metadata_db, location_id)
            .await
            .map_err(GetRevisionByLocationIdError)
    }

    /// Lists physical table revisions from the metadata database.
    ///
    /// When `active` is `None`, returns all revisions. When `Some(true)` or
    /// `Some(false)`, returns only revisions matching that active status.
    pub async fn list_all_table_revisions(
        &self,
        active: Option<bool>,
        limit: i64,
    ) -> Result<Vec<PhysicalTableRevision>, ListAllTableRevisionsError> {
        metadata_db::physical_table_revision::list_all(&self.metadata_db, active, limit)
            .await
            .map_err(ListAllTableRevisionsError)
    }

    /// Gets the active revision of a table from the metadata database.
    ///
    /// Returns the active revision info if one exists, or None if not found.
    pub async fn get_table_active_revision(
        &self,
        dataset_ref: &HashReference,
        table_name: &TableName,
    ) -> Result<Option<PhyTableRevision>, GetTableActiveRevisionError> {
        let Some(row) = metadata_db::physical_table_revision::get_active(
            &self.metadata_db,
            dataset_ref.hash(),
            table_name,
        )
        .await
        .map_err(GetTableActiveRevisionError)?
        else {
            return Ok(None);
        };

        let path: PhyTableRevisionPath = row.path.into();
        let url = PhyTableUrl::new(self.url(), &path);

        Ok(Some(PhyTableRevision {
            location_id: row.id,
            path,
            url,
        }))
    }

    /// Activates a specific table revision by location ID.
    ///
    /// This atomically deactivates all existing revisions for the table and then
    /// marks the specified revision as active within a single transaction.
    ///
    /// Unlike [`create_and_activate_table_revision`](Self::create_and_activate_table_revision), this does not
    /// create a new revision — it activates an already-registered one.
    pub async fn activate_table_revision(
        &self,
        dataset: &HashReference,
        table_name: &TableName,
        location_id: LocationId,
    ) -> Result<(), ActivateTableRevisionError> {
        let mut tx = self
            .metadata_db
            .begin_txn()
            .await
            .map_err(ActivateTableRevisionError::TransactionBegin)?;

        metadata_db::physical_table::mark_inactive_by_table_name(
            &mut tx,
            dataset.namespace(),
            dataset.name(),
            dataset.hash(),
            table_name,
        )
        .await
        .map_err(ActivateTableRevisionError::MarkInactive)?;

        let updated = metadata_db::physical_table::mark_active_by_id(
            &mut tx,
            location_id,
            dataset.namespace(),
            dataset.name(),
            dataset.hash(),
            table_name,
        )
        .await
        .map_err(ActivateTableRevisionError::MarkActive)?;

        if !updated {
            return Err(ActivateTableRevisionError::TableNotFound);
        }

        tx.commit()
            .await
            .map_err(ActivateTableRevisionError::TransactionCommit)?;

        Ok(())
    }

    /// Upserts the physical table entry and activates a specific revision by location ID.
    ///
    /// This atomically:
    /// 1. Upserts the `physical_tables` row (ensures it exists)
    /// 2. Marks all existing revisions for the table as inactive
    /// 3. Marks the specified revision as active
    ///
    /// Unlike [`activate_table_revision`](Self::activate_table_revision), this also
    /// ensures the `physical_tables` row exists before activation.
    pub async fn register_and_activate_physical_table(
        &self,
        dataset: &HashReference,
        table_name: &TableName,
        location_id: LocationId,
    ) -> Result<(), RegisterAndActivatePhysicalTableError> {
        let mut tx = self
            .metadata_db
            .begin_txn()
            .await
            .map_err(RegisterAndActivatePhysicalTableError::TransactionBegin)?;

        metadata_db::physical_table::register(
            &mut tx,
            dataset.namespace(),
            dataset.name(),
            dataset.hash(),
            table_name,
        )
        .await
        .map_err(RegisterAndActivatePhysicalTableError::RegisterPhysicalTable)?;

        metadata_db::physical_table::mark_inactive_by_table_name(
            &mut tx,
            dataset.namespace(),
            dataset.name(),
            dataset.hash(),
            table_name,
        )
        .await
        .map_err(RegisterAndActivatePhysicalTableError::MarkInactive)?;

        let updated = metadata_db::physical_table::mark_active_by_id(
            &mut tx,
            location_id,
            dataset.namespace(),
            dataset.name(),
            dataset.hash(),
            table_name,
        )
        .await
        .map_err(RegisterAndActivatePhysicalTableError::MarkActive)?;

        if !updated {
            return Err(RegisterAndActivatePhysicalTableError::PhysicalTableNotFound);
        }

        tx.commit()
            .await
            .map_err(RegisterAndActivatePhysicalTableError::TransactionCommit)?;

        Ok(())
    }

    /// Deactivates all revisions for a table.
    ///
    /// Marks all revisions for the given dataset table as inactive. This is a
    /// single database operation (not wrapped in a transaction).
    pub async fn deactivate_table_revision(
        &self,
        dataset: &HashReference,
        table_name: &TableName,
    ) -> Result<(), DeactivateTableRevisionError> {
        let updated = metadata_db::physical_table::mark_inactive_by_table_name(
            &self.metadata_db,
            dataset.namespace(),
            dataset.name(),
            dataset.hash(),
            table_name,
        )
        .await
        .map_err(DeactivateTableRevisionError::MarkInactive)?;
        if !updated {
            return Err(DeactivateTableRevisionError::TableNotFound);
        }
        Ok(())
    }

    /// Deletes a physical table revision by its location ID.
    ///
    /// This removes the revision record from the metadata database. Due to CASCADE
    /// constraints, all associated `file_metadata` entries are also deleted.
    ///
    /// Returns `true` if the revision was deleted, `false` if it did not exist or
    /// was active at the time of deletion.
    pub async fn delete_table_revision(
        &self,
        location_id: LocationId,
    ) -> Result<bool, DeleteTableRevisionError> {
        metadata_db::physical_table_revision::delete_by_id(&self.metadata_db, location_id)
            .await
            .map_err(DeleteTableRevisionError)
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
impl DataStore {
    /// Registers a file in the metadata database for a specific revision.
    ///
    /// Associates the file with the revision's location ID and stores its Parquet metadata.
    #[expect(clippy::too_many_arguments)]
    pub async fn register_revision_file(
        &self,
        revision: &PhyTableRevision,
        file_name: &FileName,
        object_size: u64,
        object_e_tag: Option<String>,
        object_version: Option<String>,
        parquet_meta_json: serde_json::Value,
        footer: &Vec<u8>,
    ) -> Result<(), RegisterFileError> {
        metadata_db::files::register(
            &self.metadata_db,
            revision.location_id,
            revision.url.inner(),
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

    /// Streams file metadata for a revision with path-resolved object metadata.
    ///
    /// This method combines the database metadata with the revision path to produce
    /// `PhyTableRevisionFileMetadata` entries that include the full object store path.
    pub fn stream_revision_file_metadata(
        &self,
        revision: &PhyTableRevision,
    ) -> impl Stream<Item = Result<PhyTableRevisionFileMetadata, StreamFileMetadataError>> + '_
    {
        let location_id = revision.location_id;
        let table_path = revision.path.as_object_store_path().clone();

        metadata_db::files::stream_by_location_id_with_details(&self.metadata_db, location_id)
            .map_err(StreamFileMetadataError)
            .map_ok(move |row| {
                let location = table_path.child(row.file_name.as_str());
                let size = row.object_size.unwrap_or_default() as u64;

                // Extract created_at from ParquetMeta to use as last_modified.
                // Segments are created once and never modified, so created_at is appropriate.
                #[derive(serde::Deserialize)]
                struct Timestamp {
                    created_at: std::time::Duration,
                }
                let last_modified = serde_json::from_value::<Timestamp>(row.metadata.clone())
                    .ok()
                    .and_then(|t| {
                        chrono::DateTime::from_timestamp(
                            t.created_at.as_secs() as i64,
                            t.created_at.subsec_nanos(),
                        )
                    })
                    .unwrap_or_default();

                let object_meta = ObjectMeta {
                    location,
                    last_modified,
                    size,
                    e_tag: row.object_e_tag,
                    version: row.object_version,
                };
                PhyTableRevisionFileMetadata {
                    file_id: row.id,
                    file_name: row.file_name.into(),
                    location_id,
                    object_meta,
                    parquet_meta_json: row.metadata,
                }
            })
    }

    /// Gets all files for a revision as a Vec.
    ///
    /// This is a convenience method that collects the stream from
    /// `stream_revision_file_metadata` into a vector.
    pub async fn get_revision_files(
        &self,
        revision: &PhyTableRevision,
    ) -> Result<Vec<PhyTableRevisionFileMetadata>, StreamFileMetadataError> {
        self.stream_revision_file_metadata(revision)
            .try_collect()
            .await
    }

    /// Truncate a table revision by deleting all files and their metadata.
    ///
    /// For each file in the revision (with bounded concurrency):
    /// 1. Deletes the file from object storage (treats "not found" as success
    ///    for idempotent retries)
    /// 2. Deletes the corresponding `file_metadata` row from the database
    ///    (logs a warning and continues on failure)
    ///
    /// Returns the number of files processed.
    pub async fn truncate_revision(
        &self,
        revision: &PhyTableRevision,
        concurrency: usize,
    ) -> Result<u64, TruncateError> {
        let files: Vec<PhyTableRevisionFileMetadata> = self
            .stream_revision_file_metadata(revision)
            .try_collect()
            .await
            .map_err(TruncateError::StreamMetadata)?;

        let metadata_db = self.metadata_db.clone();
        let object_store = self.object_store.clone();

        let file_count = files.len() as u64;

        futures::stream::iter(files)
            .map(|file| {
                let metadata_db = metadata_db.clone();
                let object_store = object_store.clone();
                async move {
                    // Delete from object store (treat "not found" as success)
                    match object_store.delete(&file.object_meta.location).await {
                        Ok(()) => {}
                        Err(object_store::Error::NotFound { .. }) => {
                            tracing::debug!(
                                file_id = %file.file_id,
                                file_name = %file.file_name,
                                "file already deleted from object store, skipping"
                            );
                        }
                        Err(err) => {
                            tracing::error!(
                                file_id = %file.file_id,
                                file_name = %file.file_name,
                                error = %err,
                                "failed to delete file from object store"
                            );
                            return Err(TruncateError::DeleteFile {
                                file_name: file.file_name.as_str().to_owned(),
                                source: err,
                            });
                        }
                    }

                    // Delete file_metadata row from DB (warn and continue on failure)
                    if let Err(err) =
                        metadata_db::files::delete_by_ids(&metadata_db, &[file.file_id]).await
                    {
                        tracing::warn!(
                            file_id = %file.file_id,
                            file_name = %file.file_name,
                            error = %err,
                            "failed to delete file_metadata row, will be cleaned up on retry or by CASCADE"
                        );
                    }

                    Ok(())
                }
            })
            .buffer_unordered(concurrency)
            .try_collect::<()>()
            .await?;

        Ok(file_count)
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
        revision: &PhyTableRevision,
    ) -> Result<Vec<ObjectMeta>, ListRevisionFilesInObjectStoreError> {
        let mut files = self
            .object_store
            .list(Some(revision.path.as_object_store_path()))
            .try_collect::<Vec<_>>()
            .await
            .map_err(ListRevisionFilesInObjectStoreError)?;
        files.sort_unstable_by(|a, b| a.location.cmp(&b.location));
        Ok(files)
    }

    /// Gets object metadata (size, e_tag, version) for a file in a table revision.
    pub async fn head_revision_file_in_object_store(
        &self,
        revision: &PhyTableRevision,
        filename: &FileName,
    ) -> Result<ObjectMeta, HeadInObjectStoreError> {
        let file_path = revision
            .path
            .as_object_store_path()
            .child(filename.as_str());
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
        let metadata_db = self.metadata_db.clone();
        let cache = self.parquet_footer_cache.clone();

        cache
            .get_or_fetch(&file_id, || async move {
                // Cache miss, fetch from database
                let footer = metadata_db::files::get_footer_bytes(&metadata_db, file_id)
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

/// Progress and status tracking
impl DataStore {
    /// Gets tables associated with a specific writer.
    ///
    /// Returns a list of active tables where the specified writer is assigned,
    /// along with metadata about each table including dataset information.
    pub async fn get_tables_by_writer(
        &self,
        writer_id: impl Into<metadata_db::jobs::JobId> + std::fmt::Debug,
    ) -> Result<Vec<metadata_db::physical_table::WriterTableInfo>, GetTablesByWriterError> {
        metadata_db::physical_table::get_tables_by_writer(&self.metadata_db, writer_id)
            .await
            .map_err(GetTablesByWriterError)
    }
}

/// Object store file readers and writers
impl DataStore {
    /// Creates a buffered writer for writing a file to a table revision.
    pub fn create_revision_file_writer(
        &self,
        revision: &PhyTableRevision,
        filename: &FileName,
    ) -> BufWriter {
        let file_path = revision
            .path
            .as_object_store_path()
            .child(filename.as_str());
        BufWriter::new(self.object_store.clone(), file_path)
    }

    /// Creates a [`ParquetObjectReader`] for reading parquet files from a full path.
    ///
    /// This is a lower-level method for cases where you already have the full path.
    pub fn create_file_reader_from_path(&self, path: Path) -> ParquetObjectReader {
        ParquetObjectReader::new(self.object_store.clone(), path)
    }
}

/// Core physical table information managed by the data store.
///
/// Contains essential storage-related information without domain-specific knowledge.
/// Higher-level crates wrap this to add domain-specific functionality.
#[derive(Debug, Clone)]
pub struct PhyTableRevision {
    /// Location ID in the metadata database.
    pub location_id: LocationId,
    /// Relative path to the table revision in object storage.
    pub path: PhyTableRevisionPath,
    /// Full URL to the table directory.
    pub url: PhyTableUrl,
}

/// File metadata with path-resolved object metadata.
///
/// This is the file metadata for a specific revision, with the object metadata
/// resolved to include the full path in object storage.
#[derive(Debug, Clone)]
pub struct PhyTableRevisionFileMetadata {
    /// Unique identifier for this file in the metadata database.
    pub file_id: FileId,
    /// The file name (without path) in object storage.
    pub file_name: FileName,
    /// Location ID from the physical table revision.
    pub location_id: LocationId,
    /// Object metadata including the full path, size, and version information.
    pub object_meta: ObjectMeta,
    /// Parquet metadata including block ranges and other Amp-specific information.
    pub parquet_meta_json: serde_json::Value,
}

/// Construct opaque revision metadata from dataset context.
///
/// # Panics
///
/// Panics if the metadata JSON fails to serialize to a `RawValue`. This cannot happen in
/// practice because the input is constructed from `serde_json::json!()` which always
/// produces a valid `serde_json::Value`.
fn build_revision_metadata(
    dataset: &HashReference,
    table_name: &TableName,
) -> RevisionMetadataOwned {
    let json = serde_json::json!({
        "dataset_namespace": dataset.namespace(),
        "dataset_name": dataset.name(),
        "manifest_hash": dataset.hash(),
        "table_name": table_name,
    });
    // SAFETY: `to_raw_value` cannot fail on a `serde_json::Value` produced by the `json!` macro.
    let raw = serde_json::value::to_raw_value(&json)
        .expect("revision metadata should serialize to RawValue");
    metadata_db::physical_table_revision::RevisionMetadata::from_owned_unchecked(raw)
}

/// Errors that occur when registering and activating a physical table revision
///
/// This error type is used by [`DataStore::create_and_activate_table_revision()`].
#[derive(Debug, thiserror::Error)]
pub enum CreateAndActivateTableRevisionError {
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

/// Errors that occur when getting a revision by location ID
///
/// This error type is used by `DataStore::get_revision_by_location_id()`.
#[derive(Debug, thiserror::Error)]
#[error("Failed to get revision by location ID from metadata database")]
pub struct GetRevisionByLocationIdError(#[source] metadata_db::Error);

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
pub struct GetTableActiveRevisionError(#[source] pub metadata_db::Error);

/// Errors that occur when activating a physical table revision
///
/// This error type is used by `DataStore::activate_table_revision()`.
#[derive(Debug, thiserror::Error)]
pub enum ActivateTableRevisionError {
    /// Failed to begin transaction
    ///
    /// This error occurs when the database connection fails to start a transaction.
    ///
    /// Common causes:
    /// - Database connection pool exhausted
    /// - Database server unreachable
    /// - Network connectivity issues
    #[error("Failed to begin transaction")]
    TransactionBegin(#[source] metadata_db::Error),

    /// Failed to mark existing active revisions as inactive
    ///
    /// This error occurs when updating the status of previously active revisions fails.
    /// The transaction is still open and will be rolled back.
    ///
    /// Common causes:
    /// - Database connection lost during update
    /// - Lock timeout on physical_tables rows
    /// - Database constraint violation
    #[error("Failed to mark table revision as inactive")]
    MarkInactive(#[source] metadata_db::Error),

    /// Failed to mark the specified revision as active
    ///
    /// This error occurs when updating the target revision to active status fails.
    /// The transaction is still open and will be rolled back, so the inactive
    /// status updates will not be persisted.
    ///
    /// Common causes:
    /// - Database connection lost during update
    /// - Lock timeout on physical_tables row
    /// - Database constraint violation
    #[error("Failed to mark table revision as active")]
    MarkActive(#[source] metadata_db::Error),

    /// No physical table row found for the given dataset and table name
    ///
    /// This error occurs when the UPDATE matched zero rows, meaning no
    /// `physical_tables` entry exists for the dataset+table combination.
    /// The table may not have been registered yet.
    #[error("No physical table found for dataset and table name")]
    TableNotFound,

    /// Failed to commit transaction after successful database operations
    ///
    /// When a commit fails, PostgreSQL guarantees that all changes are rolled back.
    /// Neither the deactivation nor the activation will be persisted.
    ///
    /// Common causes:
    /// - Database connection lost during commit
    /// - Transaction conflict with concurrent operations
    /// - Database constraint violation detected at commit time
    ///
    /// The operation is safe to retry from the beginning as no partial state was persisted.
    #[error("Failed to commit transaction")]
    TransactionCommit(#[source] metadata_db::Error),
}

/// Errors that occur when upserting a physical table and activating a revision
///
/// This error type is used by `DataStore::register_and_activate_physical_table()`.
/// The operation runs as a single database transaction: on any failure,
/// PostgreSQL guarantees all changes are rolled back.
#[derive(Debug, thiserror::Error)]
pub enum RegisterAndActivatePhysicalTableError {
    /// Failed to begin a database transaction
    ///
    /// Common causes:
    /// - Database connection pool exhausted
    /// - Database server unreachable
    #[error("Failed to begin transaction")]
    TransactionBegin(#[source] metadata_db::Error),

    /// Failed to upsert the physical table record
    ///
    /// The UPSERT ensures a `physical_tables` row exists for the
    /// dataset+table combination before attempting to activate a revision.
    ///
    /// Common causes:
    /// - Database connection lost during insert
    /// - Database constraint violation
    #[error("Failed to register physical table in metadata database")]
    RegisterPhysicalTable(#[source] metadata_db::Error),

    /// Failed to mark existing active revisions as inactive
    ///
    /// Common causes:
    /// - Database connection lost during update
    /// - Lock timeout on physical_table_revisions rows
    /// - Database constraint violation
    #[error("Failed to mark existing physical tables as inactive")]
    MarkInactive(#[source] metadata_db::Error),

    /// Failed to mark the specified revision as active
    ///
    /// Common causes:
    /// - Database connection lost during update
    /// - Lock timeout on physical_table_revisions row
    /// - Database constraint violation
    #[error("Failed to mark table revision as active: {0}")]
    MarkActive(#[source] metadata_db::Error),

    /// No physical table found with the given location ID
    ///
    /// The `mark_active_by_id` update matched zero rows, meaning the provided
    /// `location_id` does not correspond to a registered physical table for this table.
    #[error("No physical table found")]
    PhysicalTableNotFound,

    /// Failed to commit transaction after successful database operations
    ///
    /// When a commit fails, PostgreSQL guarantees that all changes are rolled back.
    /// Neither the upsert, deactivation, nor the activation will be persisted.
    ///
    /// Common causes:
    /// - Database connection lost during commit
    /// - Transaction conflict with concurrent operations
    ///
    /// The operation is safe to retry from the beginning as no partial state was persisted.
    #[error("Failed to commit transaction")]
    TransactionCommit(#[source] metadata_db::Error),
}

/// Errors that occur when deactivating a physical table revision
///
/// This error type is used by `DataStore::deactivate_table_revision()`.
#[derive(Debug, thiserror::Error)]
#[error("Failed to mark table revision as inactive")]
pub enum DeactivateTableRevisionError {
    /// Failed to mark existing active revisions as inactive
    ///
    /// This error occurs when updating the status of previously active revisions fails.
    ///
    /// Common causes:
    /// - Database connection lost during update
    /// - Lock timeout on physical_tables rows
    /// - Database constraint violation
    #[error("Failed to mark table revision as inactive")]
    MarkInactive(#[source] metadata_db::Error),
    /// No physical table row found for the given dataset and table name
    ///
    /// This error occurs when the UPDATE matched zero rows, meaning no
    /// `physical_tables` entry exists for the dataset+table combination.
    /// The table may not have been registered yet.
    #[error("No physical table found for dataset and table name")]
    TableNotFound,
}

/// Failed to delete a table revision from the metadata database
///
/// This error occurs when the database delete operation fails.
///
/// Common causes:
/// - Database connection lost during delete
/// - Database server unreachable
/// - Network connectivity issues
#[derive(Debug, thiserror::Error)]
#[error("Failed to delete table revision from metadata database")]
pub struct DeleteTableRevisionError(#[source] pub metadata_db::Error);

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

/// Failed to create new table revision
///
/// This error occurs when creating a new table revision fails during the
/// registration and activation process. This typically happens during
/// dataset dumping operations.
///
/// Common causes:
/// - Database connection issues during registration
/// - Transaction commit failures
/// - Permission denied for database operations
#[derive(Debug, thiserror::Error)]
#[error("Failed to create new table revision")]
pub struct CreateNewTableRevisionError(#[source] pub CreateAndActivateTableRevisionError);

/// Failed to register a physical table revision
///
/// This error occurs when inserting a revision record into the metadata database fails.
///
/// Common causes:
/// - Database connection issues
/// - Database constraint violations
/// - Permission denied for database operations
#[derive(Debug, thiserror::Error)]
#[error("failed to register table revision")]
pub struct RegisterTableRevisionError(#[source] pub metadata_db::Error);

/// Failed to lock revisions for writer job
///
/// This error occurs when assigning a writer job to physical table revisions fails.
/// This typically happens during dataset dumping operations when establishing
/// the relationship between a job and the physical tables it manages.
///
/// Common causes:
/// - Database connection issues during assignment
/// - Invalid location IDs
/// - Database constraint violations
/// - Permission denied for database operations
#[derive(Debug, thiserror::Error)]
#[error("failed to lock revisions for writer")]
pub struct LockRevisionsForWriterError(#[source] pub metadata_db::Error);

/// Errors that occur when restoring the latest table revision
///
/// This error type is used by `DataStore::restore_latest_table_revision()`.
#[derive(Debug, thiserror::Error)]
pub enum RestoreLatestTableRevisionError {
    /// Failed to find latest revision in object store
    ///
    /// This error occurs when the object store fails to list revision directories
    /// while attempting to find the most recent revision.
    ///
    /// Common causes:
    /// - Object store unavailable or unreachable
    /// - Network connectivity issues
    /// - Permission denied for listing objects
    /// - Invalid credentials or authentication failure
    #[error("Failed to find latest revision in object store")]
    FindLatestRevision(#[source] FindLatestTableRevisionInObjectStoreError),

    /// Failed to register revision in metadata database
    ///
    /// This error occurs when registering the discovered revision in the
    /// metadata database fails.
    ///
    /// Common causes:
    /// - Database connection issues
    /// - Transaction commit failures
    /// - Permission denied for database operations
    #[error("Failed to register revision in metadata database")]
    RegisterRevision(#[source] CreateAndActivateTableRevisionError),
}

/// Errors that occur when listing all table revisions
///
/// This error type is used by [`DataStore::list_all_table_revisions()`].
#[derive(Debug, thiserror::Error)]
#[error("Failed to list all table revisions")]
pub struct ListAllTableRevisionsError(#[source] pub metadata_db::Error);

/// Errors that occur when truncating a physical table revision
///
/// This error type is used by `DataStore::truncate_revision()`.
#[derive(Debug, thiserror::Error)]
pub enum TruncateError {
    /// Failed to stream file metadata from metadata database
    ///
    /// This error occurs when querying the database for file metadata fails.
    ///
    /// Common causes:
    /// - Database connection lost during streaming
    /// - Database server unreachable
    /// - Network connectivity issues
    #[error("Failed to stream file metadata")]
    StreamMetadata(#[source] StreamFileMetadataError),

    /// Failed to delete a file from object store
    ///
    /// This error occurs when deleting a specific file from object storage fails.
    /// "Not found" errors are treated as success for idempotency and do not
    /// produce this variant.
    ///
    /// Common causes:
    /// - Object store unavailable or unreachable
    /// - Network connectivity issues
    /// - Permission denied for deleting objects
    #[error("Failed to delete file from object store: {file_name}")]
    DeleteFile {
        /// Name of the file that failed to delete
        file_name: String,
        /// The underlying object store error
        #[source]
        source: object_store::Error,
    },
}

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

/// Failed to get tables written by job from metadata database
///
/// This error occurs when querying the metadata database for tables
/// written by a specific job fails. This typically happens during
/// job progress reporting operations.
///
/// Common causes:
/// - Database connection lost during query
/// - Database server unreachable
/// - Network connectivity issues
/// - Invalid job ID
#[derive(Debug, thiserror::Error)]
#[error("failed to get tables by writer from metadata database")]
pub struct GetTablesByWriterError(#[source] pub metadata_db::Error);

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

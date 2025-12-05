use std::{sync::Arc, time::Duration};

use futures::{
    StreamExt, TryStreamExt,
    future::BoxFuture,
    stream::{BoxStream, Stream},
};
use sqlx::{postgres::types::PgInterval, types::chrono::NaiveDateTime};
use tracing::instrument;
use url::Url;

pub mod datasets;
mod db;
mod error;
mod files;
pub mod job_attempts;
pub mod jobs;
pub mod manifests;
pub mod notification_multiplexer;
pub mod physical_table;
#[cfg(feature = "temp-db")]
pub mod temp;
pub mod workers;

use self::db::ConnPool;
#[cfg(feature = "temp-db")]
pub use self::temp::{KEEP_TEMP_DIRS, temp_metadata_db};
pub use self::{
    datasets::{
        DatasetName, DatasetNameOwned, DatasetNamespace, DatasetNamespaceOwned, DatasetTag,
        DatasetVersion, DatasetVersionOwned,
    },
    db::{ConnError, Executor, Transaction},
    error::Error,
    files::{
        FileId, FileIdFromStrError, FileIdI64ConvError, FileIdU64Error, FileMetadata,
        FileMetadataWithDetails, FooterBytes,
    },
    jobs::{Job, JobId, JobStatus, JobStatusUpdateError},
    manifests::{ManifestHash, ManifestHashOwned, ManifestPath, ManifestPathOwned},
    notification_multiplexer::NotificationMultiplexerHandle,
    physical_table::{
        LocationId, LocationIdFromStrError, LocationIdI64ConvError, LocationIdU64Error,
        LocationWithDetails, PhysicalTable,
        events::{
            LocationNotifListener, LocationNotifRecvError, LocationNotifSendError,
            LocationNotification,
        },
    },
    workers::{
        HEARTBEAT_INTERVAL, Worker, WorkerInfo, WorkerInfoOwned, WorkerNodeId, WorkerNodeIdOwned,
        events::{NotifListener as WorkerNotifListener, NotifRecvError as WorkerNotifRecvError},
    },
};

/// Default pool size for the metadata DB.
pub const DEFAULT_POOL_SIZE: u32 = 10;

/// Connection pool to the metadata DB. Clones will refer to the same instance.
#[derive(Clone, Debug)]
pub struct MetadataDb {
    pool: ConnPool,
    pub(crate) url: Arc<str>,
}

impl MetadataDb {
    /// Sets up a connection pool to the Metadata DB
    ///
    /// Runs migrations if necessary.
    #[instrument(skip_all, err)]
    pub async fn connect(url: &str, pool_size: u32) -> Result<Self, Error> {
        Self::connect_with_config(url, pool_size, true).await
    }

    /// Sets up a connection pool to the Metadata DB with configurable migration behavior
    ///
    /// Runs migrations only if `auto_migrate` is true.
    #[instrument(skip_all, err)]
    pub async fn connect_with_config(
        url: &str,
        pool_size: u32,
        auto_migrate: bool,
    ) -> Result<Self, Error> {
        let pool = ConnPool::connect(url, pool_size).await?;
        if auto_migrate {
            pool.run_migrations().await?;
        }
        Ok(Self {
            pool,
            url: url.into(),
        })
    }

    /// Sets up a connection pool to the Metadata DB with retry logic for temporary databases.
    #[instrument(skip_all, err)]
    pub async fn connect_with_retry(url: &str, pool_size: u32) -> Result<Self, Error> {
        use backon::{ExponentialBuilder, Retryable};

        let retry_policy = ExponentialBuilder::default()
            .with_min_delay(Duration::from_millis(10))
            .with_max_delay(Duration::from_millis(100))
            .with_max_times(20);

        fn is_db_starting_up(err: &ConnError) -> bool {
            matches!(
                err,
                ConnError::ConnectionError(sqlx::Error::Database(db_err))
                if db_err.code().is_some_and(|code| code == "57P03")
            )
        }

        fn notify_retry(err: &ConnError, dur: Duration) {
            tracing::warn!(
                error = %err,
                "Database still starting up during connection. Retrying in {:.1}s",
                dur.as_secs_f32()
            );
        }

        let pool = (|| ConnPool::connect(url, pool_size))
            .retry(retry_policy)
            .when(is_db_starting_up)
            .notify(notify_retry)
            .await?;

        pool.run_migrations().await?;

        Ok(Self {
            pool,
            url: url.into(),
        })
    }

    /// Begins a new database transaction
    ///
    /// Returns a `Transaction` that provides RAII semantics - it will automatically
    /// roll back when dropped unless explicitly committed with `.commit()`.
    #[instrument(skip(self), err)]
    pub async fn begin_txn(&self) -> Result<Transaction, Error> {
        let tx = self.pool.begin().await.map_err(Error::Database)?;
        Ok(Transaction::new(tx))
    }

    pub fn default_pool_size() -> u32 {
        DEFAULT_POOL_SIZE
    }
}

// Implement sqlx::Executor for &MetadataDb by delegating to the pool
impl<'c> sqlx::Executor<'c> for &'c MetadataDb {
    type Database = sqlx::Postgres;

    fn fetch_many<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxStream<
        'e,
        Result<
            sqlx::Either<
                <sqlx::Postgres as sqlx::Database>::QueryResult,
                <sqlx::Postgres as sqlx::Database>::Row,
            >,
            sqlx::Error,
        >,
    >
    where
        'c: 'e,
        E: 'q + sqlx::Execute<'q, Self::Database>,
    {
        (&self.pool).fetch_many(query)
    }

    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxFuture<'e, Result<Option<<sqlx::Postgres as sqlx::Database>::Row>, sqlx::Error>>
    where
        'c: 'e,
        E: 'q + sqlx::Execute<'q, Self::Database>,
    {
        (&self.pool).fetch_optional(query)
    }

    fn prepare_with<'e, 'q: 'e>(
        self,
        sql: &'q str,
        parameters: &'e [<sqlx::Postgres as sqlx::Database>::TypeInfo],
    ) -> BoxFuture<'e, Result<<sqlx::Postgres as sqlx::Database>::Statement<'q>, sqlx::Error>>
    where
        'c: 'e,
    {
        (&self.pool).prepare_with(sql, parameters)
    }

    fn describe<'e, 'q: 'e>(
        self,
        sql: &'q str,
    ) -> BoxFuture<'e, Result<sqlx::Describe<Self::Database>, sqlx::Error>>
    where
        'c: 'e,
    {
        (&self.pool).describe(sql)
    }
}

impl<'c> Executor<'c> for &'c MetadataDb {}

impl _priv::Sealed for &MetadataDb {}

/// File metadata-related API
impl MetadataDb {
    /// Inserts new file metadata record.
    ///
    /// Creates a new file metadata entry with the provided information. Uses
    /// ON CONFLICT DO NOTHING to make the operation idempotent.
    /// Also inserts the footer into the footer_cache table.
    #[expect(clippy::too_many_arguments)]
    pub async fn register_file(
        &self,
        location_id: LocationId,
        url: &Url,
        file_name: String,
        object_size: u64,
        object_e_tag: Option<String>,
        object_version: Option<String>,
        parquet_meta: serde_json::Value,
        footer: &Vec<u8>,
    ) -> Result<(), Error> {
        files::insert(
            &*self.pool,
            location_id,
            url,
            file_name,
            object_size,
            object_e_tag,
            object_version,
            parquet_meta,
            footer,
        )
        .await
        .map_err(Error::Database)
    }

    /// Streams file metadata for a specific location.
    ///
    /// Returns a stream of file metadata rows that includes information from both
    /// the file_metadata and locations tables via a JOIN operation.
    pub fn stream_files_by_location_id_with_details(
        &self,
        location_id: LocationId,
    ) -> impl Stream<Item = Result<FileMetadataWithDetails, Error>> + '_ {
        files::stream_with_details(&*self.pool, location_id)
            .map(|result| result.map_err(Error::Database))
    }

    /// List file metadata records for a specific location with cursor-based pagination support
    ///
    /// Uses cursor-based pagination where `last_file_id` is the ID of the last file
    /// from the previous page. For the first page, pass `None` for `last_file_id`.
    /// Returns file metadata records for the specified location ordered by ID in descending order (newest first).
    pub async fn list_files_by_location_id(
        &self,
        location_id: LocationId,
        limit: i64,
        last_file_id: Option<FileId>,
    ) -> Result<Vec<FileMetadata>, Error> {
        match last_file_id {
            Some(file_id) => {
                files::pagination::list_next_page(&*self.pool, location_id, limit, file_id)
                    .await
                    .map_err(Error::Database)
            }
            None => files::pagination::list_first_page(&*self.pool, location_id, limit)
                .await
                .map_err(Error::Database),
        }
    }

    /// Get file metadata by ID with detailed location information
    ///
    /// Retrieves a single file metadata record joined with location data.
    /// Returns the complete file metadata including location URL needed for object store operations.
    /// Returns `None` if the file ID is not found.
    pub async fn get_file_by_id_with_details(
        &self,
        file_id: FileId,
    ) -> Result<Option<FileMetadataWithDetails>, Error> {
        files::get_by_id_with_details(&*self.pool, file_id)
            .await
            .map_err(Error::Database)
    }

    /// Retrieves footer bytes for a specific file.
    ///
    /// Returns the binary footer data stored for the specified file ID.
    #[instrument(skip(self), err)]
    pub async fn get_file_footer_bytes(&self, file_id: FileId) -> Result<Vec<u8>, Error> {
        files::get_footer_bytes_by_id(&*self.pool, file_id)
            .await
            .map_err(Error::Database)
    }

    /// Delete file metadata record by ID
    ///
    /// Also deletes the corresponding footer_cache entry.
    /// Returns `true` if a file was deleted, `false` if no file was found.
    pub async fn delete_file(&self, file_id: FileId) -> Result<bool, Error> {
        files::delete(&*self.pool, file_id)
            .await
            .map_err(Error::Database)
    }
}

#[derive(Debug, sqlx::FromRow)]
pub struct GcManifestRow {
    /// gc_manifest.location_id
    pub location_id: LocationId,
    /// gc_manifest.file_id
    pub file_id: FileId,
    /// gc_manifest.file_path
    pub file_path: String,
    /// gc_manifest.expiration
    pub expiration: NaiveDateTime,
}

// Garbage Collection API
impl MetadataDb {
    pub async fn delete_file_id(&self, file_id: FileId) -> Result<(), Error> {
        let sql = "
        DELETE FROM file_metadata
         WHERE id = $1;
        ";

        sqlx::query(sql)
            .bind(file_id)
            .execute(&*self.pool)
            .await
            .map_err(Error::Database)?;

        Ok(())
    }

    pub async fn delete_file_ids(
        &self,
        file_ids: impl Iterator<Item = &FileId>,
    ) -> Result<Vec<FileId>, Error> {
        let sql = "
        DELETE FROM file_metadata
         WHERE id = ANY($1)
        RETURNING id;
        ";

        sqlx::query_scalar(sql)
            .bind(file_ids.map(|id| **id).collect::<Vec<_>>())
            .fetch_all(&*self.pool)
            .await
            .map_err(Error::Database)
    }

    /// Deletes file metadata entries and inserts them into the GC manifest.
    ///
    /// This eagerly removes files from file_metadata so they won't be listed,
    /// while keeping footer_cache entries until the Collector runs.
    /// The expiration time is set to the current time plus the given duration.
    /// If the file ID already exists in gc_manifest, it updates the expiration time.
    pub async fn upsert_gc_manifest(
        &self,
        location_id: LocationId,
        file_ids: &[FileId],
        duration: Duration,
    ) -> Result<(), Error> {
        let interval = PgInterval {
            microseconds: (duration.as_micros() as u64) as i64,
            ..Default::default()
        };

        let sql = indoc::indoc! {r#"
            WITH deleted_files AS (
                DELETE FROM file_metadata
                WHERE id = ANY($2)
                RETURNING id, file_name
            )
            INSERT INTO gc_manifest (location_id, file_id, file_path, expiration)
            SELECT $1
                  , deleted_files.id
                  , deleted_files.file_name
                  , CURRENT_TIMESTAMP AT TIME ZONE 'UTC' + $3
            FROM deleted_files
            ON CONFLICT (file_id) DO UPDATE SET expiration = EXCLUDED.expiration
        "#};
        sqlx::query(sql)
            .bind(location_id)
            .bind(file_ids.iter().map(|id| **id).collect::<Vec<_>>())
            .bind(interval)
            .execute(&*self.pool)
            .await
            .map_err(Error::Database)?;

        Ok(())
    }

    pub fn stream_expired_files<'a>(
        &'a self,
        location_id: LocationId,
    ) -> BoxStream<'a, Result<GcManifestRow, Error>> {
        let sql = "
        SELECT location_id
             , file_id
             , file_path
             , expiration
          FROM gc_manifest
         WHERE location_id = $1
               AND expiration < CURRENT_TIMESTAMP AT TIME ZONE 'UTC';
        ";

        sqlx::query_as(sql)
            .bind(location_id)
            .fetch(&*self.pool)
            .map_err(Error::Database)
            .boxed()
    }

    /// Deletes footer cache entries for the given file IDs.
    ///
    /// Called by the Collector when gc_manifest entries have expired
    /// and physical files are ready to be deleted.
    pub async fn delete_footer_cache(
        &self,
        file_ids: impl Iterator<Item = &FileId>,
    ) -> Result<(), Error> {
        let sql = "DELETE FROM footer_cache WHERE file_id = ANY($1)";

        sqlx::query(sql)
            .bind(file_ids.map(|id| **id).collect::<Vec<_>>())
            .execute(&*self.pool)
            .await
            .map_err(Error::Database)?;

        Ok(())
    }

    /// Deletes gc_manifest entries for the given file IDs.
    ///
    /// Called by the Collector after physical files have been deleted.
    pub async fn delete_gc_manifest(
        &self,
        file_ids: impl Iterator<Item = &FileId>,
    ) -> Result<(), Error> {
        let sql = "DELETE FROM gc_manifest WHERE file_id = ANY($1)";

        sqlx::query(sql)
            .bind(file_ids.map(|id| **id).collect::<Vec<_>>())
            .execute(&*self.pool)
            .await
            .map_err(Error::Database)?;

        Ok(())
    }

    /// Counts footer_cache entries for a location.
    ///
    /// This counts footer_cache entries that belong to files associated with
    /// this location, whether they are still in file_metadata (current files)
    /// or in gc_manifest (files pending garbage collection).
    #[cfg(feature = "test-utils")]
    pub async fn count_footer_cache_by_location(
        &self,
        location_id: LocationId,
            SELECT COUNT(DISTINCT fc.file_id) FROM footer_cache fc
            WHERE fc.file_id IN (
                SELECT id FROM file_metadata WHERE location_id = $1
                UNION ALL
                SELECT file_id FROM gc_manifest WHERE location_id = $1
            )
                SELECT file_id FROM gc_manifest WHERE location_id = $1
            )
        "#};

        sqlx::query_scalar(sql)
            .bind(location_id)
            .fetch_one(&*self.pool)
            .await
            .map_err(Error::Database)
    }
}

/// Private module for sealed trait pattern
///
/// This module contains the `Sealed` trait used to prevent external
/// implementations of our `Executor` trait. The trait implementations
/// are co-located with the `Executor` trait in `db/exec.rs`.
pub(crate) mod _priv {
    /// Sealed trait to prevent external implementations
    ///
    /// This trait has no methods and serves only as a marker.
    /// Types implement this trait in `db/exec.rs` alongside the
    /// `Executor` trait implementation.
    pub trait Sealed {}
}

#[cfg(test)]
mod tests;

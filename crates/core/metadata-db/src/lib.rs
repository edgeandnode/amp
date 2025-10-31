use std::{sync::Arc, time::Duration};

use futures::{
    StreamExt, TryStreamExt,
    future::BoxFuture,
    stream::{BoxStream, Stream},
};
use sqlx::{postgres::types::PgInterval, types::chrono::NaiveDateTime};
use tokio::time::MissedTickBehavior;
use tracing::instrument;
use url::Url;

pub mod datasets;
mod db;
mod error;
mod files;
pub mod jobs;
mod locations;
pub mod manifests;
pub mod notification_multiplexer;
#[cfg(feature = "temp-db")]
pub mod temp;
mod workers;

use self::db::{ConnPool, Connection};
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
    locations::{
        Location, LocationId, LocationIdFromStrError, LocationIdI64ConvError, LocationIdU64Error,
        LocationWithDetails,
        events::{
            LocationNotifListener, LocationNotifRecvError, LocationNotifSendError,
            LocationNotification,
        },
    },
    manifests::{ManifestHash, ManifestHashOwned, ManifestPath, ManifestPathOwned},
    notification_multiplexer::NotificationMultiplexerHandle,
    workers::{
        NodeId as WorkerNodeId, NodeIdOwned as WorkerNodeIdOwned, Worker, WorkerInfo,
        WorkerInfoOwned,
        events::{NotifListener as WorkerNotifListener, NotifRecvError as WorkerNotifRecvError},
    },
};

/// Frequency on which to send a heartbeat.
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);

/// A worker is considered active if it has sent a heartbeat in this period. The scheduler will
/// schedule new jobs only on active workers.
pub const DEFAULT_DEAD_WORKER_INTERVAL: Duration = Duration::from_secs(5);

/// Default pool size for the metadata DB.
pub const DEFAULT_POOL_SIZE: u32 = 10;

/// Connection pool to the metadata DB. Clones will refer to the same instance.
#[derive(Clone, Debug)]
pub struct MetadataDb {
    pool: ConnPool,
    #[cfg(feature = "temp-db")]
    pub(crate) url: Arc<str>,
    #[cfg(not(feature = "temp-db"))]
    url: Arc<str>,
    dead_worker_interval: Duration,
}

/// Tables are identified by the triple: `(dataset, dataset_version, table)`. For each table, there
/// is at most one active location.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct TableId<'a> {
    pub dataset: &'a str,
    pub dataset_version: Option<&'a str>,
    pub table: &'a str,
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
            dead_worker_interval: DEFAULT_DEAD_WORKER_INTERVAL,
        })
    }

    /// Sets up a connection pool to the Metadata DB with retry logic for temporary databases.
    #[cfg(feature = "temp-db")]
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
            dead_worker_interval: DEFAULT_DEAD_WORKER_INTERVAL,
        })
    }

    /// Configures the "dead worker" interval for the metadata DB instance
    pub fn with_dead_worker_interval(self, dead_worker_interval: Duration) -> Self {
        Self {
            pool: self.pool,
            url: self.url,
            dead_worker_interval,
        }
    }

    /// Begins a new database transaction
    ///
    /// Returns a `Transaction` that provides RAII semantics - it will automatically
    /// roll back when dropped unless explicitly committed with `.commit()`.
    #[instrument(skip(self), err)]
    pub async fn begin_txn(&self) -> Result<Transaction, Error> {
        let tx = self.pool.begin().await.map_err(Error::DbError)?;
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

/// Worker-related API
impl MetadataDb {
    /// Registers a worker in the `workers` table, and updates the latest heartbeat timestamp.
    ///
    /// This operation is idempotent.
    pub async fn register_worker(
        &self,
        node_id: impl Into<WorkerNodeId<'_>>,
        info: impl Into<WorkerInfo<'_>>,
    ) -> Result<(), Error> {
        workers::register(&*self.pool, node_id.into(), info.into()).await?;
        Ok(())
    }

    /// Establish a dedicated connection to the metadata DB, and return a future that loops
    /// forever, updating the worker's heartbeat in the dedicated DB connection.
    ///
    /// If the initial connection fails, an error is returned.
    pub async fn worker_heartbeat_loop(
        &self,
        node_id: impl Into<WorkerNodeIdOwned>,
    ) -> Result<BoxFuture<'static, Result<(), Error>>, Error> {
        let mut conn = Connection::connect(&self.url).await?;

        let node_id = node_id.into();
        let fut = async move {
            let mut interval = tokio::time::interval(HEARTBEAT_INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                workers::update_heartbeat(&mut *conn, node_id.clone()).await?;
            }
        };

        Ok(Box::pin(fut))
    }

    /// Returns a list of active workers.
    ///
    /// A worker is active if it has sent a sufficiently recent heartbeat.
    ///
    /// The dead worker interval can be configured when instantiating the metadata DB.
    pub async fn active_workers(&self) -> Result<Vec<WorkerNodeIdOwned>, Error> {
        Ok(workers::list_active(&*self.pool, self.dead_worker_interval).await?)
    }

    /// Returns a list of all workers.
    ///
    /// Returns all workers in the database with their complete information including
    /// id, node_id, and heartbeat_at timestamp.
    pub async fn list_workers(&self) -> Result<Vec<Worker>, Error> {
        Ok(workers::list(&*self.pool).await?)
    }

    /// Listen to the worker actions notification channel for job notifications
    ///
    /// The listener will only yield notifications targeted to the specified `node_id`.
    pub async fn listen_for_job_notifications<'a, N>(
        &self,
        node_id: N,
    ) -> Result<WorkerNotifListener, Error>
    where
        N: Into<WorkerNodeId<'a>>,
    {
        workers::events::listen_url(&self.url, node_id.into().to_owned())
            .await
            .map_err(Into::into)
    }

    /// Send a job notification to a worker
    ///
    /// This function sends a notification with a custom payload to the specified worker node.
    /// The payload must implement `serde::Serialize` and will be serialized to JSON.
    ///
    /// # Usage
    ///
    /// Typically called after successful job state transitions (e.g., after scheduling a job
    /// or requesting a job stop) to notify the worker of the change.
    #[instrument(skip(self, payload), err)]
    pub async fn send_job_notification<T>(
        &self,
        node_id: impl Into<WorkerNodeIdOwned> + std::fmt::Debug,
        payload: &T,
    ) -> Result<(), Error>
    where
        T: serde::Serialize,
    {
        workers::events::notify(&*self.pool, node_id.into(), payload).await?;
        Ok(())
    }
}

/// Location-related API
impl MetadataDb {
    /// Register a materialized table into the metadata database.
    ///
    /// If setting `active = true`, make sure no other active location exists for this table, to avoid
    /// a constraint violation. If an active location might exist, it is better to initialize the
    /// location with `active = false` and then call `set_active_location` to switch it as active.
    #[instrument(skip(self), err)]
    pub async fn register_location(
        &self,
        table: TableId<'_>,
        bucket: Option<&str>,
        path: &str,
        url: &Url,
        active: bool,
    ) -> Result<LocationId, sqlx::Error> {
        locations::insert(&*self.pool, table, bucket, path, url, active).await
    }

    pub async fn get_location_by_id(&self, id: LocationId) -> Result<Option<Location>, Error> {
        Ok(locations::get_by_id(&*self.pool, id).await?)
    }

    pub async fn url_to_location_id(&self, url: &Url) -> Result<Option<LocationId>, Error> {
        Ok(locations::url_to_id(&*self.pool, url).await?)
    }

    /// Returns the active location. The active location has meaning on both the write and read side:
    /// - On the write side, it is the location that is being kept in sync with the source data.
    /// - On the read side, it is default location that should receive queries.
    #[instrument(skip(self), err)]
    pub async fn get_active_location(
        &self,
        table: TableId<'_>,
    ) -> Result<Option<(Url, LocationId)>, Error> {
        let active_locations = locations::get_active_by_table_id(&*self.pool, table).await?;

        match active_locations.as_slice() {
            [] => Ok(None),
            [(url, location_id)] => {
                let url = Url::parse(url)?;
                Ok(Some((url, *location_id)))
            }
            multiple => Err(Error::MultipleActiveLocations(
                table.dataset.to_string(),
                table.dataset_version.unwrap_or("").to_string(),
                table.table.to_string(),
                multiple.iter().map(|(url, _)| url.clone()).collect(),
            )),
        }
    }

    /// Set a location as the active materialization for a table.
    ///
    /// If there was a previously active location, it will be made inactive in the
    /// same transaction, achieving an atomic switch.
    #[instrument(skip(self), err)]
    pub async fn set_active_location(
        &self,
        table: TableId<'_>,
        location: &Url,
    ) -> Result<(), Error> {
        let mut tx = self.pool.begin().await?;
        locations::mark_inactive_by_table_id(&mut *tx, table).await?;
        locations::mark_active_by_url(&mut *tx, table, location).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Returns locations that were written by a specific job.
    ///
    /// This method queries the `locations` table for all locations where the given job
    /// was assigned as the writer, converting them to [`Location`] type.
    pub async fn output_locations(&self, id: impl Into<JobId>) -> Result<Vec<Location>, Error> {
        Ok(locations::get_by_job_id(&*self.pool, id.into()).await?)
    }

    /// List locations with cursor-based pagination support
    ///
    /// Uses cursor-based pagination where `last_location_id` is the ID of the last location
    /// from the previous page. For the first page, pass `None` for `last_location_id`.
    pub async fn list_locations(
        &self,
        limit: i64,
        last_location_id: Option<LocationId>,
    ) -> Result<Vec<Location>, Error> {
        match last_location_id {
            Some(location_id) => {
                Ok(locations::list_next_page(&*self.pool, limit, location_id).await?)
            }
            None => Ok(locations::list_first_page(&*self.pool, limit).await?),
        }
    }

    // Get a location by its ID with full details including writer job
    //
    // Returns the location if found, or None if no location exists with the given ID.
    pub async fn get_location_by_id_with_details(
        &self,
        location_id: LocationId,
    ) -> Result<Option<LocationWithDetails>, Error> {
        Ok(locations::get_by_id_with_details(&*self.pool, location_id).await?)
    }

    /// Delete a location by its ID
    ///
    /// This will also delete all associated file_metadata entries due to CASCADE.
    /// Returns true if the location was deleted, false if it didn't exist.
    pub async fn delete_location_by_id(&self, location_id: LocationId) -> Result<bool, Error> {
        Ok(locations::delete_by_id(&*self.pool, location_id).await?)
    }

    /// Notify that a location has changed
    #[instrument(skip(self), err)]
    pub async fn notify_location_change(&self, location_id: LocationId) -> Result<(), Error> {
        locations::events::notify(&*self.pool, location_id).await?;
        Ok(())
    }

    /// Listen for location change notifications
    pub async fn listen_for_location_notifications(&self) -> Result<LocationNotifListener, Error> {
        locations::events::listen_url(&self.url)
            .await
            .map_err(Into::into)
    }
}

/// File metadata-related API
impl MetadataDb {
    /// Inserts new file metadata record.
    ///
    /// Creates a new file metadata entry with the provided information. Uses
    /// ON CONFLICT DO NOTHING to make the operation idempotent.
    #[allow(clippy::too_many_arguments)]
    pub async fn register_file(
        &self,
        location_id: LocationId,
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
            file_name,
            object_size,
            object_e_tag,
            object_version,
            parquet_meta,
            footer,
        )
        .await
        .map_err(Error::DbError)
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
            .map(|result| result.map_err(Error::DbError))
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
                Ok(
                    files::pagination::list_next_page(&*self.pool, location_id, limit, file_id)
                        .await?,
                )
            }
            None => Ok(files::pagination::list_first_page(&*self.pool, location_id, limit).await?),
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
            .map_err(Error::DbError)
    }

    /// Retrieves footer bytes for a specific file.
    ///
    /// Returns the binary footer data stored for the specified file ID.
    pub async fn get_file_footer_bytes(&self, file_id: FileId) -> Result<Vec<u8>, Error> {
        files::get_footer_bytes_by_id(&*self.pool, file_id)
            .await
            .map_err(Error::DbError)
    }

    /// Delete file metadata record by ID
    ///
    /// Returns `true` if a file was deleted, `false` if no file was found.
    pub async fn delete_file(&self, file_id: FileId) -> Result<bool, Error> {
        files::delete(&*self.pool, file_id)
            .await
            .map_err(Error::DbError)
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

        sqlx::query(sql).bind(file_id).execute(&*self.pool).await?;

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

        Ok(sqlx::query_scalar(sql)
            .bind(file_ids.map(|id| **id).collect::<Vec<_>>())
            .fetch_all(&*self.pool)
            .await?)
    }

    /// Inserts or updates the GC manifest for the given file IDs.
    /// If a file ID already exists, it updates the expiration time.
    /// The expiration time is set to the current time plus the given duration.
    /// If the file ID does not exist, it inserts a new row.
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

        let sql = "
            INSERT INTO gc_manifest (location_id, file_id, file_path, expiration)
            SELECT $1
                  , file.id
                  , file_metadata.file_name
                  , CURRENT_TIMESTAMP AT TIME ZONE 'UTC' + $3
               FROM UNNEST ($2) AS file(id)
         INNER JOIN file_metadata ON file_metadata.id = file.id
        ON CONFLICT (file_id) DO UPDATE SET expiration = EXCLUDED.expiration;
        ";
        sqlx::query(sql)
            .bind(location_id)
            .bind(file_ids.iter().map(|id| **id).collect::<Vec<_>>())
            .bind(interval)
            .execute(&*self.pool)
            .await?;

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
            .map_err(Error::DbError)
            .boxed()
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

use std::{future::Future, sync::Arc, time::Duration};

use futures::{
    StreamExt, TryStreamExt,
    stream::{BoxStream, Stream},
};
use sqlx::{
    postgres::{PgListener, PgNotification, types::PgInterval},
    types::chrono::{DateTime, NaiveDateTime, Utc},
};
use thiserror::Error;
use tokio::time::MissedTickBehavior;
use tracing::instrument;
use url::Url;

mod conn;
mod jobs;
mod locations;
pub mod registry;
#[cfg(feature = "temp-db")]
pub mod temp;
mod workers;

use self::conn::{DbConn, DbConnPool};
#[cfg(feature = "temp-db")]
pub use self::temp::{KEEP_TEMP_DIRS, temp_metadata_db};
pub use self::{
    jobs::{
        Job, JobId, JobIdFromStrError, JobIdI64ConvError, JobIdU64Error, JobStatus,
        JobStatusUpdateError, JobWithDetails,
    },
    locations::{
        Location, LocationId, LocationIdFromStrError, LocationIdI64ConvError, LocationIdU64Error,
        LocationWithDetails,
    },
    workers::{
        Worker, WorkerNodeId,
        events::{JobNotifAction, JobNotifListener, JobNotifRecvError, JobNotification},
    },
};
use crate::registry::{Registry, insert_dataset_to_registry};

/// Frequency on which to send a heartbeat.
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);

/// A worker is considered active if it has sent a heartbeat in this period. The scheduler will
/// schedule new jobs only on active workers.
pub const DEFAULT_DEAD_WORKER_INTERVAL: Duration = Duration::from_secs(5);

/// Default pool size for the metadata DB.
pub const DEFAULT_POOL_SIZE: u32 = 10;

/// Row ids, always non-negative.
pub type FileId = i64;
pub type FooterBytes = Vec<u8>;

#[derive(Debug, sqlx::FromRow)]
pub struct FileMetadataRow {
    /// file_metadata.id
    pub id: FileId,
    /// file_metadata.location_id
    pub location_id: LocationId,
    /// file_metadata.file_name
    pub file_name: String,
    /// location.url
    pub url: String,
    /// file_metadata.object_size
    pub object_size: Option<i64>,
    /// file_metadata.object_e_tag
    pub object_e_tag: Option<String>,
    /// file_metadata.object_version
    pub object_version: Option<String>,
    /// file_metadata.metadata
    pub metadata: serde_json::Value,
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("Error connecting to metadata db: {0}")]
    ConnectionError(sqlx::Error),

    #[error("Error running migrations: {0}")]
    MigrationError(#[from] sqlx::migrate::MigrateError),

    #[error("Error executing database query: {0}")]
    DbError(#[from] sqlx::Error),

    #[error("Error sending notification: {0}")]
    NotificationSendError(#[from] workers::events::JobNotifSendError),

    #[error("Error receiving notification: {0}")]
    NotificationRecvError(#[from] workers::events::JobNotifRecvError),

    #[error(
        "Multiple active locations found for dataset={0}, dataset_version={1}, table={2}: {3:?}"
    )]
    MultipleActiveLocations(String, String, String, Vec<String>),

    #[error("Error parsing URL: {0}")]
    UrlParseError(#[from] url::ParseError),

    #[error("Job status update error: {0}")]
    JobStatusUpdateError(#[from] jobs::JobStatusUpdateError),
}

impl Error {
    /// Returns `true` if the error is likely to be a transient connection issue.
    ///
    /// This is used to determine if an operation should be retried.
    ///
    /// The following errors are considered retryable:
    /// - `Error::ConnectionError`: This is a wrapper around `sqlx::Error` that is returned when
    ///   the initial connection to the database fails.
    /// - `sqlx::Error::Io`: An I/O error, often indicating a network issue or a closed socket.
    /// - `sqlx::Error::Tls`: An error that occurred during the TLS handshake.
    /// - `sqlx::Error::PoolTimedOut`: The connection pool timed out waiting for a free connection.
    /// - `sqlx::Error::PoolClosed`: The connection pool was closed while an operation was pending.
    ///
    /// Other database errors, such as constraint violations or serialization issues, are not
    /// considered transient and will not be retried.
    pub fn is_connection_error(&self) -> bool {
        match self {
            Error::ConnectionError(_) => true,
            Error::DbError(err) => matches!(
                err,
                sqlx::Error::Io(_)
                    | sqlx::Error::Tls(_)
                    | sqlx::Error::PoolTimedOut
                    | sqlx::Error::PoolClosed
            ),
            _ => false,
        }
    }
}

impl From<conn::ConnError> for Error {
    fn from(err: conn::ConnError) -> Self {
        match err {
            conn::ConnError::ConnectionError(err) => Error::ConnectionError(err),
            conn::ConnError::MigrationFailed(err) => Error::MigrationError(err),
        }
    }
}

/// Connection pool to the metadata DB. Clones will refer to the same instance.
#[derive(Clone, Debug)]
pub struct MetadataDb {
    pub pool: DbConnPool,
    pub(crate) url: Arc<str>,
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
        let pool = DbConnPool::connect(url, pool_size).await?;
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

        fn is_db_starting_up(err: &conn::ConnError) -> bool {
            matches!(
                err,
                conn::ConnError::ConnectionError(sqlx::Error::Database(db_err))
                if db_err.code().map_or(false, |code| code == "57P03")
            )
        }

        fn notify_retry(err: &conn::ConnError, dur: Duration) {
            tracing::warn!(
                error = %err,
                "Database still starting up during connection. Retrying in {:.1}s",
                dur.as_secs_f32()
            );
        }

        let pool = (|| DbConnPool::connect(url, pool_size))
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

    pub fn default_pool_size() -> u32 {
        DEFAULT_POOL_SIZE
    }
}

/// Worker-related API
impl MetadataDb {
    /// Registers a worker in the `workers` table, and updates the latest heartbeat timestamp.
    ///
    /// This operation is idempotent.
    pub async fn register_worker(&self, node_id: &WorkerNodeId) -> Result<(), Error> {
        workers::heartbeat::register_worker(&*self.pool, node_id).await?;
        Ok(())
    }

    /// Establish a dedicated connection to the metadata DB, and return a [`Future`] that loops
    /// forever, updating the worker's heartbeat in the dedicated DB connection.
    ///
    /// If the initial connection fails, an error is returned.
    pub async fn worker_heartbeat_loop(
        &self,
        node_id: WorkerNodeId,
    ) -> Result<impl Future<Output = Result<(), Error>> + use<>, Error> {
        let mut conn = DbConn::connect(&self.url).await?;

        let fut = async move {
            let mut interval = tokio::time::interval(HEARTBEAT_INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                workers::heartbeat::update_heartbeat(&mut *conn, &node_id).await?;
            }
        };

        Ok(fut)
    }

    /// Returns a list of active workers.
    ///
    /// A worker is active if it has sent a sufficiently recent heartbeat.
    ///
    /// The dead worker interval can be configured when instantiating the metadata DB.
    pub async fn active_workers(&self) -> Result<Vec<WorkerNodeId>, Error> {
        Ok(workers::heartbeat::get_active_workers(&*self.pool, self.dead_worker_interval).await?)
    }
}

/// Job-related API
impl MetadataDb {
    /// Listen to the worker actions notification channel for job notifications
    pub async fn listen_for_job_notifications(&self) -> Result<JobNotifListener, Error> {
        workers::events::listen_url(&self.url)
            .await
            .map_err(Into::into)
    }

    /// Schedules a job on the given worker
    ///
    /// The job will only be scheduled if the locations are successfully locked.
    ///
    /// This function performs in a single transaction:
    ///
    ///  1. Registers the job in the workers job queue
    ///  2. Locks the locations
    ///  3. Sends a notification to the worker
    ///
    /// If any of these steps fail, the transaction is rolled back, and no notification is sent.
    #[instrument(skip(self), err)]
    pub async fn schedule_job(
        &self,
        node_id: &WorkerNodeId,
        job_desc: &str,
        locations: &[LocationId],
    ) -> Result<JobId, Error> {
        // Use a transaction, such that the job will only be scheduled if the locations are
        // successfully locked.
        let mut tx = self.pool.begin().await?;

        // Register the job in the workers job queue
        let job_id = jobs::insert_with_default_status(&mut *tx, node_id, job_desc).await?;

        // Lock the locations for this job by assigning the job ID as the writer
        locations::assign_job_writer(&mut *tx, locations, job_id).await?;

        // Notify the worker about the new job
        workers::events::notify(&mut *tx, JobNotification::start(node_id.to_owned(), job_id))
            .await?;

        tx.commit().await?;

        Ok(job_id)
    }

    /// Atomically update job status to StopRequested and notify worker
    ///
    /// This function will only update the job status if it's currently in a valid state
    /// to be stopped (Scheduled or Running). If the job is already stopping, this is
    /// considered success (idempotent behavior). If the job is in a terminal state
    /// (Stopped, Completed, Failed), this returns a conflict error.
    ///
    /// This function performs in a single transaction:
    ///  1. Conditionally updates the job status to StopRequested
    ///  2. Sends a notification to the worker if the update succeeded
    ///
    /// Returns an error if the job doesn't exist, is in a terminal state, or if there's a database error.
    #[instrument(skip(self), err)]
    pub async fn request_job_stop(
        &self,
        job_id: &JobId,
        node_id: &WorkerNodeId,
    ) -> Result<(), Error> {
        // Use transaction for atomic update and notification
        let mut tx = self.pool.begin().await?;

        // Try to update job status
        match jobs::update_status_if_any_state(
            &mut *tx,
            job_id,
            &[JobStatus::Running, JobStatus::Scheduled],
            JobStatus::StopRequested,
        )
        .await
        {
            Ok(()) => {} // OK!
            // Check if the job is already stopping (idempotent behavior)
            Err(JobStatusUpdateError::StateConflict {
                actual: JobStatus::StopRequested | JobStatus::Stopping,
                ..
            }) => {
                return Ok(());
            }
            Err(other) => return Err(other.into()),
        }

        // Send notification to worker
        workers::events::notify(&mut *tx, JobNotification::stop(node_id.clone(), *job_id)).await?;

        tx.commit().await?;
        Ok(())
    }

    /// List jobs with cursor-based pagination support
    ///
    /// Uses cursor-based pagination where `last_job_id` is the ID of the last job
    /// from the previous page. For the first page, pass `None` for `last_job_id`.
    pub async fn list_jobs_with_details(
        &self,
        limit: i64,
        last_job_id: Option<JobId>,
    ) -> Result<Vec<JobWithDetails>, Error> {
        match last_job_id {
            Some(job_id) => Ok(jobs::list_next_page(&*self.pool, limit, job_id).await?),
            None => Ok(jobs::list_first_page(&*self.pool, limit).await?),
        }
    }

    /// Given a worker [`WorkerNodeId`], return all the scheduled jobs
    ///
    /// A job is considered scheduled if it's in one of the following non-terminal states:
    /// - [`JobStatus::Scheduled`]
    /// - [`JobStatus::Running`]
    ///
    /// This method is used to fetch all the jobs that the worker should be running after a restart.
    pub async fn get_scheduled_jobs(&self, node_id: &WorkerNodeId) -> Result<Vec<Job>, Error> {
        Ok(jobs::get_by_node_id_and_statuses(
            &*self.pool,
            node_id,
            [JobStatus::Scheduled, JobStatus::Running],
        )
        .await?)
    }

    /// Given a worker [`WorkerNodeId`], return all the active jobs
    ///
    /// A job is considered active if it's in  on of the following non-terminal states:
    /// - [`JobStatus::Scheduled`]
    /// - [`JobStatus::Running`]
    /// - [`JobStatus::StopRequested`]
    ///
    /// When connection issues cause the job notification channel to miss notifications, a job reconciliation routine
    /// ensures each worker's job set remains synchronized with the Metadata DB. This method fetches all jobs that a
    /// worker should be tracking, enabling the worker to reconcile its state when notifications are lost.
    pub async fn get_active_jobs(&self, node_id: &WorkerNodeId) -> Result<Vec<Job>, Error> {
        Ok(jobs::get_by_node_id_and_statuses(
            &*self.pool,
            node_id,
            [
                JobStatus::Scheduled,
                JobStatus::Running,
                JobStatus::StopRequested,
            ],
        )
        .await?)
    }

    /// Returns the job with the given ID
    pub async fn get_job(&self, id: &JobId) -> Result<Option<Job>, Error> {
        Ok(jobs::get_by_id(&*self.pool, id).await?)
    }

    /// Get a job by ID with full details including timestamps
    pub async fn get_job_by_id_with_details(
        &self,
        id: &JobId,
    ) -> Result<Option<JobWithDetails>, Error> {
        Ok(jobs::get_by_id_with_details(&*self.pool, id).await?)
    }

    /// Conditionally marks a job as `RUNNING` only if it's currently `SCHEDULED`
    ///
    /// This provides idempotent behavior - if the job is already running, completed, or failed,
    /// the appropriate error will be returned indicating the state conflict.
    pub async fn mark_job_running(&self, id: &JobId) -> Result<(), Error> {
        Ok(jobs::update_status_if_any_state(
            &*self.pool,
            id,
            &[JobStatus::Scheduled],
            JobStatus::Running,
        )
        .await?)
    }

    /// Conditionally marks a job as `STOPPING` only if it's currently `STOP_REQUESTED`
    ///
    /// This is typically used by workers to acknowledge a stop request.
    pub async fn mark_job_stopping(&self, id: &JobId) -> Result<(), Error> {
        Ok(jobs::update_status_if_any_state(
            &*self.pool,
            id,
            &[JobStatus::StopRequested],
            JobStatus::Stopping,
        )
        .await?)
    }

    /// Conditionally marks a job as `STOPPED` only if it's currently `STOPPING`
    ///
    /// This provides proper state transition from stopping to stopped.
    pub async fn mark_job_stopped(&self, id: &JobId) -> Result<(), Error> {
        Ok(jobs::update_status_if_any_state(
            &*self.pool,
            id,
            &[JobStatus::Stopping],
            JobStatus::Stopped,
        )
        .await?)
    }

    /// Conditionally marks a job as `COMPLETED` only if it's currently `RUNNING`
    ///
    /// This ensures jobs can only be completed from a running state.
    pub async fn mark_job_completed(&self, id: &JobId) -> Result<(), Error> {
        Ok(jobs::update_status_if_any_state(
            &*self.pool,
            id,
            &[JobStatus::Running],
            JobStatus::Completed,
        )
        .await?)
    }

    /// Conditionally marks a job as `FAILED` from either `RUNNING` or `SCHEDULED` states
    ///
    /// Jobs can fail from either scheduled (startup failure) or running (runtime failure) states.
    pub async fn mark_job_failed(&self, id: &JobId) -> Result<(), Error> {
        Ok(jobs::update_status_if_any_state(
            &*self.pool,
            id,
            &[JobStatus::Scheduled, JobStatus::Running],
            JobStatus::Failed,
        )
        .await?)
    }

    /// Delete a job by ID if it's in a terminal state
    ///
    /// This function will only delete the job if it exists and is in a terminal state
    /// (Completed, Stopped, or Failed). Returns true if a job was deleted, false otherwise.
    pub async fn delete_job_if_terminal(&self, id: &JobId) -> Result<bool, Error> {
        Ok(
            jobs::delete_by_id_and_statuses(&*self.pool, id, JobStatus::terminal_statuses())
                .await?,
        )
    }

    /// Delete all jobs that are in terminal states
    ///
    /// This function deletes all jobs that are in terminal states (Completed, Stopped, or Failed).
    /// Returns the number of jobs that were deleted.
    pub async fn delete_all_terminal_jobs(&self) -> Result<usize, Error> {
        Ok(jobs::delete_by_statuses(&*self.pool, JobStatus::terminal_statuses()).await?)
    }

    /// Delete all jobs that match the specified status
    ///
    /// This function deletes all jobs that are in the specified status.
    /// Returns the number of jobs that were deleted.
    pub async fn delete_all_jobs_by_status(&self, status: JobStatus) -> Result<usize, Error> {
        Ok(jobs::delete_by_status(&*self.pool, status).await?)
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
        // An empty `dataset_version` is represented as an empty string in the DB.
        let dataset_version = table.dataset_version.unwrap_or("");
        let mut tx = self.pool.begin().await?;

        let query = "
            INSERT INTO locations (dataset, dataset_version, tbl, bucket, path, url, active, start_block)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT DO NOTHING;
        ";

        sqlx::query(query)
            .bind(table.dataset)
            .bind(dataset_version)
            .bind(table.table)
            .bind(bucket)
            .bind(path)
            .bind(url.to_string())
            .bind(active)
            .bind(0)
            .execute(&mut *tx)
            .await?;

        let query = "
            SELECT id
            FROM locations
            WHERE dataset = $1 AND dataset_version = $2 AND tbl = $3 AND url = $4
        ";

        let location_id: LocationId = sqlx::query_scalar(query)
            .bind(table.dataset)
            .bind(dataset_version)
            .bind(table.table)
            .bind(url.to_string())
            .fetch_one(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(location_id)
    }

    pub async fn get_location_by_id(&self, id: LocationId) -> Result<Option<Location>, Error> {
        sqlx::query_as("SELECT * FROM locations WHERE id = $1")
            .bind(id)
            .fetch_optional(&*self.pool)
            .await
            .map_err(Error::from)
    }

    pub async fn url_to_location_id(&self, url: &Url) -> Result<Option<LocationId>, Error> {
        Ok(locations::url_to_location_id(&*self.pool, url).await?)
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
    pub async fn output_locations(&self, id: JobId) -> Result<Vec<Location>, Error> {
        Ok(locations::get_by_job_id(&*self.pool, id).await?)
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
}

/// File metadata-related API
impl MetadataDb {
    pub fn stream_file_metadata(
        &self,
        location_id: LocationId,
    ) -> impl Stream<Item = Result<FileMetadataRow, Error>> + '_ {
        let query = "
        SELECT fm.id
             , fm.location_id
             , fm.file_name
             , l.url
             , fm.object_size
             , fm.object_e_tag
             , fm.object_version
             , fm.metadata
          FROM file_metadata fm
          JOIN locations l ON fm.location_id = l.id
         WHERE location_id = $1;
        ";

        sqlx::query_as(query)
            .bind(location_id)
            .fetch(&*self.pool)
            .map(|result| result.map_err(Error::DbError))
    }

    pub async fn insert_metadata(
        &self,
        location_id: LocationId,
        file_name: String,
        object_size: u64,
        object_e_tag: Option<String>,
        object_version: Option<String>,
        parquet_meta: serde_json::Value,
        footer: &Vec<u8>,
    ) -> Result<(), Error> {
        let sql = "
        INSERT INTO file_metadata (location_id, file_name, object_size, object_e_tag, object_version, metadata, footer)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT DO NOTHING
        ";

        sqlx::query(sql)
            .bind(location_id)
            .bind(file_name)
            .bind(object_size as i64)
            .bind(object_e_tag)
            .bind(object_version)
            .bind(parquet_meta)
            .bind(footer)
            .execute(&*self.pool)
            .await?;

        Ok(())
    }

    pub async fn get_footer_bytes(&self, file_id: FileId) -> Result<Vec<u8>, Error> {
        let sql = "
        SELECT footer
          FROM file_metadata
         WHERE id = $1;
        ";

        Ok(sqlx::query_scalar(sql)
            .bind(file_id)
            .fetch_one(&*self.pool)
            .await?)
    }
}

/// Generic notification API
impl MetadataDb {
    #[instrument(skip(self), err)]
    pub async fn notify(&self, channel_name: &str, payload: &str) -> Result<(), Error> {
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(channel_name)
            .bind(payload)
            .execute(&*self.pool)
            .await?;
        Ok(())
    }

    #[instrument(skip(self), err)]
    pub async fn notify_location_change(&self, location_id: LocationId) -> Result<(), Error> {
        self.notify("change-tracking", &location_id.to_string())
            .await
    }

    /// Listens on a PostgreSQL notification channel using LISTEN.
    ///
    /// # Connection management
    ///
    /// This does not take a connection from the pool, but instead establishes a new connection
    /// to the database. This connection is maintained for the lifetime of the stream.
    ///
    /// # Error cases
    ///
    /// This stream will generally not return `Err`, except on failure to estabilish the intial
    /// connection, because connection errors are retried.
    ///
    /// # Delivery Guarantees
    ///
    /// - Notifications sent before the LISTEN command is issued will not be delivered.
    /// - Notifications may be lost during automatic retry of a closed DB connection.
    #[instrument(skip(self), err)]
    pub async fn listen(
        &self,
        channel_name: &str,
    ) -> Result<impl Stream<Item = Result<PgNotification, sqlx::Error>> + use<>, Error> {
        let mut channel = PgListener::connect(&self.url)
            .await
            .map_err(Error::ConnectionError)?;
        channel.listen(channel_name).await.map_err(Error::DbError)?;
        Ok(channel.into_stream())
    }
}

/// Registry API
impl MetadataDb {
    pub async fn register_dataset(&self, registry: Registry) -> Result<(), Error> {
        let mut tx = self.pool.begin().await?;
        insert_dataset_to_registry(&mut *tx, registry).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn dataset_exists(&self, dataset_name: &str, version: &str) -> Result<bool, Error> {
        let sql = "
            SELECT COUNT(*) FROM registry 
            WHERE dataset = $1 AND version = $2
        ";
        let count: i64 = sqlx::query_scalar(sql)
            .bind(dataset_name)
            .bind(version)
            .fetch_one(&*self.pool)
            .await?;
        Ok(count > 0)
    }

    pub async fn get_manifest(
        &self,
        dataset: &str,
        version: &str,
    ) -> Result<Option<String>, Error> {
        let sql = "
            SELECT manifest FROM registry 
            WHERE dataset = $1 AND version = $2
        ";
        let result = sqlx::query_scalar(sql)
            .bind(dataset)
            .bind(version)
            .fetch_optional(&*self.pool)
            .await?;
        Ok(result)
    }

    #[instrument(skip(self), err)]
    pub async fn get_latest_dataset_version(
        &self,
        dataset_name: &str,
    ) -> Result<Option<(String, String)>, Error> {
        let sql = "
            SELECT version FROM registry 
            WHERE dataset = $1
            ORDER BY version DESC
            LIMIT 1
        ";

        let version: Option<String> = sqlx::query_scalar(sql)
            .bind(&dataset_name)
            .fetch_optional(&*self.pool)
            .await?;
        match version {
            Some(version) => Ok(Some((dataset_name.to_string(), version))),
            None => Ok(None),
        }
    }

    #[instrument(skip(self), err)]
    pub async fn get_dataset(
        &self,
        dataset_name: &str,
        version: &str,
    ) -> Result<Option<String>, Error> {
        let sql = "
            SELECT manifest FROM registry 
            WHERE dataset = $1 AND version = $2
            LIMIT 1
        ";

        let dataset: Option<String> = sqlx::query_scalar(sql)
            .bind(&dataset_name)
            .bind(&version)
            .fetch_optional(&*self.pool)
            .await?;
        match dataset {
            Some(dataset) => Ok(Some(dataset.trim_end_matches(".json").to_string())),
            None => Ok(None),
        }
    }

    pub async fn get_registry_info(
        &self,
        dataset_name: &str,
        version: &str,
    ) -> Result<Registry, sqlx::Error> {
        let sql = "SELECT owner, dataset, version, manifest FROM registry WHERE dataset = $1 AND version = $2";
        let dataset = sqlx::query_as(sql)
            .bind(&dataset_name)
            .bind(&version)
            .fetch_one(&*self.pool)
            .await?;
        Ok(dataset)
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
    pub async fn delete_file_ids(&self, file_ids: &[FileId]) -> Result<(), Error> {
        let sql = "
        DELETE FROM gc_manifest
         WHERE file_id = ANY($1);
        ";

        sqlx::query(sql).bind(file_ids).execute(&*self.pool).await?;

        Ok(())
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
        let mut interval = PgInterval::default();
        interval.microseconds = duration.as_micros() as i64;

        let sql = "
            INSERT INTO gc_manifest (location_id, file_id, file_path, expiration)
            SELECT $1
                  , file.id
                  , locations.url || file_metadata.file_name
                  , NOW() + $3
               FROM UNNEST ($2) AS file(id)
         INNER JOIN file_metadata ON file_metadata.id = file.id
         INNER JOIN locations ON file_metadata.location_id = locations.id
        ON CONFLICT (file_id) DO UPDATE SET expiration = EXCLUDED.expiration;
        ";

        sqlx::query(sql)
            .bind(location_id)
            .bind(file_ids.as_ref())
            .bind(interval)
            .execute(&*self.pool)
            .await?;

        Ok(())
    }

    pub fn stream_expired_files<'a>(
        &'a self,
        location_id: LocationId,
        secs: i64,
        nsecs: u32,
    ) -> BoxStream<'a, Result<GcManifestRow, Error>> {
        let sql = "
        SELECT location_id
             , file_id
             , file_path
             , expiration
          FROM gc_manifest
         WHERE location_id = $1
               AND expiration <= $2;
        ";

        let expiration = DateTime::<Utc>::from_timestamp(secs, nsecs);

        sqlx::query_as(sql)
            .bind(location_id)
            .bind(expiration)
            .fetch(&*self.pool)
            .map_err(Error::DbError)
            .boxed()
    }
}

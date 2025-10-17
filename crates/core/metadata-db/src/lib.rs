use std::{sync::Arc, time::Duration};

use futures::{
    StreamExt, TryStreamExt,
    future::BoxFuture,
    stream::{BoxStream, Stream},
};
use sqlx::{postgres::types::PgInterval, types::chrono::NaiveDateTime};
use thiserror::Error;
use tokio::time::MissedTickBehavior;
use tracing::instrument;
use url::Url;

mod conn;
mod datasets;
mod files;
mod jobs;
mod locations;
pub mod notification_multiplexer;
#[cfg(feature = "temp-db")]
pub mod temp;
mod workers;

use self::conn::{DbConn, DbConnPool};
#[cfg(feature = "temp-db")]
pub use self::temp::{KEEP_TEMP_DIRS, temp_metadata_db};
pub use self::{
    datasets::{
        Hash as DatasetHash, HashOwned as DatasetHashOwned, Name as DatasetName,
        NameOwned as DatasetNameOwned, Namespace as DatasetNamespace,
        NamespaceOwned as DatasetNamespaceOwned, Version as DatasetVersion,
        VersionOwned as DatasetVersionOwned, tags::Tag as DatasetTag,
    },
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
    notification_multiplexer::NotificationMultiplexerHandle,
    workers::{
        NodeId as WorkerNodeId, NodeIdOwned as WorkerNodeIdOwned, Worker,
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

#[derive(Error, Debug)]
pub enum Error {
    #[error("Error connecting to metadata db: {0}")]
    ConnectionError(sqlx::Error),

    #[error("Error running migrations: {0}")]
    MigrationError(#[from] sqlx::migrate::MigrateError),

    #[error("Error executing database query: {0}")]
    DbError(#[from] sqlx::Error),

    #[error("Error sending job notification: {0}")]
    JobNotificationSendError(#[from] workers::events::NotifSendError),

    #[error("Error receiving job notification: {0}")]
    JobNotificationRecvError(#[from] workers::events::NotifRecvError),

    #[error("Error sending location notification: {0}")]
    LocationNotificationSendError(#[from] locations::events::LocationNotifSendError),

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
    pool: DbConnPool,
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
                if db_err.code().is_some_and(|code| code == "57P03")
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
    pub async fn register_worker(&self, node_id: impl Into<WorkerNodeId<'_>>) -> Result<(), Error> {
        workers::heartbeat::register_worker(&*self.pool, node_id.into()).await?;
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
        let mut conn = DbConn::connect(&self.url).await?;

        let node_id = node_id.into();
        let fut = async move {
            let mut interval = tokio::time::interval(HEARTBEAT_INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                workers::heartbeat::update_heartbeat(&mut *conn, node_id.clone()).await?;
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
        Ok(workers::heartbeat::get_active_workers(&*self.pool, self.dead_worker_interval).await?)
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

/// Job-related API
impl MetadataDb {
    /// Schedules a job on the given worker
    ///
    /// The job will only be scheduled if the locations are successfully locked.
    ///
    /// This function performs in a single transaction:
    ///
    ///  1. Registers the job in the workers job queue
    ///  2. Locks the locations
    ///
    /// If any of these steps fail, the transaction is rolled back.
    ///
    /// **Note:** This function does not send notifications. The caller is responsible for
    /// calling `send_job_notification` after successful job scheduling if worker notification
    /// is required.
    #[instrument(skip(self), err)]
    pub async fn schedule_job(
        &self,
        node_id: impl Into<WorkerNodeId<'_>> + std::fmt::Debug,
        job_desc: &str,
        locations: &[LocationId],
    ) -> Result<JobId, Error> {
        let node_id = node_id.into();

        // Use a transaction, such that the job will only be scheduled if the locations are
        // successfully locked.
        let mut tx = self.pool.begin().await?;

        // Register the job in the workers job queue
        let job_id = jobs::insert_with_default_status(&mut *tx, node_id.clone(), job_desc).await?;

        // Lock the locations for this job by assigning the job ID as the writer
        locations::assign_job_writer(&mut *tx, locations, job_id).await?;

        tx.commit().await?;

        Ok(job_id)
    }

    /// Update job status to StopRequested
    ///
    /// This function will only update the job status if it's currently in a valid state
    /// to be stopped (Scheduled or Running). If the job is already stopping, this is
    /// considered success (idempotent behavior). If the job is in a terminal state
    /// (Stopped, Completed, Failed), this returns a conflict error.
    ///
    /// Returns an error if the job doesn't exist, is in a terminal state, or if there's a database error.
    ///
    /// **Note:** This function does not send notifications. The caller is responsible for
    /// calling `send_job_notification` after successful status update if worker notification
    /// is required.
    #[instrument(skip(self), err)]
    pub async fn request_job_stop(
        &self,
        job_id: impl Into<JobId> + std::fmt::Debug,
    ) -> Result<(), Error> {
        let job_id = job_id.into();

        // Try to update job status
        match jobs::update_status_if_any_state(
            &*self.pool,
            job_id,
            &[JobStatus::Running, JobStatus::Scheduled],
            JobStatus::StopRequested,
        )
        .await
        {
            Ok(()) => Ok(()),
            // Check if the job is already stopping (idempotent behavior)
            Err(JobStatusUpdateError::StateConflict {
                actual: JobStatus::StopRequested | JobStatus::Stopping,
                ..
            }) => Ok(()),
            Err(other) => Err(other.into()),
        }
    }

    /// List jobs with cursor-based pagination support
    ///
    /// Uses cursor-based pagination where `last_job_id` is the ID of the last job
    /// from the previous page. For the first page, pass `None` for `last_job_id`.
    pub async fn list_jobs(
        &self,
        limit: i64,
        last_job_id: Option<JobId>,
    ) -> Result<Vec<Job>, Error> {
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
    pub async fn get_scheduled_jobs(
        &self,
        node_id: impl Into<WorkerNodeId<'_>>,
    ) -> Result<Vec<Job>, Error> {
        Ok(jobs::get_by_node_id_and_statuses(
            &*self.pool,
            node_id.into(),
            [JobStatus::Scheduled, JobStatus::Running],
        )
        .await?)
    }

    pub async fn get_jobs_by_dataset(
        &self,
        dataset_name: impl Into<DatasetName<'_>>,
        dataset_version: Option<impl Into<DatasetVersion<'_>>>,
    ) -> Result<Vec<Job>, Error> {
        Ok(jobs::get_jobs_by_dataset(
            &*self.pool,
            dataset_name.into(),
            dataset_version.map(Into::into),
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
    pub async fn get_active_jobs(
        &self,
        node_id: impl Into<WorkerNodeId<'_>>,
    ) -> Result<Vec<Job>, Error> {
        Ok(jobs::get_by_node_id_and_statuses(
            &*self.pool,
            node_id.into(),
            [
                JobStatus::Scheduled,
                JobStatus::Running,
                JobStatus::StopRequested,
            ],
        )
        .await?)
    }

    /// Returns the job with the given ID
    pub async fn get_job(&self, id: impl Into<JobId>) -> Result<Option<Job>, Error> {
        Ok(jobs::get_by_id(&*self.pool, id.into()).await?)
    }

    /// Conditionally marks a job as `RUNNING` only if it's currently `SCHEDULED`
    ///
    /// This provides idempotent behavior - if the job is already running, completed, or failed,
    /// the appropriate error will be returned indicating the state conflict.
    pub async fn mark_job_running(&self, id: impl Into<JobId>) -> Result<(), Error> {
        Ok(jobs::update_status_if_any_state(
            &*self.pool,
            id.into(),
            &[JobStatus::Scheduled],
            JobStatus::Running,
        )
        .await?)
    }

    /// Conditionally marks a job as `STOPPING` only if it's currently `STOP_REQUESTED`
    ///
    /// This is typically used by workers to acknowledge a stop request.
    pub async fn mark_job_stopping(&self, id: impl Into<JobId>) -> Result<(), Error> {
        Ok(jobs::update_status_if_any_state(
            &*self.pool,
            id.into(),
            &[JobStatus::StopRequested],
            JobStatus::Stopping,
        )
        .await?)
    }

    /// Conditionally marks a job as `STOPPED` only if it's currently `STOPPING`
    ///
    /// This provides proper state transition from stopping to stopped.
    pub async fn mark_job_stopped(&self, id: impl Into<JobId>) -> Result<(), Error> {
        Ok(jobs::update_status_if_any_state(
            &*self.pool,
            id.into(),
            &[JobStatus::Stopping],
            JobStatus::Stopped,
        )
        .await?)
    }

    /// Conditionally marks a job as `COMPLETED` only if it's currently `RUNNING`
    ///
    /// This ensures jobs can only be completed from a running state.
    pub async fn mark_job_completed(&self, id: impl Into<JobId>) -> Result<(), Error> {
        Ok(jobs::update_status_if_any_state(
            &*self.pool,
            id.into(),
            &[JobStatus::Running],
            JobStatus::Completed,
        )
        .await?)
    }

    /// Conditionally marks a job as `FAILED` from either `RUNNING` or `SCHEDULED` states
    ///
    /// Jobs can fail from either scheduled (startup failure) or running (runtime failure) states.
    pub async fn mark_job_failed(&self, id: impl Into<JobId>) -> Result<(), Error> {
        Ok(jobs::update_status_if_any_state(
            &*self.pool,
            id.into(),
            &[JobStatus::Scheduled, JobStatus::Running],
            JobStatus::Failed,
        )
        .await?)
    }

    /// Delete a job by ID if it's in a terminal state
    ///
    /// This function will only delete the job if it exists and is in a terminal state
    /// (Completed, Stopped, or Failed). Returns true if a job was deleted, false otherwise.
    pub async fn delete_job_if_terminal(&self, id: impl Into<JobId>) -> Result<bool, Error> {
        Ok(
            jobs::delete_by_id_and_statuses(&*self.pool, id.into(), JobStatus::terminal_statuses())
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

/// Dataset registry API
impl MetadataDb {
    /// Register a new dataset in the registry
    ///
    /// Creates a complete dataset registration including manifest, dataset-manifest link,
    /// and version tag. This operation uses a transaction to ensure atomicity - either all
    /// entities are created or none are.
    ///
    /// The combination of (namespace, name, version) must be unique.
    ///
    /// # Steps performed (in transaction)
    /// 1. Insert manifest (idempotent - no error if hash already exists)
    /// 2. Link manifest to dataset (idempotent - no error if link already exists)
    /// 3. Insert version tag
    #[instrument(skip(self), err)]
    pub async fn register_dataset(
        &self,
        namespace: impl Into<DatasetNamespace<'_>> + std::fmt::Debug,
        name: impl Into<DatasetName<'_>> + std::fmt::Debug,
        version: impl Into<DatasetVersion<'_>> + std::fmt::Debug,
        manifest_hash: impl Into<DatasetHash<'_>> + std::fmt::Debug,
        manifest_path: &str,
    ) -> Result<(), Error> {
        let namespace = namespace.into();
        let name = name.into();
        let version = version.into();
        let manifest_hash = manifest_hash.into();

        let mut tx = self.pool.begin().await?;

        // Step 1: Insert manifest (idempotent)
        datasets::manifest_files::insert(&mut *tx, &manifest_hash, manifest_path).await?;

        // Step 2: Link manifest to dataset (idempotent)
        datasets::manifests::insert(&mut *tx, &namespace, &name, &manifest_hash).await?;

        // Step 3: Insert version tag
        datasets::tags::insert(&mut *tx, &namespace, &name, &version, &manifest_hash).await?;

        tx.commit().await?;

        Ok(())
    }

    /// Check if a dataset exists for the given name and version
    ///
    /// Returns `true` if a dataset with the specified name and version exists in the registry.
    pub async fn dataset_exists(
        &self,
        namespace: impl Into<DatasetNamespace<'_>>,
        name: impl Into<DatasetName<'_>>,
        version: impl Into<DatasetVersion<'_>>,
    ) -> Result<bool, Error> {
        datasets::tags::exists_by_namespace_name_and_version(
            &*self.pool,
            namespace.into(),
            name.into(),
            version.into(),
        )
        .await
        .map_err(Into::into)
    }

    /// Get complete dataset registry information
    ///
    /// Retrieves a dataset record with full details including metadata.
    /// Returns `None` if no dataset is found with the specified name and version.
    pub async fn get_dataset_with_details(
        &self,
        namespace: impl Into<DatasetNamespace<'_>>,
        name: impl Into<DatasetName<'_>>,
        version: impl Into<DatasetVersion<'_>>,
    ) -> Result<Option<DatasetTag>, Error> {
        datasets::tags::get_by_namespace_name_and_version_with_details(
            &*self.pool,
            namespace.into(),
            name.into(),
            version.into(),
        )
        .await
        .map_err(Into::into)
    }

    /// Get the latest version for a dataset
    ///
    /// Finds the most recent version available for the specified dataset name.
    /// Returns the complete dataset details if found, `None` if no dataset exists with that name.
    #[instrument(skip(self), err)]
    pub async fn get_dataset_latest_version_with_details(
        &self,
        namespace: impl Into<DatasetNamespace<'_>> + std::fmt::Debug,
        name: impl Into<DatasetName<'_>> + std::fmt::Debug,
    ) -> Result<Option<DatasetTag>, Error> {
        datasets::tags::get_latest_by_namespace_and_name(&*self.pool, namespace.into(), name.into())
            .await
            .map_err(Into::into)
    }

    /// Stream all datasets from the registry
    ///
    /// Returns a stream of all dataset records with basic information (namespace, name, version),
    /// ordered by dataset name first, then by version.
    pub fn stream_all_datasets(&self) -> impl Stream<Item = Result<DatasetTag, Error>> + '_ {
        datasets::tags::stream(&*self.pool).map(|result| result.map_err(Error::DbError))
    }

    /// List all datasets from the registry
    ///
    /// Returns all dataset records ordered by dataset name ASC and version DESC.
    pub async fn list_datasets(&self) -> Result<Vec<DatasetTag>, Error> {
        Ok(datasets::tags::list_all(&*self.pool).await?)
    }

    /// List all versions for a dataset
    ///
    /// Returns all versions for the specified dataset ordered by version DESC.
    pub async fn list_dataset_versions(
        &self,
        namespace: impl Into<DatasetNamespace<'_>>,
        name: impl Into<DatasetName<'_>>,
    ) -> Result<Vec<DatasetVersionOwned>, Error> {
        Ok(
            datasets::tags::list_by_namespace_and_name(&*self.pool, namespace.into(), name.into())
                .await?,
        )
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

    pub async fn delete_file_ids(&self, file_ids: &[FileId]) -> Result<(), Error> {
        let sql = "
        DELETE FROM file_metadata
         WHERE id = ANY($1);
        ";

        sqlx::query(sql)
            .bind(file_ids.iter().map(|id| **id).collect::<Vec<_>>())
            .execute(&*self.pool)
            .await?;

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
        let interval = PgInterval {
            microseconds: duration.as_micros() as i64,
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
            .bind(file_ids.as_ref())
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
               AND expiration <= CURRENT_TIMESTAMP AT TIME ZONE 'UTC';
        ";

        sqlx::query_as(sql)
            .bind(location_id)
            .fetch(&*self.pool)
            .map_err(Error::DbError)
            .boxed()
    }
}

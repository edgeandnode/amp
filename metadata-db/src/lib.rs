use std::{future::Future, sync::Arc, time::Duration};

use futures::stream::{BoxStream, Stream};
use sqlx::{
    Executor, FromRow, Postgres,
    postgres::{PgListener, PgNotification},
};
use thiserror::Error;
use tokio::time::MissedTickBehavior;
use tracing::instrument;
use url::Url;

mod conn;
#[cfg(feature = "temp-db")]
pub mod temp;
pub mod workers;

use self::conn::{DbConn, DbConnPool};
#[cfg(feature = "temp-db")]
pub use self::temp::{KEEP_TEMP_DIRS, temp_metadata_db};
pub use self::workers::{
    WorkerNodeId,
    events::{JobNotifAction, JobNotifListener, JobNotifRecvError, JobNotification},
    jobs::{Job, JobId, JobStatus, JobStatusUpdateError, JobWithDetails},
};

/// Frequency on which to send a heartbeat.
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);

/// A worker is considered active if it has sent a heartbeat in this period. The scheduler will
/// schedule new jobs only on active workers.
pub const DEFAULT_DEAD_WORKER_INTERVAL: Duration = Duration::from_secs(5);

/// Row ids, always non-negative.
pub type FileId = i64;
pub type LocationId = i64;
pub type JobDatabaseId = i64;
pub type FooterBytes = Vec<u8>;

#[derive(Debug, FromRow)]
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

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Location {
    /// location.id
    pub id: LocationId,
    /// location.dataset
    pub dataset: String,
    /// location.tbl
    pub tbl: String,
    /// location.url
    #[sqlx(try_from = "&'a str")]
    pub url: Url,
    /// location.start_block
    pub start_block: i64,
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

    #[error(
        "Cannot start dump: location has existing start_block={existing}, but requested start_block={requested}"
    )]
    MismatchedStartBlock { existing: i64, requested: i64 },

    #[error("Job status update error: {0}")]
    JobStatusUpdateError(#[from] workers::jobs::JobStatusUpdateError),
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
    pub async fn connect(url: &str) -> Result<Self, Error> {
        let pool = DbConnPool::connect(url).await?;
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

        let job_id = workers::jobs::register_job(&mut *tx, node_id, job_desc).await?;

        lock_locations(&mut *tx, job_id, locations).await?;

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
        match workers::jobs::update_job_status_if_any_state(
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
            Some(job_id) => {
                Ok(workers::jobs::list_jobs_next_page(&*self.pool, limit, job_id).await?)
            }
            None => Ok(workers::jobs::list_jobs_first_page(&*self.pool, limit).await?),
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
        Ok(workers::jobs::get_jobs_for_node_with_statuses(
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
        Ok(workers::jobs::get_jobs_for_node_with_statuses(
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
        Ok(workers::jobs::get_job(&*self.pool, id).await?)
    }

    /// Get a job by ID with full details including timestamps
    pub async fn get_job_with_details(&self, id: &JobId) -> Result<Option<JobWithDetails>, Error> {
        Ok(workers::jobs::get_job_with_details(&*self.pool, id).await?)
    }

    /// Conditionally marks a job as `RUNNING` only if it's currently `SCHEDULED`
    ///
    /// This provides idempotent behavior - if the job is already running, completed, or failed,
    /// the appropriate error will be returned indicating the state conflict.
    pub async fn mark_job_running(&self, id: &JobId) -> Result<(), Error> {
        Ok(workers::jobs::update_job_status_if_any_state(
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
        Ok(workers::jobs::update_job_status_if_any_state(
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
        Ok(workers::jobs::update_job_status_if_any_state(
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
        Ok(workers::jobs::update_job_status_if_any_state(
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
        Ok(workers::jobs::update_job_status_if_any_state(
            &*self.pool,
            id,
            &[JobStatus::Scheduled, JobStatus::Running],
            JobStatus::Failed,
        )
        .await?)
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
        start_block: Option<i64>,
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
            .bind(start_block.unwrap_or(0))
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
        let query = "
            SELECT id
            FROM locations
            WHERE url = $1
            LIMIT 1
        ";

        let location_id: Option<LocationId> = sqlx::query_scalar(query)
            .bind(url.to_string())
            .fetch_optional(&*self.pool)
            .await?;

        Ok(location_id)
    }

    /// Returns the active location. The active location has meaning on both the write and read side:
    /// - On the write side, it is the location that is being kept in sync with the source data.
    /// - On the read side, it is default location that should receive queries.
    #[instrument(skip(self), err)]
    pub async fn get_active_location(
        &self,
        table: TableId<'_>,
    ) -> Result<Option<(Url, LocationId)>, Error> {
        let TableId {
            dataset,
            dataset_version,
            table,
        } = table;
        let dataset_version = dataset_version.unwrap_or("");

        let query = "
        SELECT url, id
        FROM locations
        WHERE dataset = $1 AND dataset_version = $2 AND tbl = $3 AND active
        ";

        let tuples: Vec<(String, LocationId)> = sqlx::query_as(query)
            .bind(dataset)
            .bind(dataset_version)
            .bind(table)
            .fetch_all(&*self.pool)
            .await?;

        let mut urls = tuples
            .into_iter()
            .map(|(url, id)| Ok((Url::parse(&url)?, id)))
            .collect::<Result<Vec<_>, Error>>()?;

        match urls.len() {
            0 => Ok(None),
            1 => Ok(Some(urls.pop().unwrap())),

            // Currently unreachable thanks to DB constraints.
            _ => Err(Error::MultipleActiveLocations(
                dataset.to_string(),
                dataset_version.to_string(),
                table.to_string(),
                urls.iter().map(|(url, _)| url.to_string()).collect(),
            )),
        }
    }

    /// Set a location as the active materialization for a table. If there was a previously active
    /// location, it will be made inactive in the same transaction, achieving an atomic switch.
    #[instrument(skip(self), err)]
    pub async fn set_active_location(
        &self,
        table: TableId<'_>,
        location: &str,
    ) -> Result<(), Error> {
        let TableId {
            dataset,
            dataset_version,
            table,
        } = table;
        let dataset_version = dataset_version.unwrap_or("");

        // Transactionally update the active location.
        let mut tx = self.pool.begin().await?;

        // First, set the existing active location to inactive.
        let query = "
        UPDATE locations
        SET active = false
        WHERE dataset = $1 AND dataset_version = $2 AND tbl = $3 AND active
        ";

        sqlx::query(query)
            .bind(dataset)
            .bind(dataset_version)
            .bind(table)
            .execute(&mut *tx)
            .await?;

        // Then, set the new active location to active.
        let query = "
        UPDATE locations
        SET active = true
        WHERE dataset = $1 AND dataset_version = $2 AND tbl = $3 AND url = $4
        ";

        sqlx::query(query)
            .bind(dataset)
            .bind(dataset_version)
            .bind(table)
            .bind(location)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;

        Ok(())
    }

    /// Returns tuples of `(location_id, table_name, url)`.
    pub async fn output_locations(&self, id: &JobId) -> Result<Vec<Location>, Error> {
        let query = indoc::indoc! {r#"
            SELECT id, dataset, tbl, url
            FROM locations
            WHERE writer = $1
        "#};

        let tuples = sqlx::query_as(query)
            .bind(id)
            .fetch_all(&*self.pool)
            .await?;

        Ok(tuples)
    }

    pub fn stream_file_metadata(
        &self,
        location_id: LocationId,
    ) -> BoxStream<'_, Result<FileMetadataRow, sqlx::Error>> {
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

        sqlx::query_as(query).bind(location_id).fetch(&*self.pool)
    }

    pub async fn insert_metadata(
        &self,
        location_id: i64,
        file_name: String,
        object_size: u64,
        object_e_tag: Option<String>,
        object_version: Option<String>,
        parquet_meta: serde_json::Value,
        footer: Vec<u8>,
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

    /// If the start block is already set, it must match the provided start_block.
    pub async fn check_start_block(
        &self,
        location_id: LocationId,
        start_block: i64,
    ) -> Result<(), Error> {
        let existing_start_block: i64 =
            sqlx::query_scalar("SELECT start_block FROM locations WHERE id = $1")
                .bind(location_id)
                .fetch_one(&*self.pool)
                .await?;

        if existing_start_block != start_block {
            return Err(Error::MismatchedStartBlock {
                existing: existing_start_block,
                requested: start_block,
            });
        }

        Ok(())
    }

    pub async fn get_footer_bytes(
        &self,
        location_id: LocationId,
        file_name: String,
    ) -> Result<Vec<u8>, Error> {
        let sql = "
        SELECT footer
          FROM file_metadata
         WHERE location_id = $1
               AND file_name = $2
      ORDER BY id DESC LIMIT 1
        ";

        Ok(sqlx::query_scalar(sql)
            .bind(location_id)
            .bind(file_name)
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

#[instrument(skip(executor), err)]
async fn lock_locations(
    executor: impl Executor<'_, Database = Postgres>,
    job_id: JobId,
    locations: &[LocationId],
) -> Result<(), Error> {
    let query = "UPDATE locations SET writer = $1 WHERE id = ANY($2)";
    sqlx::query(query)
        .bind(job_id)
        .bind(locations)
        .execute(executor)
        .await?;
    Ok(())
}

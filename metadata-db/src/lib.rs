use std::{
    sync::{Arc, LazyLock},
    time::Duration,
};

use futures::stream::{BoxStream, Stream};
use pgtemp::{PgTempDB, PgTempDBBuilder};
use sqlx::{
    postgres::{PgListener, PgNotification},
    Executor, Postgres,
};
use thiserror::Error;
use tokio::time::MissedTickBehavior;
use tracing::instrument;
use url::Url;

mod conn;
pub mod workers;

use self::conn::{DbConn, DbConnPool};
pub use self::workers::{
    events::{JobNotifAction, JobNotifListener, JobNotification},
    jobs::{Job, JobId, JobStatus},
    WorkerNodeId,
};

/// Frequency on which to send a heartbeat.
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);

/// A worker is considered active if it has sent a heartbeat in this period. The scheduler will
/// schedule new jobs only on active workers.
pub const DEFAULT_DEAD_WORKER_INTERVAL: Duration = Duration::from_secs(5);

pub static ALLOW_TEMP_DB: LazyLock<bool> = LazyLock::new(|| {
    std::env::var("ALLOW_TEMP_DB")
        .map(|v| v.to_lowercase() == "true" || v == "1")
        .unwrap_or(true)
});

pub static KEEP_TEMP_DIRS: LazyLock<bool> =
    LazyLock::new(|| std::env::var("KEEP_TEMP_DIRS").is_ok());

/// Row ids, always non-negative.
pub type LocationId = i64;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Error connecting to metadata db: {0}")]
    ConnectionError(sqlx::Error),

    #[error("{0}")]
    MigrationError(#[from] sqlx::migrate::MigrateError),

    #[error("Metadata db error: {0}")]
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
}

impl From<conn::ConnError> for Error {
    fn from(err: conn::ConnError) -> Self {
        match err {
            conn::ConnError::ConnectionError(e) => Error::ConnectionError(e),
            conn::ConnError::MigrationFailed(e) => Error::MigrationError(e),
        }
    }
}

/// Connection pool to the metadata DB. Clones will refer to the same instance.
#[derive(Clone, Debug)]
pub struct MetadataDb {
    pub(crate) pool: DbConnPool,
    pub(crate) url: Arc<str>,
    dead_worker_interval: Duration,
    // Handle to keep the DB alive.
    _temp_db: Option<Arc<PgTempDB>>,
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
            _temp_db: None,
        })
    }

    #[instrument(skip_all, err)]
    pub async fn temporary(keep: bool) -> Result<Self, Error> {
        // Set C locale
        std::env::set_var("LANG", "C");

        let builder = PgTempDBBuilder::new().persist_data(keep);
        let temp_db = PgTempDB::from_builder(builder);
        tracing::info!(
            "initializing temp metadata db at: {}",
            temp_db.data_dir().display()
        );
        let url = temp_db.connection_uri();
        tracing::info!("connecting to metadata db at: {}", url);

        let pool = DbConnPool::connect(&url).await?;
        pool.run_migrations().await?;
        Ok(Self {
            pool,
            url: url.into(),
            dead_worker_interval: DEFAULT_DEAD_WORKER_INTERVAL,
            _temp_db: Some(Arc::new(temp_db)),
        })
    }

    /// Configures the "dead worker" interval for the metadata DB instance
    pub fn with_dead_worker_interval(self, dead_worker_interval: Duration) -> Self {
        Self {
            pool: self.pool,
            url: self.url,
            dead_worker_interval,
            _temp_db: self._temp_db,
        }
    }

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
            INSERT INTO locations (dataset, dataset_version, tbl, bucket, path, url, active)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
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

    pub async fn hello_worker(&self, node_id: &WorkerNodeId) -> Result<(), Error> {
        workers::heartbeat::register_worker(&*self.pool, node_id).await?;
        Ok(())
    }

    /// Periodically updates the worker's heartbeat in a dedicated DB connection.
    ///
    /// Loops forever unless the DB returns an error.
    pub async fn heartbeat_loop(&self, node_id: &WorkerNodeId) -> Result<(), Error> {
        let mut conn = DbConn::connect(&self.url).await?;

        let mut interval = tokio::time::interval(HEARTBEAT_INTERVAL);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            workers::heartbeat::update_heartbeat(&mut *conn, node_id).await?;
        }
    }

    /// Returns a list of active workers.
    ///
    /// A worker is active if it has sent a sufficiently recent heartbeat.
    pub async fn active_workers(&self) -> Result<Vec<WorkerNodeId>, Error> {
        Ok(workers::heartbeat::get_active_workers(&*self.pool, self.dead_worker_interval).await?)
    }

    /// Schedules a job on the given worker
    ///
    /// The job will only be scheduled if the locations are successfully locked.
    ///
    /// This function performs in a single transaction:
    ///  1. Registers the job in the workers job queue
    ///  2. Locks the locations
    ///  3. Sends a notification to the worker
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

    /// Given a worker `node_id`, returns all the job IDs
    pub async fn scheduled_jobs(&self, node_id: &WorkerNodeId) -> Result<Vec<JobId>, Error> {
        Ok(workers::jobs::get_job_ids_for_node(&*self.pool, node_id).await?)
    }

    /// Returns the job with the given ID
    pub async fn get_job(&self, id: &JobId) -> Result<Option<Job>, Error> {
        Ok(workers::jobs::get_job(&*self.pool, id).await?)
    }

    /// For a given job ID, returns the job descriptor.
    pub async fn job_desc(&self, id: &JobId) -> Result<Option<String>, Error> {
        Ok(workers::jobs::get_job_descriptor(&*self.pool, id).await?)
    }

    /// Send a notification over the worker actions notification channel
    pub async fn send_job_notification(&self, payload: JobNotification) -> Result<(), Error> {
        workers::events::notify(&*self.pool, payload)
            .await
            .map_err(Into::into)
    }

    /// Listen to the worker actions notification channel for job notifications
    pub async fn listen_for_job_notifications(&self) -> Result<JobNotifListener, Error> {
        workers::events::listen_url(&*self.url)
            .await
            .map_err(Into::into)
    }

    /// Returns tuples of `(location_id, table_name, url)`.
    pub async fn output_locations(
        &self,
        id: &JobId,
    ) -> Result<Vec<(LocationId, String, Url)>, Error> {
        let query = indoc::indoc! {r#"
            SELECT id, tbl, url
            FROM locations
            WHERE writer = $1
        "#};

        let tuples: Vec<(LocationId, String, String)> = sqlx::query_as(query)
            .bind(id)
            .fetch_all(&*self.pool)
            .await?;

        let urls = tuples
            .into_iter()
            .map(|(id, tbl, url)| Ok((id, tbl, Url::parse(&url)?)))
            .collect::<Result<Vec<_>, Error>>()?;

        Ok(urls)
    }

    #[instrument(skip(self), err)]
    pub async fn notify(&self, channel_name: &str, payload: &str) -> Result<(), Error> {
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(channel_name)
            .bind(payload)
            .execute(&*self.pool)
            .await?;
        Ok(())
    }

    /// Listens on a PostgreSQL notification channel using LISTEN.
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
    ) -> Result<impl Stream<Item = Result<PgNotification, sqlx::Error>>, sqlx::Error> {
        let mut channel = PgListener::connect(&self.url).await?;
        channel.listen(channel_name).await?;
        Ok(channel.into_stream())
    }

    /// Produces a stream of all names of files for a given tbl catalogued by the MetadataDb
    pub fn stream_file_names<'a>(
        &'a self,
        location_id: i64,
    ) -> BoxStream<'a, Result<String, sqlx::Error>> {
        let sql = "
            SELECT sr.file_name
              FROM file_metadata sr
             WHERE sr.location_id = $1
          ORDER BY 1 ASC
        ";

        sqlx::query_scalar(sql).bind(location_id).fetch(&*self.pool)
    }

    #[instrument(skip(self))]
    /// Produces a stream of all block ranges for a given tbl catalogued by the MetadataDb
    pub fn stream_ranges<'a>(
        &'a self,
        location_id: i64,
    ) -> BoxStream<'a, Result<(i64, i64), sqlx::Error>> {
        let sql = "
            SELECT CAST(sr.metadata->>'range_start' AS BIGINT)
                 , CAST(sr.metadata->>'range_end' AS BIGINT)
              FROM file_metadata sr
             WHERE sr.location_id = $1
          ORDER BY 1 ASC
        ";

        sqlx::query_as(sql).bind(location_id).fetch(&*self.pool)
    }

    pub async fn insert_metadata(
        &self,
        location_id: i64,
        file_name: String,
        metadata: serde_json::Value,
    ) -> Result<(), Error> {
        let sql = "
        INSERT INTO file_metadata (location_id, file_name, metadata)
        VALUES ($1, $2, $3)
        ON CONFLICT DO NOTHING
        ";

        sqlx::query(sql)
            .bind(location_id)
            .bind(file_name)
            .bind(metadata)
            .execute(&*self.pool)
            .await?;

        Ok(())
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

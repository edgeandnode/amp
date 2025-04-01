use std::time::Duration;

use futures::stream::{BoxStream, Stream};
use log::error;
use sqlx::{
    migrate::{MigrateError, Migrator},
    postgres::{PgListener, PgNotification},
    Connection as _, Executor, PgConnection, Pool, Postgres,
};
use thiserror::Error;
use tokio::time::MissedTickBehavior;
use tracing::instrument;
use url::Url;

/// Frequency on which to send a heartbeat.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);

/// A worker is considered active if it has sent a heartbeat in this period. The scheduler will
/// schedule new operators only on active workers.
const ACTIVE_INTERVAL_SECS: i32 = 5;

/// Row ids, always non-negative.
pub type LocationId = i64;
pub type OperatorDatabaseId = i64;

#[derive(Error, Debug)]
pub enum Error {
    #[error("{0}")]
    MigrationError(#[from] MigrateError),

    #[error("Error connecting to metadata db: {0}")]
    ConnectionError(sqlx::Error),

    #[error("Metadata db error: {0}")]
    DbError(#[from] sqlx::Error),

    #[error(
        "Multiple active locations found for dataset={0}, dataset_version={1}, table={2}: {3:?}"
    )]
    MultipleActiveLocations(String, String, String, Vec<String>),

    #[error("Error parsing URL: {0}")]
    UrlParseError(#[from] url::ParseError),
}

static MIGRATOR: Migrator = sqlx::migrate!();

/// Connection pool to the metadata DB. Clones will refer to the same instance.
#[derive(Clone)]
pub struct MetadataDb {
    pool: Pool<Postgres>,
    url: String,
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
    /// Sets up a connection pool to the metadata DB. Runs migrations if necessary.
    #[instrument(skip_all, err)]
    pub async fn connect(url: &str) -> Result<MetadataDb, Error> {
        let pool = Pool::connect(url).await.map_err(Error::ConnectionError)?;
        let db = MetadataDb {
            pool,
            url: url.to_string(),
        };
        db.run_migrations().await?;
        Ok(db)
    }

    /// sqlx does the right things:
    /// - Locks the DB before running migrations.
    /// - Never runs the same migration twice.
    /// - Errors on changes to old migrations.
    #[instrument(skip(self), err)]
    async fn run_migrations(&self) -> Result<(), Error> {
        Ok(MIGRATOR.run(&self.pool).await?)
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

        let query = "
        INSERT INTO locations (dataset, dataset_version, tbl, bucket, path, url, active)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING id;
        ";

        let location_id: LocationId = sqlx::query_scalar(query)
            .bind(table.dataset)
            .bind(dataset_version)
            .bind(table.table)
            .bind(bucket)
            .bind(path)
            .bind(url.to_string())
            .bind(active)
            .fetch_one(&self.pool)
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
            .fetch_all(&self.pool)
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

    pub async fn hello_worker(&self, node_id: &str) -> Result<(), Error> {
        let query = "
        INSERT INTO workers (node_id, last_heartbeat)
        VALUES ($1, now() at time zone 'utc')
        ON CONFLICT (node_id) DO UPDATE SET last_heartbeat = (now() at time zone 'utc')
        ";

        sqlx::query(query).bind(node_id).execute(&self.pool).await?;

        Ok(())
    }

    /// Periodically updates the worker's heartbeat in a dedicated DB connection.
    ///
    /// Loops forever unless the DB returns an error.
    pub async fn heartbeat_loop(self, node_id: String) -> Result<(), Error> {
        let mut conn = PgConnection::connect(&self.url).await?;
        let mut interval = tokio::time::interval(HEARTBEAT_INTERVAL);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            let query =
                "UPDATE workers SET last_heartbeat = (now() at time zone 'utc') WHERE node_id = $1";
            sqlx::query(query).bind(&node_id).execute(&mut conn).await?;
        }
    }

    /// Returns a list of active workers. A worker is active if it has sent a sufficiently recent heartbeat.
    pub async fn active_workers(&self) -> Result<Vec<String>, Error> {
        let query = "SELECT node_id FROM workers WHERE last_heartbeat > (now() at time zone 'utc') - make_interval(secs => $1)";
        Ok(sqlx::query_scalar(query)
            .bind(ACTIVE_INTERVAL_SECS)
            .fetch_all(&self.pool)
            .await?)
    }

    #[instrument(skip(self), err)]
    pub async fn schedule_operator(
        &self,
        node_id: &str,
        operator_desc: &str,
        locations: &[LocationId],
    ) -> Result<OperatorDatabaseId, Error> {
        // Use a transaction, such that the operator will only be scheduled if the locations are
        // successfully locked.
        let mut tx = self.pool.begin().await?;

        let query =
            "INSERT INTO operators (node_id, descriptor) VALUES ($1, $2::jsonb) RETURNING id";
        let id: OperatorDatabaseId = sqlx::query_scalar(query)
            .bind(node_id)
            .bind(operator_desc)
            .fetch_one(&self.pool)
            .await?;

        lock_locations(&mut *tx, id, locations).await?;

        tx.commit().await?;

        Ok(id)
    }

    pub async fn scheduled_operators(
        &self,
        node_id: &str,
    ) -> Result<Vec<OperatorDatabaseId>, Error> {
        let query = "SELECT id FROM operators WHERE node_id = $1";
        Ok(sqlx::query_scalar(query)
            .bind(node_id)
            .fetch_all(&self.pool)
            .await?)
    }

    pub async fn operator_desc(&self, operator_id: OperatorDatabaseId) -> Result<String, Error> {
        let query = "SELECT descriptor::text FROM operators WHERE id = $1";
        Ok(sqlx::query_scalar(query)
            .bind(operator_id)
            .fetch_one(&self.pool)
            .await?)
    }

    /// Returns tuples of `(location_id, table_name, url)`.
    pub async fn output_locations(
        &self,
        operator_id: OperatorDatabaseId,
    ) -> Result<Vec<(LocationId, String, Url)>, Error> {
        let query = "SELECT id, tbl, url FROM locations WHERE writer = $1";
        let tuples: Vec<(LocationId, String, String)> = sqlx::query_as(query)
            .bind(operator_id)
            .fetch_all(&self.pool)
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
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Listens on a Postgres notification channel using LISTEN.
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
        tbl: TableId<'a>,
    ) -> BoxStream<'a, Result<String, sqlx::Error>> {
        let TableId {
            dataset,
            dataset_version,
            table,
        } = tbl;
        let sql = "
            SELECT DISTINCT sr.file_name
              FROM scanned_ranges sr
        INNER JOIN locations l
                ON sr.location_id = l.vid
             WHERE l.dataset = $1 
                   AND l.dataset_version = $2
                   AND l.tbl = $3
                   AND l.active
          ORDER BY 1 ASC
        ";

        sqlx::query_scalar(sql)
            .bind(dataset)
            .bind(dataset_version.unwrap_or_default())
            .bind(table.to_string())
            .fetch(&self.pool)
    }

    /// Produces a stream of all scanned block ranges for a given tbl catalogued by the MetadataDb
    pub fn stream_ranges<'a>(
        &'a self,
        tbl: TableId<'a>,
    ) -> BoxStream<'a, Result<(i64, i64), sqlx::Error>> {
        let TableId {
            dataset,
            dataset_version,
            table,
        } = tbl;
        let sql = "
            SELECT DISTINCT sr.metadata->range_start
                 , sr.metadata->range_end 
              FROM scanned_ranges sr 
        INNER JOIN locations l 
                ON sr.location_id = l.vid 
             WHERE l.dataset = $1 
                   AND l.dataset_version = $2
                   AND l.tbl = $3
                   AND l.active
          ORDER BY 1 ASC
        ";

        sqlx::query_scalar(sql)
            .bind(dataset)
            .bind(dataset_version.unwrap_or_default())
            .bind(table.to_string())
            .fetch(&self.pool)
    }

    pub async fn insert_scanned_range(
        &self,
        location_id: i64,
        file_name: String,
        scanned_range: serde_json::Value,
    ) -> Result<(), Error> {
        let mut tx = self.pool.begin().await?;

        let sql = "
        INSERT INTO scanned_ranges (location_id, file_name, metadata)
        VALUES ($1, $2, $3)
        RETURNING id;
        ";

        let _id = sqlx::query(sql)
            .bind(location_id)
            .bind(file_name)
            .bind(scanned_range)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;

        Ok(())
    }
}

#[instrument(skip(executor), err)]
async fn lock_locations(
    executor: impl Executor<'_, Database = Postgres>,
    operator_id: OperatorDatabaseId,
    locations: &[LocationId],
) -> Result<(), Error> {
    let query = "UPDATE locations SET writer = $1 WHERE id = ANY($2)";
    sqlx::query(query)
        .bind(operator_id)
        .bind(locations)
        .execute(executor)
        .await?;
    Ok(())
}

use std::fmt;

use serde::{Deserialize, Serialize};
use sqlx::{
    migrate::{MigrateError, Migrator},
    Pool, Postgres,
};
use thiserror::Error;
use tracing::instrument;
use url::Url;

/// Always non-negative.
pub type LocationId = i64;

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
}

static MIGRATOR: Migrator = sqlx::migrate!();

#[derive(Clone)]
pub struct MetadataDb {
    pool: Pool<Postgres>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum JobState {
    Created,
    Running,
}

impl fmt::Display for JobState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Serialize, Deserialize)]
pub struct WorkerAction {
    pub node_id: String,
    pub location: LocationId,
    pub current_state: JobState,
    pub next_state: JobState,
}

/// Tables are identified by the triple: `(dataset, dataset_version, table)`. For each table, there
/// is at most one active location.
#[derive(Debug, Copy, Clone)]
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
        let db = MetadataDb { pool };
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
        INSERT INTO locations (dataset, dataset_version, tbl, bucket, path, url, active) \
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
    pub async fn get_active_location(&self, table: TableId<'_>) -> Result<Option<String>, Error> {
        let TableId {
            dataset,
            dataset_version,
            table,
        } = table;
        let dataset_version = dataset_version.unwrap_or("");

        let query = "
        SELECT url
        FROM locations
        WHERE dataset = $1 AND dataset_version = $2 AND tbl = $3 AND active
        ";

        let mut urls: Vec<String> = sqlx::query_scalar(query)
            .bind(dataset)
            .bind(dataset_version)
            .bind(table)
            .fetch_all(&self.pool)
            .await?;

        match urls.len() {
            0 => Ok(None),
            1 => Ok(Some(urls.pop().unwrap())),

            // Currently unreachable thanks to DB constraints.
            _ => Err(Error::MultipleActiveLocations(
                dataset.to_string(),
                dataset_version.to_string(),
                table.to_string(),
                urls,
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

    pub async fn heartbeat(&self, node_id: &str) -> Result<(), Error> {
        let query = "
        INSERT INTO workers (node_id, last_heartbeat)
        VALUES ($1, now() at time zone 'utc')
        ON CONFLICT (node_id) DO UPDATE SET last_heartbeat = (now() at time zone 'utc')
        ";

        sqlx::query(query).bind(node_id).execute(&self.pool).await?;

        Ok(())
    }

    /// Returns a list of live workers. A worker is live if it has sent a heartbeat in the last 5 seconds.
    pub async fn live_workers(&self) -> Result<Vec<String>, Error> {
        let query = "SELECT node_id FROM workers WHERE last_heartbeat > (now() at time zone 'utc') - interval '5 seconds'";
        Ok(sqlx::query_scalar(query).fetch_all(&self.pool).await?)
    }

    pub async fn create_job(&self, node_id: &str, location: LocationId) -> Result<(), Error> {
        let initial_state = JobState::Created;
        let query = "INSERT INTO jobs (node_id, location, state) VALUES ($1, $2, $3)";
        sqlx::query(query)
            .bind(node_id)
            .bind(location)
            .bind(initial_state.to_string())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn notify_worker(&self, action: WorkerAction) -> Result<(), Error> {
        sqlx::query("NOTIFY worker_actions, $1")
            .bind(serde_json::to_string(&action).unwrap())
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

use sqlx::{
    migrate::{MigrateError, Migrator},
    Pool, Postgres,
};
use thiserror::Error;
use url::Url;

#[derive(Error, Debug)]
pub enum Error {
    #[error("{0}")]
    MigrationError(#[from] MigrateError),

    #[error("Error connecting to metadata db: {0}")]
    ConnectionError(sqlx::Error),

    #[error("Metadata db error: {0}")]
    DbError(#[from] sqlx::Error),

    #[error(
        "Multiple active locations found for dataset={0}, dataset_version={1}, view={2}: {3:?}"
    )]
    MultipleActiveLocations(String, String, String, Vec<String>),
}

static MIGRATOR: Migrator = sqlx::migrate!();

#[derive(Clone)]
pub struct MetadataDb {
    pool: Pool<Postgres>,
}

/// Materialized views are commonly looked up by the view being materialized. Views are identified by
/// the triple: `(dataset, dataset_version, view)`. For each view, there is at most one active
/// materialized location.
#[derive(Debug, Copy, Clone)]
pub struct ViewId<'a> {
    pub dataset: &'a str,
    pub dataset_version: Option<&'a str>,
    pub view: &'a str,
}

impl MetadataDb {
    /// Sets up a connection pool to the metadata DB. Runs migrations if necessary.
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
    async fn run_migrations(&self) -> Result<(), Error> {
        Ok(MIGRATOR.run(&self.pool).await?)
    }

    /// Register a materialized view into the metadata database.
    ///
    /// If setting `active = true`, make sure no other active location exists for this view, to avoid
    /// a constraint violation. If an active location might exist, it is better to initialize the
    /// location with `active = false` and then call `set_active_location` to switch it as active.
    pub async fn register_location(
        &self,
        view: ViewId<'_>,
        bucket: Option<&str>,
        path: &str,
        url: &Url,
        active: bool,
    ) -> Result<(), sqlx::Error> {
        // An empty `dataset_version` is represented as an empty string in the DB.
        let dataset_version = view.dataset_version.unwrap_or("");

        let query = "
        INSERT INTO locations (dataset, dataset_version, view, bucket, path, url, active) \
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ";

        sqlx::query(query)
            .bind(view.dataset)
            .bind(dataset_version)
            .bind(view.view)
            .bind(bucket)
            .bind(path)
            .bind(url.to_string())
            .bind(active)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Returns the active location. The active location has meaning on both the write and read side:
    /// - On the write side, it is the location that is being kept in sync with the source data.
    /// - On the read side, it is default location that should receive queries.
    pub async fn get_active_location(&self, view: ViewId<'_>) -> Result<Option<String>, Error> {
        let ViewId {
            dataset,
            dataset_version,
            view,
        } = view;
        let dataset_version = dataset_version.unwrap_or("");

        let query = "
        SELECT url
        FROM locations
        WHERE dataset = $1 AND dataset_version = $2 AND view = $3 AND active
        ";

        let mut urls: Vec<String> = sqlx::query_scalar(query)
            .bind(dataset)
            .bind(dataset_version)
            .bind(view)
            .fetch_all(&self.pool)
            .await?;

        match urls.len() {
            0 => Ok(None),
            1 => Ok(Some(urls.pop().unwrap())),

            // Currently unreachable thanks to DB constraints.
            _ => Err(Error::MultipleActiveLocations(
                dataset.to_string(),
                dataset_version.to_string(),
                view.to_string(),
                urls,
            )),
        }
    }

    /// Set a location as the active materialization for a view. If there was a previously active
    /// location, it will be made inactive in the same transaction, achieving an atomic switch.
    pub async fn set_active_location(&self, view: ViewId<'_>, location: &str) -> Result<(), Error> {
        let ViewId {
            dataset,
            dataset_version,
            view,
        } = view;
        let dataset_version = dataset_version.unwrap_or("");

        // Transactionally update the active location.
        let mut tx = self.pool.begin().await?;

        // First, set the existing active location to inactive.
        let query = "
        UPDATE locations
        SET active = false
        WHERE dataset = $1 AND dataset_version = $2 AND view = $3 AND active
        ";

        sqlx::query(query)
            .bind(dataset)
            .bind(dataset_version)
            .bind(view)
            .execute(&mut *tx)
            .await?;

        // Then, set the new active location to active.
        let query = "
        UPDATE locations
        SET active = true
        WHERE dataset = $1 AND dataset_version = $2 AND view = $3 AND url = $4
        ";

        sqlx::query(query)
            .bind(dataset)
            .bind(dataset_version)
            .bind(view)
            .bind(location)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;

        Ok(())
    }
}

use std::time::Duration;

use futures::{
    stream::{BoxStream, Stream},
    StreamExt, TryStreamExt,
};
use log::error;
use object_store::{path::Path, ObjectMeta};
use serde::{Deserialize, Serialize};
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
#[derive(Clone, Debug)]
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
            SELECT fm.file_name
              FROM file_metadata fm
        INNER JOIN locations l
                ON fm.location_id = l.id
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

    pub fn stream_ranges<'a>(
        &'a self,
        tbl: TableId<'a>,
    ) -> BoxStream<'a, Result<(i64, i64), sqlx::Error>> {
        let TableId {
            dataset,
            dataset_version: _,
            table,
        } = tbl;
        let sql = "
            SELECT fm.range_start, fm.range_end
              FROM file_metadata fm
        INNER JOIN locations l 
                ON fm.location_id = l.id 
             WHERE l.dataset = $1
                   AND l.tbl = $2
                   AND l.active
          ORDER BY 1 ASC
        ";

        sqlx::query_as(sql)
            .bind(dataset)
            .bind(table.to_string())
            .fetch(&self.pool)
    }

    /// Produces a stream of object metadata for a given table catalogued by the MetadataDb
    pub fn stream_object_meta<'a>(
        &'a self,
        tbl: TableId<'a>,
    ) -> BoxStream<'a, Result<(String, object_store::ObjectMeta), sqlx::Error>> {
        let TableId {
            dataset,
            dataset_version: _,
            table,
        } = tbl;
        let sql = "
            SELECT l.url, fm.file_name, fm.file_size, fm.e_tag, fm.version, fm.last_modified
              FROM file_metadata fm
        INNER JOIN locations l 
                ON fm.location_id = l.id 
             WHERE l.dataset = $1
                   AND l.tbl = $2
                   AND l.active
          ORDER BY 1 ASC
        ";

        sqlx::query_as(sql)
            .bind(dataset)
            .bind(table)
            .fetch(&self.pool)
            .try_filter_map(
                |(url, file_name, file_size, etag, version, last_modified): (
                    String,
                    String,
                    i64,
                    String,
                    String,
                    NozzleTimestamp,
                )| async move {
                    // Unwrap: we know the URL is valid because it was inserted by us.
                    // Unwrap: we know the file name is valid because it was inserted by us.
                    let url = Url::parse(&url).unwrap().join(&file_name).unwrap();
                    let location = Path::from_url_path(url.path()).unwrap();
                    let size = file_size as usize;
                    let last_modified = last_modified.into();
                    let e_tag = if etag.is_empty() { None } else { Some(etag) };
                    let version = if version.is_empty() {
                        None
                    } else {
                        Some(version)
                    };
                    Ok(Some((
                        file_name,
                        object_store::ObjectMeta {
                            location,
                            last_modified,
                            size,
                            e_tag,
                            version,
                        },
                    )))
                },
            )
            .boxed()
    }

    /// Produces a stream of nozzle metadata for a given table catalogued by the MetadataDb
    ///
    /// Stream items are tuples of:
    /// - ObjectMeta
    /// - Range start and end
    /// - Data size
    /// - Size hint
    pub fn stream_nozzle_metadata<'a>(
        &'a self,
        tbl: TableId<'a>,
    ) -> BoxStream<'a, Result<FileMetadata, sqlx::Error>> {
        let TableId { dataset, table, .. } = tbl;
        let sql = "
            SELECT l.url
                 , fm.file_name
                 , fm.file_size
                 , fm.e_tag
                 , fm.version
                 , fm.last_modified
                 , fm.range_start
                 , fm.range_end
                 , fm.row_count
                 , fm.data_size
                 , fm.size_hint
              FROM file_metadata fm
        INNER JOIN locations l 
                ON fm.location_id = l.id
             WHERE l.dataset = $1
                   AND l.tbl = $2
                   AND l.active";

        sqlx::query_as::<_, FileMetadataBuilder>(sql)
            .bind(dataset)
            .bind(table)
            .fetch(&self.pool)
            .map_ok(|builder| builder.build())
            .boxed()
    }

    /// Inserts a new file metadata entry into the database.
    pub async fn insert_nozzle_metadata(
        &self,
        FileMetadata {
            object_meta,
            range: (range_start, range_end),
            row_count,
            data_size,
            size_hint,
        }: FileMetadata,
        location_id: i64,
        file_name: String,
        created_at: (i64, i32),
    ) -> Result<(), Error> {
        let sql = "
        INSERT INTO file_metadata (
            location_id, file_name, range_start, range_end, row_count, file_size, data_size, size_hint,
            e_tag, version, created_at, last_modified
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NULL)
        ";

        let created_at = NozzleTimestamp::from(created_at);
        let file_size = object_meta.size as i64;
        let e_tag = object_meta.e_tag;
        let version = object_meta.version;

        sqlx::query(sql)
            .bind(location_id)
            .bind(file_name)
            .bind(range_start)
            .bind(range_end)
            .bind(row_count)
            .bind(file_size)
            .bind(data_size)
            .bind(size_hint)
            .bind(e_tag.unwrap_or_default())
            .bind(version.unwrap_or_default())
            .bind(created_at)
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}

impl AsRef<MetadataDb> for MetadataDb {
    fn as_ref(&self) -> &MetadataDb {
        self
    }
}

#[derive(Clone, Debug, sqlx::FromRow)]
struct FileMetadataBuilder {
    url: String,
    file_name: String,
    file_size: i64,
    e_tag: String,
    version: String,
    last_modified: NozzleTimestamp,
    range_start: i64,
    range_end: i64,
    row_count: Option<i64>,
    data_size: Option<i64>,
    size_hint: Option<i64>,
}

impl FileMetadataBuilder {
    fn build(self) -> FileMetadata {
        // Unwrap: we know the URL is valid because it was inserted by us.
        // Unwrap: we know the file name is valid because it was inserted by us.
        let url = Url::parse(&self.url)
            .unwrap()
            .join(&self.file_name)
            .unwrap();
        let location = Path::from_url_path(url.path()).unwrap();
        let size = self.file_size as usize;
        let last_modified = self.last_modified.into();
        let e_tag = if self.e_tag.is_empty() {
            None
        } else {
            Some(self.e_tag)
        };
        let version = if self.version.is_empty() {
            None
        } else {
            Some(self.version)
        };

        FileMetadata {
            object_meta: ObjectMeta {
                location,
                last_modified,
                size,
                e_tag,
                version,
            },
            range: (self.range_start, self.range_end),
            row_count: self.row_count,
            data_size: self.data_size,
            size_hint: self.size_hint,
        }
    }
}

#[derive(Clone, Debug)]
pub struct FileMetadata {
    pub object_meta: ObjectMeta,
    pub range: (i64, i64),
    pub row_count: Option<i64>,
    pub data_size: Option<i64>,
    pub size_hint: Option<i64>,
}

#[derive(Debug, Deserialize, Serialize, sqlx::Decode, sqlx::Encode, sqlx::FromRow)]
pub struct Block<const N: usize>
where
    for<'d> [u8; N]: Deserialize<'d> + Serialize,
{
    pub number: i64,
    pub timestamp: NozzleTimestamp,
    pub hash: [u8; N],
}

impl sqlx::Type<sqlx::Postgres> for Block<32> {
    fn type_info() -> <Postgres as sqlx::Database>::TypeInfo {
        sqlx::postgres::PgTypeInfo::with_name("BLOCK_32")
    }
}

#[derive(Debug, Deserialize, Serialize, sqlx::Decode, sqlx::Encode, sqlx::FromRow)]
pub struct Network {
    kind: String,
    #[sqlx(default)]
    caip2: Option<String>,
}

impl sqlx::Type<sqlx::Postgres> for Network {
    fn type_info() -> <Postgres as sqlx::Database>::TypeInfo {
        sqlx::postgres::PgTypeInfo::with_name("NETWORK")
    }
}

#[derive(Debug, Deserialize, Serialize, sqlx::Decode, sqlx::Encode, sqlx::FromRow)]
pub struct BlockRange<const N: usize>
where
    for<'d> [u8; N]: Deserialize<'d> + Serialize,
    Block<N>: sqlx::Type<sqlx::Postgres>,
{
    pub start: Block<N>,
    pub end: Block<N>,
    pub network: Network,
}

#[derive(Debug, Deserialize, Serialize, sqlx::FromRow)]
pub struct FileMetaRow {}

#[derive(Debug, Deserialize, Serialize, sqlx::FromRow)]
pub struct ObjectMetaRow {
    pub location: String,
    pub last_modified: NozzleTimestamp,
    pub size: i64,
    pub e_tag: Option<String>,
    pub version: Option<String>,
}

impl From<FileMetadataBuilder> for FileMetadata {
    fn from(builder: FileMetadataBuilder) -> Self {
        builder.build()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, sqlx::Type)]
#[sqlx(type_name = "NOZZLE_TIMESTAMP")]
pub struct NozzleTimestamp {
    pub secs: i64,
    pub nanos: i32,
}

impl From<(i64, i32)> for NozzleTimestamp {
    fn from((secs, nanos): (i64, i32)) -> Self {
        NozzleTimestamp { secs, nanos }
    }
}

impl From<NozzleTimestamp> for (i64, i32) {
    fn from(ts: NozzleTimestamp) -> Self {
        (ts.secs, ts.nanos)
    }
}

impl From<NozzleTimestamp> for chrono::DateTime<chrono::Utc> {
    fn from(ts: NozzleTimestamp) -> Self {
        let timestamp_nanos = ts.secs * 1_000_000_000 + ts.nanos as i64;
        Self::from_timestamp_nanos(timestamp_nanos)
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

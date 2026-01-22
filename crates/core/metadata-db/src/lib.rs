use std::{sync::Arc, time::Duration};

use futures::{future::BoxFuture, stream::BoxStream};
use sqlx::Connection as _;
use tracing::instrument;

pub mod datasets;
mod db;
mod error;
pub mod files;
pub mod gc;
pub mod job_attempts;
pub mod jobs;
pub mod manifests;
pub mod notification_multiplexer;
pub mod physical_table;
pub mod workers;

pub use self::{
    datasets::{
        DatasetName, DatasetNameOwned, DatasetNamespace, DatasetNamespaceOwned, DatasetTag,
        DatasetVersion, DatasetVersionOwned,
    },
    db::{ConnError, Executor, Transaction},
    error::Error,
    jobs::{Job, JobId, JobStatus, JobStatusUpdateError},
    manifests::{ManifestHash, ManifestHashOwned, ManifestPath, ManifestPathOwned},
    notification_multiplexer::NotificationMultiplexerHandle,
    physical_table::{
        LocationId, LocationIdFromStrError, LocationIdI64ConvError, LocationIdU64Error,
        LocationWithDetails, PhysicalTable, TableWriterInfo, WriterTableInfo,
        events::{
            LocationNotifListener, LocationNotifRecvError, LocationNotifSendError,
            LocationNotification,
        },
    },
    workers::{
        Worker, WorkerInfo, WorkerInfoOwned, WorkerNodeId, WorkerNodeIdOwned,
        events::{NotifListener as WorkerNotifListener, NotifRecvError as WorkerNotifRecvError},
    },
};

/// Default pool size for the metadata DB.
pub const DEFAULT_POOL_SIZE: u32 = 10;

/// Connects to the metadata database with a single connection (no pooling).
/// Does not run migrations - the database schema must already be initialized.
#[instrument(skip_all, err)]
pub async fn connect(url: &str) -> Result<SingleConnMetadataDb, Error> {
    let conn = db::Connection::connect(url).await?;
    Ok(SingleConnMetadataDb(conn))
}

/// Connects to the metadata database with retry logic.
/// Retries on PostgreSQL error code 57P03 (database starting up). Does not run migrations.
#[instrument(skip_all, err)]
pub async fn connect_with_retry(url: &str) -> Result<SingleConnMetadataDb, Error> {
    let conn = db::Connection::connect_with_retry(url).await?;
    Ok(SingleConnMetadataDb(conn))
}

/// Connects to the metadata database with connection pooling.
/// Automatically runs migrations to ensure the database schema is up-to-date.
#[instrument(skip_all, err)]
pub async fn connect_pool(url: &str, size: u32) -> Result<MetadataDb, Error> {
    connect_pool_with_config(url, size, true).await
}

/// Connects to the metadata database with connection pooling and configurable migrations.
/// Similar to [`connect_pool`], but allows control over whether migrations run automatically.
#[instrument(skip_all, err)]
pub async fn connect_pool_with_config(
    url: &str,
    size: u32,
    auto_migrate: bool,
) -> Result<MetadataDb, Error> {
    let pool = db::ConnPool::connect(url, size).await?;
    if auto_migrate {
        pool.run_migrations().await?;
    }
    Ok(MetadataDb {
        pool,
        url: url.into(),
    })
}

/// Connects to the metadata database with connection pooling and retry logic.
/// Retries on PostgreSQL error code 57P03 with exponential backoff (10-100ms, max 20 attempts).
/// Automatically runs migrations after the pool is established.
#[instrument(skip_all, err)]
pub async fn connect_pool_with_retry(url: &str, size: u32) -> Result<MetadataDb, Error> {
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

    let pool = (|| db::ConnPool::connect(url, size))
        .retry(retry_policy)
        .when(is_db_starting_up)
        .notify(notify_retry)
        .await?;

    pool.run_migrations().await?;

    Ok(MetadataDb {
        pool,
        url: url.into(),
    })
}

/// Pooled connection handle to the metadata database. Clones share the same pool.
#[derive(Clone, Debug)]
pub struct MetadataDb {
    pool: db::ConnPool,
    pub(crate) url: Arc<str>,
}

impl MetadataDb {
    /// Returns the database URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Begins a new database transaction. Automatically rolls back when dropped unless committed.
    #[instrument(skip(self), err)]
    pub async fn begin_txn(&self) -> Result<Transaction<'static>, Error> {
        let tx = self.pool.begin().await.map_err(Error::Database)?;
        Ok(Transaction::new(tx))
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

/// Single-connection handle to the metadata database. Not cloneable.
#[derive(Debug)]
pub struct SingleConnMetadataDb(db::Connection);

impl SingleConnMetadataDb {
    /// Begins a new database transaction. Borrows the connection until transaction completes.
    /// Automatically rolls back when dropped unless committed.
    #[instrument(skip(self), err)]
    pub async fn begin_txn(&mut self) -> Result<Transaction<'_>, Error> {
        let tx = self.0.begin().await.map_err(Error::Database)?;
        Ok(Transaction::new(tx))
    }
}

// Implement sqlx::Executor for &mut SingleConnMetadataDb by delegating to the connection
impl<'c> sqlx::Executor<'c> for &'c mut SingleConnMetadataDb {
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
        (&mut self.0).fetch_many(query)
    }

    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxFuture<'e, Result<Option<<sqlx::Postgres as sqlx::Database>::Row>, sqlx::Error>>
    where
        'c: 'e,
        E: 'q + sqlx::Execute<'q, Self::Database>,
    {
        (&mut self.0).fetch_optional(query)
    }

    fn prepare_with<'e, 'q: 'e>(
        self,
        sql: &'q str,
        parameters: &'e [<sqlx::Postgres as sqlx::Database>::TypeInfo],
    ) -> BoxFuture<'e, Result<<sqlx::Postgres as sqlx::Database>::Statement<'q>, sqlx::Error>>
    where
        'c: 'e,
    {
        (&mut self.0).prepare_with(sql, parameters)
    }

    fn describe<'e, 'q: 'e>(
        self,
        sql: &'q str,
    ) -> BoxFuture<'e, Result<sqlx::Describe<Self::Database>, sqlx::Error>>
    where
        'c: 'e,
    {
        (&mut self.0).describe(sql)
    }
}

impl<'c> Executor<'c> for &'c mut SingleConnMetadataDb {}

impl _priv::Sealed for &mut SingleConnMetadataDb {}

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

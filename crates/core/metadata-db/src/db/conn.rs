//! Database connection and connection pool implementations

use std::time::Duration;

use sqlx::{
    Connection as _, PgConnection, Pool, Postgres,
    migrate::{MigrateError, Migrator},
    postgres::PgPoolOptions,
};

/// A dedicated connection to the metadata DB.
#[derive(Debug)]
pub struct Connection(PgConnection);

impl Connection {
    /// Creates a single database connection without running migrations.
    #[tracing::instrument(skip_all, err)]
    pub async fn connect(url: &str) -> Result<Self, ConnError> {
        PgConnection::connect(url)
            .await
            .map(Self)
            .map_err(ConnError::ConnectionError)
    }

    /// Creates a connection with exponential backoff retry for temporary databases.
    ///
    /// Retries up to 20 times when receiving error code 57P03 (database starting up).
    /// Used in test environments with ephemeral PostgreSQL instances.
    #[cfg(all(test, feature = "temp-db"))]
    #[tracing::instrument(skip_all, err)]
    pub async fn connect_with_retry(url: &str) -> Result<Self, ConnError> {
        use backon::{ExponentialBuilder, Retryable};

        let retry_policy = ExponentialBuilder::default()
            .with_min_delay(Duration::from_millis(10))
            .with_max_delay(Duration::from_millis(100))
            .with_max_times(20);

        fn is_db_starting_up(err: &sqlx::Error) -> bool {
            match err {
                sqlx::Error::Database(db_err) => db_err.code().is_some_and(|code| code == "57P03"),
                _ => false,
            }
        }

        fn notify_retry(err: &sqlx::Error, dur: Duration) {
            tracing::warn!(
                error = %err,
                "Database still starting up during connection. Retrying in {:.1}s",
                dur.as_secs_f32()
            );
        }

        (|| PgConnection::connect(url))
            .retry(retry_policy)
            .when(is_db_starting_up)
            .notify(notify_retry)
            .await
            .map(Self)
            .map_err(ConnError::ConnectionError)
    }

    /// Runs migrations on the database.
    ///
    /// SQLx does the right things:
    /// - Locks the DB before running migrations.
    /// - Never runs the same migration twice.
    /// - Errors on changes to old migrations.
    #[cfg(test)]
    #[tracing::instrument(skip(self), err)]
    pub async fn run_migrations(&mut self) -> Result<(), ConnError> {
        use sqlx::migrate::Migrator;

        static MIGRATOR: Migrator = sqlx::migrate!();
        MIGRATOR
            .run(&mut self.0)
            .await
            .map_err(ConnError::MigrationFailed)
    }
}

impl std::ops::Deref for Connection {
    type Target = PgConnection;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for Connection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// A connection pool to the metadata DB.
#[derive(Debug, Clone)]
pub struct ConnPool(Pool<Postgres>);

impl ConnPool {
    /// Creates a connection pool with the specified size and 5-second acquire timeout.
    #[tracing::instrument(skip_all, err)]
    pub async fn connect(url: &str, pool_size: u32) -> Result<Self, ConnError> {
        PgPoolOptions::new()
            .max_connections(pool_size)
            .acquire_timeout(Duration::from_secs(5))
            .connect(url)
            .await
            .map(Self)
            .map_err(ConnError::ConnectionError)
    }

    /// Runs migrations on the database.
    ///
    /// SQLx does the right things:
    /// - Locks the DB before running migrations.
    /// - Never runs the same migration twice.
    /// - Errors on changes to old migrations.
    #[tracing::instrument(skip(self), err)]
    pub async fn run_migrations(&self) -> Result<(), ConnError> {
        static MIGRATOR: Migrator = sqlx::migrate!();
        MIGRATOR
            .run(&self.0)
            .await
            .map_err(ConnError::MigrationFailed)
    }
}

impl std::ops::Deref for ConnPool {
    type Target = Pool<Postgres>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for ConnPool {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

// Implement sqlx::Executor for &ConnPool by delegating to the underlying Pool
impl<'c> sqlx::Executor<'c> for &'c ConnPool {
    type Database = Postgres;

    fn fetch_many<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> futures::stream::BoxStream<
        'e,
        Result<
            sqlx::Either<
                <Postgres as sqlx::Database>::QueryResult,
                <Postgres as sqlx::Database>::Row,
            >,
            sqlx::Error,
        >,
    >
    where
        'c: 'e,
        E: 'q + sqlx::Execute<'q, Self::Database>,
    {
        (&self.0).fetch_many(query)
    }

    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> futures::future::BoxFuture<
        'e,
        Result<Option<<Postgres as sqlx::Database>::Row>, sqlx::Error>,
    >
    where
        'c: 'e,
        E: 'q + sqlx::Execute<'q, Self::Database>,
    {
        (&self.0).fetch_optional(query)
    }

    fn prepare_with<'e, 'q: 'e>(
        self,
        sql: &'q str,
        parameters: &'e [<Postgres as sqlx::Database>::TypeInfo],
    ) -> futures::future::BoxFuture<
        'e,
        Result<<Postgres as sqlx::Database>::Statement<'q>, sqlx::Error>,
    >
    where
        'c: 'e,
    {
        (&self.0).prepare_with(sql, parameters)
    }

    fn describe<'e, 'q: 'e>(
        self,
        sql: &'q str,
    ) -> futures::future::BoxFuture<'e, Result<sqlx::Describe<Self::Database>, sqlx::Error>>
    where
        'c: 'e,
    {
        (&self.0).describe(sql)
    }
}

impl<'c> super::Executor<'c> for &'c ConnPool {}

impl crate::_priv::Sealed for &ConnPool {}

// Implement sqlx::Executor for &mut Connection by delegating to the underlying PgConnection
impl<'c> sqlx::Executor<'c> for &'c mut Connection {
    type Database = Postgres;

    fn fetch_many<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> futures::stream::BoxStream<
        'e,
        Result<
            sqlx::Either<
                <Postgres as sqlx::Database>::QueryResult,
                <Postgres as sqlx::Database>::Row,
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
    ) -> futures::future::BoxFuture<
        'e,
        Result<Option<<Postgres as sqlx::Database>::Row>, sqlx::Error>,
    >
    where
        'c: 'e,
        E: 'q + sqlx::Execute<'q, Self::Database>,
    {
        (&mut self.0).fetch_optional(query)
    }

    fn prepare_with<'e, 'q: 'e>(
        self,
        sql: &'q str,
        parameters: &'e [<Postgres as sqlx::Database>::TypeInfo],
    ) -> futures::future::BoxFuture<
        'e,
        Result<<Postgres as sqlx::Database>::Statement<'q>, sqlx::Error>,
    >
    where
        'c: 'e,
    {
        (&mut self.0).prepare_with(sql, parameters)
    }

    fn describe<'e, 'q: 'e>(
        self,
        sql: &'q str,
    ) -> futures::future::BoxFuture<'e, Result<sqlx::Describe<Self::Database>, sqlx::Error>>
    where
        'c: 'e,
    {
        (&mut self.0).describe(sql)
    }
}

impl<'c> super::Executor<'c> for &'c mut Connection {}

impl crate::_priv::Sealed for &mut Connection {}

/// Errors that can occur when connecting to the metadata DB.
#[derive(Debug, thiserror::Error)]
pub enum ConnError {
    /// Failed to establish database connection.
    #[error("Error connecting to metadata db: {0}")]
    ConnectionError(#[source] sqlx::Error),

    /// Failed to run database migrations.
    #[error("Error running migrations: {0}")]
    MigrationFailed(#[source] MigrateError),
}

//! Internal connection and connection pool implementations

use std::time::Duration;

use sqlx::{
    migrate::{MigrateError, Migrator},
    postgres::PgPoolOptions,
    Connection as _, PgConnection, Pool, Postgres,
};
use tracing::instrument;

/// Errors that can occur when connecting to the metadata DB.
#[derive(Debug, thiserror::Error)]
pub enum ConnError {
    /// Error connecting to the metadata DB.
    #[error("Error connecting to metadata db: {0}")]
    ConnectionError(#[source] sqlx::Error),

    /// An error occurred while running migrations.
    #[error("Error running migrations: {0}")]
    MigrationFailed(#[source] MigrateError),
}

/// A connection pool to the metadata DB.
#[derive(Debug, Clone)]
pub struct DbConnPool(Pool<Postgres>);

impl DbConnPool {
    /// Set up a connection pool to the metadata DB.
    #[instrument(skip_all, err)]
    pub async fn connect(url: &str) -> Result<Self, ConnError> {
        PgPoolOptions::new()
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
    #[instrument(skip(self), err)]
    pub async fn run_migrations(&self) -> Result<(), ConnError> {
        static MIGRATOR: Migrator = sqlx::migrate!();
        MIGRATOR
            .run(&self.0)
            .await
            .map_err(ConnError::MigrationFailed)
    }
}

impl std::ops::Deref for DbConnPool {
    type Target = Pool<Postgres>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for DbConnPool {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// A dedicated connection to the metadata DB.
#[derive(Debug)]
pub struct DbConn(PgConnection);

impl DbConn {
    /// Set up a connection to the metadata DB.
    ///
    /// Does not run migrations.
    #[instrument(skip_all, err)]
    pub async fn connect(url: &str) -> Result<Self, ConnError> {
        PgConnection::connect(url)
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
    #[instrument(skip(self), err)]
    pub async fn run_migrations(&mut self) -> Result<(), ConnError> {
        static MIGRATOR: Migrator = sqlx::migrate!();
        MIGRATOR
            .run(&mut self.0)
            .await
            .map_err(ConnError::MigrationFailed)
    }
}

impl std::ops::Deref for DbConn {
    type Target = PgConnection;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for DbConn {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

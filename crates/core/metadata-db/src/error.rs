//! Error types for metadata database operations

use crate::{WorkerNodeIdOwned, db::ConnError, physical_table::events::LocationNotifSendError};

/// Errors that can occur when interacting with the metadata database
///
/// This error type covers all metadata database operations including connection
/// management, migrations, query execution, and PostgreSQL LISTEN/NOTIFY events.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failed to establish connection to the metadata database
    ///
    /// This occurs during the initial connection phase when the database client
    /// fails to connect to PostgreSQL.
    ///
    /// Common causes:
    /// - Database server is not running or unreachable
    /// - Incorrect connection string or credentials
    /// - Network connectivity issues
    /// - Firewall blocking database port
    /// - Database server at capacity (too many connections)
    ///
    /// This error is considered retryable. See `is_connection_error()` method.
    #[error("Error connecting to metadata db: {0}")]
    Connection(sqlx::Error),

    /// Failed to run database migrations
    ///
    /// This occurs when sqlx migration runner fails to apply database schema migrations.
    /// Migrations are typically run during application startup to ensure the database
    /// schema is up to date.
    ///
    /// Common causes:
    /// - Migration files are missing or corrupted
    /// - Database schema is in an inconsistent state
    /// - Migration conflicts with existing database objects
    /// - Insufficient database permissions to ALTER tables
    /// - Migration syntax errors
    ///
    /// Migration failures are usually fatal and require manual intervention.
    #[error("Error running migrations: {0}")]
    Migration(#[source] sqlx::migrate::MigrateError),

    /// Database query execution failed
    ///
    /// This occurs when executing a SQL query against the metadata database fails.
    /// This is the most common error variant and covers all database operation failures.
    ///
    /// Common causes:
    /// - Connection lost during query execution (retryable - see `is_retryable()`)
    /// - SQL syntax errors (programming error)
    /// - Constraint violations (unique, foreign key, check constraints)
    /// - Serialization failures from concurrent transactions (retryable)
    /// - Deadlock detected (retryable)
    /// - Query timeout
    /// - Insufficient permissions
    ///
    /// Use `is_retryable()` to determine if the error should be retried.
    /// Use `is_foreign_key_violation()` to detect missing referenced entities.
    #[error("Error executing database query: {0}")]
    Database(#[source] sqlx::Error),

    /// Failed to send job notification via PostgreSQL NOTIFY
    ///
    /// This occurs when attempting to send a job start/stop notification to worker
    /// nodes via PostgreSQL's NOTIFY mechanism fails.
    ///
    /// Common causes:
    /// - Database connection lost before NOTIFY could be sent
    /// - NOTIFY payload exceeds PostgreSQL's 8000 byte limit
    /// - Transaction rollback prevented NOTIFY from being sent
    ///
    /// Workers use LISTEN/NOTIFY for real-time job coordination. Notification failures
    /// may delay job processing but workers will eventually reconcile via polling.
    #[error("Error sending job notification: {0}")]
    JobNotificationSend(#[source] crate::workers::events::NotifSendError),

    /// Failed to receive job notification via PostgreSQL LISTEN
    ///
    /// This occurs when the LISTEN connection encounters an error while receiving
    /// notifications from the database.
    ///
    /// Common causes:
    /// - Database connection lost
    /// - LISTEN connection closed unexpectedly
    /// - Malformed notification payload (deserialization error)
    ///
    /// This error typically indicates the LISTEN connection needs to be re-established.
    #[error("Error receiving job notification: {0}")]
    JobNotificationRecv(#[source] crate::workers::events::NotifRecvError),

    /// Failed to send location notification via PostgreSQL NOTIFY
    ///
    /// This occurs when attempting to send a physical table location notification
    /// via PostgreSQL's NOTIFY mechanism fails.
    ///
    /// Common causes:
    /// - Database connection lost before NOTIFY could be sent
    /// - NOTIFY payload exceeds PostgreSQL's 8000 byte limit
    /// - Transaction rollback prevented NOTIFY from being sent
    ///
    /// Location notifications inform query servers about new physical table locations.
    /// Notification failures may delay catalog updates but servers will eventually
    /// discover new locations via polling.
    #[error("Error sending location notification: {0}")]
    LocationNotificationSend(#[source] LocationNotifSendError),

    /// Multiple active physical table locations found for a manifest/table combination
    ///
    /// This occurs when the metadata database contains more than one active location
    /// for the same manifest_hash and table name combination. This indicates a data
    /// integrity issue as there should only be one active location per table.
    ///
    /// Common causes:
    /// - Concurrent operations failed to properly deactivate old locations
    /// - Manual database modifications bypassing application logic
    /// - Bug in location management code
    /// - Failed cleanup from previous location updates
    ///
    /// This is a serious data integrity error that requires investigation and manual
    /// database cleanup. The extra locations should be deactivated.
    #[error("Multiple active locations found for manifest_hash={0}, table={1}: {2:?}")]
    MultipleActiveLocations(String, String, Vec<String>),

    /// Failed to parse a URL
    ///
    /// This occurs when converting a file path or object store path to a URL fails.
    ///
    /// Common causes:
    /// - Invalid URL characters in path
    /// - Malformed file:// or s3:// URL
    /// - Path contains invalid percent-encoding
    ///
    /// This typically indicates corrupted data in the database or a bug in path
    /// generation code.
    #[error("Error parsing URL: {0}")]
    UrlParse(#[source] url::ParseError),

    /// Invalid file path provided
    ///
    /// This occurs when a file path cannot be processed or parsed correctly,
    /// such as when extracting the file name from a path fails.
    ///
    /// Common causes:
    /// - Path does not contain a valid file name component
    /// - Path contains invalid UTF-8 characters
    /// - Path is empty or malformed
    /// - Path ends with a directory separator
    ///
    /// This typically indicates invalid input data or a bug in path construction.
    #[error("Invalid file path: {0}")]
    InvalidFilePath(String),

    /// Failed to update job status
    ///
    /// This occurs when attempting to transition a job to a new status but the
    /// transition is invalid or the database update fails.
    ///
    /// Common causes:
    /// - Invalid status transition (e.g., COMPLETED -> RUNNING)
    /// - Job no longer exists in the database
    /// - Job already in a terminal state
    /// - Database connection error during update
    ///
    /// See `JobStatusUpdateError` for specific transition validation errors.
    #[error("Job status update error: {0}")]
    JobStatusUpdate(#[source] crate::jobs::JobStatusUpdateError),

    /// The specified worker node ID is already in use
    #[error("Worker node ID is already in use: {0}")]
    WorkerNodeIdInUse(WorkerNodeIdOwned),
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
            Error::Connection(_) => true,
            Error::Database(err) => matches!(
                err,
                sqlx::Error::Io(_)
                    | sqlx::Error::Tls(_)
                    | sqlx::Error::PoolTimedOut
                    | sqlx::Error::PoolClosed
            ),
            _ => false,
        }
    }

    /// Returns `true` if the error is retryable.
    ///
    /// This includes both connection errors and transaction-specific errors that are
    /// commonly encountered with concurrent transactions and row-level locking.
    ///
    /// The following errors are considered retryable:
    /// - **Connection errors**: Network issues, pool timeouts, TLS errors (checked via `is_connection_error`)
    /// - **Serialization failures**: Occur when two transactions conflict and one needs to be retried.
    ///   Common with `SELECT FOR UPDATE` and concurrent updates.
    /// - **Deadlock detected**: Two or more transactions are waiting for each other to release locks.
    ///   One transaction is aborted and should be retried.
    ///
    /// These transaction-specific errors are transient and safe to retry from the beginning
    /// of the transaction.
    pub fn is_retryable(&self) -> bool {
        // Check connection errors first
        if self.is_connection_error() {
            return true;
        }

        // Check for transaction-specific retryable errors
        matches!(
            self,
            Error::Database(sqlx::Error::Database(err))
                if err.code().is_some_and(|code| matches!(
                    code.as_ref(),
                    pg_error_codes::SERIALIZATION_FAILURE | pg_error_codes::DEADLOCK_DETECTED
                ))
        )
    }

    /// Returns `true` if the error is a foreign key constraint violation.
    ///
    /// This occurs when an INSERT or UPDATE operation violates a foreign key constraint,
    /// typically indicating that a referenced row does not exist.
    ///
    /// This is useful for distinguishing "referenced entity not found" errors from
    /// other database errors, allowing callers to provide more specific error messages.
    pub fn is_foreign_key_violation(&self) -> bool {
        matches!(
            self,
            Error::Database(sqlx::Error::Database(err))
                if matches!(err.kind(), sqlx::error::ErrorKind::ForeignKeyViolation)
        )
    }
}

impl From<ConnError> for Error {
    fn from(err: ConnError) -> Self {
        match err {
            ConnError::ConnectionError(err) => Error::Connection(err),
            ConnError::MigrationFailed(err) => Error::Migration(err),
        }
    }
}

/// PostgreSQL error codes for transaction-related errors.
///
/// For reference: <https://www.postgresql.org/docs/current/errcodes-appendix.html>
mod pg_error_codes {
    /// Serialization failure - occurs when two transactions conflict and one needs to be retried.
    /// Common with `SELECT FOR UPDATE` and concurrent updates.
    pub const SERIALIZATION_FAILURE: &str = "40001";

    /// Deadlock detected - two or more transactions are waiting for each other to release locks.
    /// One transaction is aborted and should be retried.
    pub const DEADLOCK_DETECTED: &str = "40P01";
}

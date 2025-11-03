//! Error types for metadata database operations

use crate::db::ConnError;

/// Errors that can occur when interacting with the metadata database
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Error connecting to metadata db: {0}")]
    ConnectionError(sqlx::Error),

    #[error("Error running migrations: {0}")]
    MigrationError(#[from] sqlx::migrate::MigrateError),

    #[error("Error executing database query: {0}")]
    DbError(#[from] sqlx::Error),

    #[error("Error sending job notification: {0}")]
    JobNotificationSendError(#[from] crate::workers::events::NotifSendError),

    #[error("Error receiving job notification: {0}")]
    JobNotificationRecvError(#[from] crate::workers::events::NotifRecvError),

    #[error("Error sending location notification: {0}")]
    LocationNotificationSendError(#[from] crate::locations::events::LocationNotifSendError),

    #[error(
        "Multiple active locations found for dataset={0}, dataset_version={1}, table={2}: {3:?}"
    )]
    MultipleActiveLocations(String, String, String, Vec<String>),

    #[error("Error parsing URL: {0}")]
    UrlParseError(#[from] url::ParseError),

    #[error("Job status update error: {0}")]
    JobStatusUpdateError(#[from] crate::jobs::JobStatusUpdateError),
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
            Error::DbError(sqlx::Error::Database(err))
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
            Error::DbError(sqlx::Error::Database(err))
                if matches!(err.kind(), sqlx::error::ErrorKind::ForeignKeyViolation)
        )
    }
}

impl From<ConnError> for Error {
    fn from(err: ConnError) -> Self {
        match err {
            ConnError::ConnectionError(err) => Error::ConnectionError(err),
            ConnError::MigrationFailed(err) => Error::MigrationError(err),
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

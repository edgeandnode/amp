use std::{future::Future, path::PathBuf};

pub use crate::postgres::{Handle, ShuttingDown};
use crate::postgres::{PostgresBuilder, PostgresError};

/// Creates a new persistent PostgreSQL database service with app defaults.
///
/// This is a convenience wrapper around [`PostgresBuilder`] that applies the
/// application's preferred defaults (locale `"C"`, encoding `"UTF8"`).
///
/// If the data directory already contains a PostgreSQL database (detected by
/// the presence of `PG_VERSION` file), the existing data is reused. Otherwise,
/// a new database cluster is initialized.
///
/// For full control over PostgreSQL configuration, use [`PostgresBuilder`] directly.
pub async fn new(
    data_dir: PathBuf,
) -> Result<(Handle, impl Future<Output = Result<(), PostgresError>>), PostgresError> {
    PostgresBuilder::new(data_dir)
        .locale("C")
        .encoding("UTF8")
        .start()
        .await
}

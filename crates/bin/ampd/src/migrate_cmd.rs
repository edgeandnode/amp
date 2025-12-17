use config::Config;

pub async fn run(config: Config) -> Result<(), Error> {
    let url = config
        .metadata_db
        .url
        .as_ref()
        .ok_or(Error::MissingDatabaseUrl)?;

    tracing::info!("Running migrations on metadata database...");
    let _metadata_db =
        metadata_db::connect_pool_with_config(url, config.metadata_db.pool_size, true)
            .await
            .map_err(Error::MigrationFailed)?;
    tracing::info!("Migrations completed successfully");

    Ok(())
}

/// Errors that can occur during database migration.
///
/// This error type covers all failure modes when running database migrations
/// on the metadata database.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The metadata database URL is not configured.
    ///
    /// This occurs when the `metadata_db.url` field is missing from the configuration.
    /// This field is required for the migrate command to know which database to migrate.
    #[error("metadata_db.url is required for migrate command")]
    MissingDatabaseUrl,

    /// Failed to run database migrations.
    ///
    /// This occurs when the migration process fails, which can happen due to:
    /// - Database connection errors
    /// - Migration script errors
    /// - Insufficient permissions
    /// - Database schema conflicts
    #[error("Failed to run migrations: {0}")]
    MigrationFailed(#[source] metadata_db::Error),
}

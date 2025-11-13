use amp_client::{AmpClient, PostgresStateStore};
use anyhow::{Context, Result};
use sqlx::postgres::PgPoolOptions;
use tracing::info;

use crate::{config::SyncConfig, engine::Engine, manager::StreamManager, manifest};

pub async fn run(config: SyncConfig) -> Result<()> {
    info!("Starting ampsync");

    // Create database connection pool
    let pool = PgPoolOptions::new()
        .max_connections(config.max_db_connections)
        .connect(&config.database_url)
        .await
        .context("Failed to connect to database")?;
    info!("Database connection established");

    // Run database migrations for the state store
    PostgresStateStore::migrate(&pool)
        .await
        .context("Failed to run state store migrations")?;
    info!("State store migrations complete");

    // Fetch dataset manifest
    let manifest = manifest::fetch_manifest(&config)
        .await
        .context("Failed to fetch dataset manifest")?;
    info!("Manifest loaded: {} tables found", manifest.tables.len());

    // Create tables
    let engine = Engine::new(pool.clone());
    for (table_name, schema) in &manifest.tables {
        info!("Creating table: {}", table_name);
        engine
            .create_table(table_name, schema)
            .await
            .with_context(|| format!("Failed to create table: {}", table_name))?;
    }
    info!("All tables created");

    // Create streaming client
    let mut client = AmpClient::from_endpoint(&config.amp_flight_addr)
        .await
        .context("Failed to create amp-client")?;

    // Apply custom headers (e.g., for authentication)
    if !config.headers.is_empty() {
        let keys: Vec<_> = config.headers.keys().map(|k| k.as_str()).collect();
        client.set_headers(&config.headers);
        info!("Applied custom headers: {:?}", keys);
    }

    info!("Amp client initialized");

    // Spawn streaming tasks
    let manager = StreamManager::spawn_all(&manifest, &config, engine, client, pool);

    // Wait for shutdown signal
    info!("Ampsync is running. Press Ctrl+C to stop.");
    tokio::signal::ctrl_c()
        .await
        .context("Failed to listen for Ctrl+C")?;

    // Shutdown all tasks
    manager.shutdown().await;

    info!("Ampsync shutdown complete");
    Ok(())
}

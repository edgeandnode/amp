use amp_client::{AmpClient, PostgresStateStore};
use anyhow::{Context, Result};
use sqlx::postgres::PgPoolOptions;
use tracing::info;

use crate::{config::SyncConfig, engine::Engine, health, manager::StreamManager};

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

    info!("Tables to sync: {} tables", config.tables.len());
    for table_name in &config.tables {
        info!("  - {}", table_name);
    }

    // Create engine
    let engine = Engine::new(pool.clone());

    // Convert PartialReference to full Reference by filling in defaults
    let dataset = config.dataset.to_full_reference();
    info!("Dataset: {}", dataset);

    // Create streaming client
    let mut client = AmpClient::from_endpoint(&config.amp_flight_addr)
        .await
        .context("Failed to create amp-client")?;

    // Apply authentication if provided
    if let Some(token) = &config.auth_token {
        client.set_token(token.as_str());
        info!("Applied authentication token");
    }

    info!("Amp client initialized");

    // Spawn streaming tasks (table creation happens in StreamTask::new)
    let manager = StreamManager::new(&config.tables, dataset, &config, engine, client, pool);

    // Start health server if configured
    if let Some(port) = config.health_port {
        let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
        let (bound_addr, health_fut) = health::serve(addr)
            .await
            .context("Failed to start health server")?;
        info!("Health server listening on {}", bound_addr);
        tokio::spawn(async move {
            if let Err(e) = health_fut.await {
                tracing::error!(error = %e, "Health server error");
            }
        });
    }

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

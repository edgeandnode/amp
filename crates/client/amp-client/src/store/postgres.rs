//! PostgreSQL-based state store implementation for durable streaming
//!
//! This module provides a PostgreSQL-backed implementation of the `StateStore` trait,
//! enabling crash-safe persistence of transactional stream state.

use std::collections::VecDeque;

use sqlx::{PgPool, postgres::PgQueryResult};

use super::{StateSnapshot, StateStore};
use crate::{
    error::Error,
    transactional::{Commit, TransactionId},
};

/// PostgreSQL implementation of StateStore (crash-safe).
///
/// State is persisted to a PostgreSQL database, suitable for production deployments
/// requiring crash recovery
///
/// # Example
/// ```rust,ignore
/// // Setup pool and migrate once
/// let pool = PgPoolOptions::new().connect(url).await?;
/// PostgresStateStore::migrate(&pool).await?;
///
/// // Create stores (reuse pool)
/// let store = PostgresStateStore::new(pool.clone(), "my-stream-id").await?;
/// let stream = client.stream("SELECT * FROM eth.logs SETTINGS stream = true")
///     .transactional(store, 128)
///     .await?;
/// ```
#[derive(Debug, Clone)]
pub struct PostgresStateStore {
    pool: PgPool,
    stream_id: String,
}

impl PostgresStateStore {
    /// Create a new state store from an existing connection pool.
    ///
    /// Initializes the state row for the stream if it doesn't exist.
    ///
    /// # Arguments
    /// * `pool` - PostgreSQL connection pool (can be cloned for multiple stores)
    /// * `stream_id` - Unique identifier for this stream
    ///
    /// # Errors
    /// Returns an error if state initialization fails.
    ///
    /// # Example
    /// ```rust,ignore
    /// // Setup pool and migrate once
    /// let pool = PgPoolOptions::new().connect(url).await?;
    /// PostgresStateStore::migrate(&pool).await?;
    ///
    /// // Create stores (reuse pool)
    /// let store1 = PostgresStateStore::new(pool.clone(), "stream-1").await?;
    /// let store2 = PostgresStateStore::new(pool.clone(), "stream-2").await?;
    /// ```
    pub async fn new(pool: PgPool, stream_id: impl Into<String>) -> Result<Self, Error> {
        let store = Self {
            pool,
            stream_id: stream_id.into(),
        };

        // Initialize state row
        store.initialize().await?;

        Ok(store)
    }

    /// Run database migrations.
    ///
    /// Call this once per database before creating any state stores.
    ///
    /// # Arguments
    /// * `pool` - PostgreSQL connection pool
    ///
    /// # Errors
    /// Returns an error if migrations fail.
    pub async fn migrate(pool: &PgPool) -> Result<(), Error> {
        sqlx::migrate!("./migrations")
            .run(pool)
            .await
            .map_err(|e| Error::Store(format!("Failed to run migrations: {}", e)))
    }

    /// Initialize the state row if it doesn't exist.
    ///
    /// Uses INSERT ON CONFLICT DO NOTHING to safely ensure the row exists.
    /// Called automatically by `new()`.
    async fn initialize(&self) -> Result<(), Error> {
        let query = r#"
            INSERT INTO amp_client_state (stream_id, next_transaction_id)
            VALUES ($1, 0)
            ON CONFLICT (stream_id) DO NOTHING
        "#;

        sqlx::query(query)
            .bind(&self.stream_id)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Store(format!("Failed to initialize state: {}", e)))?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl StateStore for PostgresStateStore {
    async fn advance(&mut self, next: TransactionId) -> Result<(), Error> {
        let query = r#"
            UPDATE amp_client_state
            SET next_transaction_id = $2,
                updated_at = NOW()
            WHERE stream_id = $1
        "#;

        let result: PgQueryResult = sqlx::query(query)
            .bind(&self.stream_id)
            .bind(next as i64)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Store(format!("Failed to advance next: {}", e)))?;

        if result.rows_affected() == 0 {
            return Err(Error::Store(format!(
                "Stream not found: {}",
                self.stream_id
            )));
        }

        Ok(())
    }

    async fn truncate(&mut self, from: TransactionId) -> Result<(), Error> {
        let query = r#"
            DELETE FROM amp_client_buffer_entries
            WHERE stream_id = $1
              AND transaction_id >= $2
        "#;

        sqlx::query(query)
            .bind(&self.stream_id)
            .bind(from as i64)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Store(format!("Failed to truncate buffer: {}", e)))?;

        Ok(())
    }

    async fn commit(&mut self, commit: Commit) -> Result<(), Error> {
        // Begin a transaction to ensure atomicity of all operations
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| Error::Store(format!("Failed to begin transaction: {}", e)))?;

        // Delete pruned entries (all entries <= prune transaction id)
        if let Some(prune) = commit.prune {
            let delete_pruned = r#"
                DELETE FROM amp_client_buffer_entries
                WHERE stream_id = $1
                  AND transaction_id <= $2
            "#;

            sqlx::query(delete_pruned)
                .bind(&self.stream_id)
                .bind(prune as i64)
                .execute(&mut *tx)
                .await
                .map_err(|e| Error::Store(format!("Failed to delete pruned entries: {}", e)))?;
        }

        // Insert new watermarks
        for (transaction_id, block_ranges) in &commit.insert {
            let insert_entry = r#"
                INSERT INTO amp_client_buffer_entries (stream_id, transaction_id, block_ranges)
                VALUES ($1, $2, $3)
                ON CONFLICT (stream_id, transaction_id) DO NOTHING
            "#;

            let block_ranges_json = serde_json::to_value(block_ranges)
                .map_err(|e| Error::Store(format!("Failed to serialize block ranges: {}", e)))?;

            sqlx::query(insert_entry)
                .bind(&self.stream_id)
                .bind(*transaction_id as i64)
                .bind(block_ranges_json)
                .execute(&mut *tx)
                .await
                .map_err(|e| Error::Store(format!("Failed to insert buffer entry: {}", e)))?;
        }

        // Commit the transaction
        tx.commit()
            .await
            .map_err(|e| Error::Store(format!("Failed to commit transaction: {}", e)))?;

        Ok(())
    }

    async fn load(&self) -> Result<StateSnapshot, Error> {
        // Load the next transaction id
        let state_query = r#"
            SELECT next_transaction_id
            FROM amp_client_state
            WHERE stream_id = $1
        "#;

        let next: i64 = sqlx::query_scalar(state_query)
            .bind(&self.stream_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| Error::Store(format!("Failed to load next transaction id: {}", e)))?;

        // Load buffer entries ordered by transaction id
        let buffer_query = r#"
            SELECT transaction_id, block_ranges
            FROM amp_client_buffer_entries
            WHERE stream_id = $1
            ORDER BY transaction_id ASC
        "#;

        let rows: Vec<(i64, serde_json::Value)> = sqlx::query_as(buffer_query)
            .bind(&self.stream_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::Store(format!("Failed to load buffer entries: {}", e)))?;

        // Reconstruct the buffer
        let mut buffer = VecDeque::with_capacity(rows.len());
        for (transaction_id, block_ranges_json) in rows {
            let block_ranges = serde_json::from_value(block_ranges_json)
                .map_err(|e| Error::Store(format!("Failed to deserialize block ranges: {}", e)))?;

            buffer.push_back((transaction_id as TransactionId, block_ranges));
        }

        Ok(StateSnapshot {
            buffer,
            next: next as TransactionId,
        })
    }
}

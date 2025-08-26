use sqlx::types::chrono::{DateTime, Utc};

pub mod events;
pub mod heartbeat;
pub mod job_id;
pub mod jobs;
mod node_id;

pub use self::node_id::WorkerNodeId;

/// Represents a worker node in the metadata database.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Worker {
    /// Unique identifier for the worker (auto-incremented)
    pub id: i64,

    /// ID of the worker node
    pub node_id: WorkerNodeId,

    /// Last heartbeat timestamp
    pub last_heartbeat: DateTime<Utc>,
}

/// In-tree integration tests
#[cfg(test)]
mod tests {
    mod it_events;
    mod it_heartbeat;
    mod it_jobs;
}

//! Worker nodes metadata

use sqlx::types::chrono::{DateTime, Utc};

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

/// Represents a worker node ID.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize, sqlx::Type,
)]
#[repr(transparent)]
#[serde(transparent)]
#[sqlx(transparent)]
pub struct WorkerNodeId(String);

impl WorkerNodeId {
    /// Returns the worker ID as a string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for WorkerNodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for WorkerNodeId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<WorkerNodeId> for String {
    fn from(id: WorkerNodeId) -> Self {
        id.0
    }
}

impl std::str::FromStr for WorkerNodeId {
    type Err = InvalidWorkerId;

    /// Parses a worker ID from a string.
    ///
    /// The worker ID must start with a letter and contain only alphanumeric characters,
    /// underscores, hyphens, and dots. It must not be empty, and must start with a letter.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(c) = s.chars().next() {
            if !c.is_alphabetic() {
                return Err(InvalidWorkerId {
                    id: s.to_string(),
                    reason: "must start with a letter".into(),
                });
            }
        } else {
            return Err(InvalidWorkerId {
                id: s.to_string(),
                reason: "empty string".into(),
            });
        }

        if let Some(c) = s
            .chars()
            .find(|c| !c.is_alphanumeric() && *c != '_' && *c != '-' && *c != '.')
        {
            return Err(InvalidWorkerId {
                id: s.to_string(),
                reason: format!("invalid character '{c}'").into(),
            });
        }

        Ok(WorkerNodeId(s.to_string()))
    }
}

/// Error returned when a worker ID is invalid.
#[derive(Debug, thiserror::Error)]
#[error("Invalid worker ID '{id}': {reason}")]
pub struct InvalidWorkerId {
    id: String,
    #[source]
    reason: Box<dyn std::error::Error + Send + Sync>,
}

#[cfg(test)]
mod tests {
    use super::WorkerNodeId;

    #[test]
    fn parse_valid_worker_id() {
        assert!(
            "valid-worker-id".parse::<WorkerNodeId>().is_ok(),
            "It should parse a valid worker ID"
        );
        assert!(
            "valid-worker-id-1".parse::<WorkerNodeId>().is_ok(),
            "It should parse a valid worker ID"
        );
        assert!(
            "valid_worker_id".parse::<WorkerNodeId>().is_ok(),
            "It should parse a valid worker ID"
        );
        assert!(
            "valid.worker.id".parse::<WorkerNodeId>().is_ok(),
            "It should parse a valid worker ID"
        );
    }

    #[test]
    fn error_on_invalid_worker_id() {
        // Empty string
        assert!(
            "".parse::<WorkerNodeId>().is_err(),
            "It should not parse an empty string"
        );

        // String starting with a non-alphabetic character
        assert!(
            "123invalid-worker-id".parse::<WorkerNodeId>().is_err(),
            "It should not parse a string starting with a number"
        );
        assert!(
            " invalid-worker-id".parse::<WorkerNodeId>().is_err(),
            "It should not parse a string starting with a space"
        );
        assert!(
            "!invalid-worker-id".parse::<WorkerNodeId>().is_err(),
            "It should not parse a string starting with a special character"
        );

        // String containing whitespace
        assert!(
            "invalid worker id".parse::<WorkerNodeId>().is_err(),
            "It should not parse a string with whitespace"
        );

        // String containing invalid characters
        assert!(
            "invalid-worker-id!".parse::<WorkerNodeId>().is_err(),
            "It should not parse a string with an invalid character"
        );
        assert!(
            "invalid-worker-id@".parse::<WorkerNodeId>().is_err(),
            "It should not parse a string with an invalid character"
        );
        assert!(
            "invalid-worker-id#".parse::<WorkerNodeId>().is_err(),
            "It should not parse a string with an invalid character"
        );
    }
}

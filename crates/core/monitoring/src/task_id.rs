//! Unique identifier for task executions.
//!
//! This module provides the [`TaskId`] type used to track individual task executions
//! (queries, dumps, compactions, etc.) through the system, enabling per-task metrics and tracing.

use std::fmt;

/// Unique identifier for a task execution.
///
/// Uses UUIDv7 which incorporates a timestamp, providing both uniqueness
/// and chronological ordering of task IDs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TaskId(uuid::Uuid);

impl TaskId {
    /// Create a new task ID with the current timestamp.
    ///
    /// Uses UUIDv7 which embeds a millisecond-precision timestamp in the first 48 bits,
    /// followed by random bits for uniqueness.
    pub fn new() -> Self {
        Self(uuid::Uuid::now_v7())
    }

    /// Get the task ID as a string.
    ///
    /// Returns the standard hyphenated UUID format:
    /// `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
    pub fn as_str(&self) -> String {
        self.0.to_string()
    }

    /// Get the inner UUID.
    pub fn as_uuid(&self) -> &uuid::Uuid {
        &self.0
    }
}

impl Default for TaskId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<uuid::Uuid> for TaskId {
    fn from(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_id_uniqueness() {
        let id1 = TaskId::new();
        let id2 = TaskId::new();
        assert_ne!(id1, id2, "Task IDs should be unique");
    }

    #[test]
    fn test_task_id_display() {
        let id = TaskId::new();
        let display_str = format!("{}", id);
        let as_str = id.as_str();
        assert_eq!(display_str, as_str);
    }

    #[test]
    fn test_task_id_chronological() {
        let id1 = TaskId::new();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let id2 = TaskId::new();

        // UUIDv7 embeds timestamp, so newer IDs should be "greater"
        assert!(id2.as_uuid() > id1.as_uuid());
    }
}

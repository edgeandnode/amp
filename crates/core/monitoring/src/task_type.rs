//! Task type definitions and metadata.
//!
//! This module provides the [`TaskType`] enum which categorizes different types of
//! operations (queries, dumps, compactions, etc.) and includes task-specific metadata.

use std::collections::HashMap;

use datafusion::logical_expr::LogicalPlan;

use crate::plan_histogram;

/// Type of task being executed with associated metadata.
///
/// This enum captures both the category of work being done and relevant
/// contextual information for observability.
#[derive(Debug, Clone)]
pub enum TaskType {
    /// SQL query execution with plan analysis
    QueryExecution {
        /// The SQL query text
        sql: String,
        /// Histogram of logical plan node types
        plan_node_histogram: HashMap<String, usize>,
    },
    /// Logical plan execution (may or may not have originated from SQL)
    PlanExecution {
        /// Histogram of logical plan node types
        plan_node_histogram: HashMap<String, usize>,
        /// Whether this is a streaming query
        is_streaming: bool,
    },
    /// Data dump/extraction operation
    Dump {
        /// Dataset being dumped
        dataset: String,
        /// Block range being extracted (start, end)
        block_range: (u64, u64),
        /// Number of records extracted (if known)
        records_extracted: Option<usize>,
    },
    /// File compaction operation
    Compact {
        /// Dataset being compacted
        dataset: String,
        /// Number of input files
        input_files: usize,
        /// Number of output files
        output_files: usize,
    },
    /// Garbage collection operation
    Collect {
        /// Dataset being collected
        dataset: String,
        /// Number of files collected
        files_collected: usize,
    },
    /// Generic operation with custom metadata
    Generic {
        /// Operation name
        operation: String,
        /// Custom key-value metadata
        metadata: HashMap<String, String>,
    },
}

impl TaskType {
    /// Get a human-readable name for this task type.
    pub fn name(&self) -> &'static str {
        match self {
            TaskType::QueryExecution { .. } => "query_execution",
            TaskType::PlanExecution { .. } => "plan_execution",
            TaskType::Dump { .. } => "dump",
            TaskType::Compact { .. } => "compact",
            TaskType::Collect { .. } => "collect",
            TaskType::Generic { .. } => "generic",
        }
    }

    /// Create a QueryExecution task type from SQL and a logical plan.
    ///
    /// This is a convenience method that automatically collects the plan histogram.
    pub fn from_query(sql: impl Into<String>, plan: &LogicalPlan) -> Self {
        let plan_node_histogram = plan_histogram::collect_plan_histogram(plan);
        TaskType::QueryExecution {
            sql: sql.into(),
            plan_node_histogram,
        }
    }

    /// Create a PlanExecution task type from a logical plan.
    ///
    /// This is a convenience method that automatically collects the plan histogram.
    pub fn from_plan(plan: &LogicalPlan, is_streaming: bool) -> Self {
        let plan_node_histogram = plan_histogram::collect_plan_histogram(plan);
        TaskType::PlanExecution {
            plan_node_histogram,
            is_streaming,
        }
    }
}

/// Truncate a SQL string for display purposes.
pub fn truncate_sql(sql: &str, max_len: usize) -> String {
    let trimmed = sql.trim();
    if trimmed.len() <= max_len {
        trimmed.to_string()
    } else {
        format!("{}...", &trimmed[..max_len])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_type_names() {
        let query = TaskType::QueryExecution {
            sql: "SELECT 1".to_string(),
            plan_node_histogram: HashMap::new(),
        };
        assert_eq!(query.name(), "query_execution");

        let dump = TaskType::Dump {
            dataset: "test".to_string(),
            block_range: (0, 100),
            records_extracted: None,
        };
        assert_eq!(dump.name(), "dump");

        let compact = TaskType::Compact {
            dataset: "test".to_string(),
            input_files: 10,
            output_files: 1,
        };
        assert_eq!(compact.name(), "compact");
    }

    #[test]
    fn test_truncate_sql() {
        let short_sql = "SELECT * FROM blocks";
        assert_eq!(truncate_sql(short_sql, 100), short_sql);

        let long_sql = "SELECT * FROM blocks WHERE block_number > 1000 AND timestamp < 2000";
        let truncated = truncate_sql(long_sql, 20);
        assert_eq!(truncated, "SELECT * FROM blocks...");
        assert_eq!(truncated.len(), 23); // 20 chars + "..."
    }
}

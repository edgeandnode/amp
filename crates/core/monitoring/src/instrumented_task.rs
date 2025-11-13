//! Generic task execution instrumentation with runtime metrics and tracing.
//!
//! This module provides [`InstrumentedTaskExecution`] which wraps any task execution
//! with comprehensive instrumentation including:
//! - Distributed tracing spans with structured fields
//! - Per-task tokio runtime metrics collection
//! - Automatic metrics finalization and recording
//! - Task-local context propagation
//! - Task-specific metadata (plan histograms, block ranges, etc.)

use std::future::Future;

use tracing::{Span, field};

use crate::{
    task_id::TaskId,
    task_runtime_context::{TaskMetrics, TaskRuntimeContext},
    task_type::TaskType,
};

/// Wrapper that instruments task execution with metrics and tracing.
///
/// This is the generic version that works with any `TaskType`, providing
/// comprehensive observability for any operation (queries, dumps, compactions, etc).
///
/// # Example
///
/// ```ignore
/// use monitoring::{InstrumentedTaskExecution, TaskType};
///
/// let task_type = TaskType::from_query("SELECT * FROM blocks", &logical_plan);
/// let instrumentation = InstrumentedTaskExecution::new(task_type);
///
/// let (result, metrics) = instrumentation.execute(async {
///     // Your task execution code here
///     query_engine.execute(sql).await
/// }).await;
/// ```
pub struct InstrumentedTaskExecution {
    /// Runtime context for metrics collection
    context: TaskRuntimeContext,

    /// Tracing span for this task execution
    span: Span,
}

impl InstrumentedTaskExecution {
    /// Create a new instrumented task execution.
    ///
    /// This creates a new task context and tracing span with the task ID
    /// and task-specific fields.
    pub fn new(task_type: TaskType) -> Self {
        let context = TaskRuntimeContext::new(task_type.clone());

        let span = create_span_for_task(&context.task_id, &task_type);

        Self { context, span }
    }

    /// Create instrumentation for a SQL query execution (convenience method).
    ///
    /// This is a shorthand for creating a `TaskType::QueryExecution` and
    /// instrumenting it. The plan histogram will be empty initially.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let instrumentation = InstrumentedTaskExecution::for_query("SELECT * FROM blocks");
    /// let (result, metrics) = instrumentation.execute(async {
    ///     query_engine.execute(sql).await
    /// }).await;
    /// ```
    pub fn for_query(sql: impl Into<String>) -> Self {
        use std::collections::HashMap;
        let task_type = TaskType::QueryExecution {
            sql: sql.into(),
            plan_node_histogram: HashMap::new(),
        };
        Self::new(task_type)
    }

    /// Create instrumentation with a specific task ID.
    ///
    /// Useful when you want to correlate with an existing task ID from
    /// another part of the system.
    pub fn with_task_id(task_id: TaskId, task_type: TaskType) -> Self {
        let context = TaskRuntimeContext::with_task_id(task_id, task_type.clone());

        let span = create_span_for_task(&context.task_id, &task_type);

        Self { context, span }
    }

    /// Create instrumentation for a SQL query with a specific task ID (convenience method).
    ///
    /// Useful when you want to correlate with an existing task ID from
    /// another part of the system.
    pub fn for_query_with_task_id(task_id: TaskId, sql: impl Into<String>) -> Self {
        use std::collections::HashMap;
        let task_type = TaskType::QueryExecution {
            sql: sql.into(),
            plan_node_histogram: HashMap::new(),
        };
        Self::with_task_id(task_id, task_type)
    }

    /// Execute a future with full instrumentation.
    ///
    /// This:
    /// 1. Enters the tracing span
    /// 2. Wraps the future with task monitoring
    /// 3. Sets up task-local context
    /// 4. Automatically records metrics to the span on completion
    ///
    /// Returns the result of the future along with the final metrics.
    pub async fn execute<F, R>(self, future: F) -> (R, TaskMetrics)
    where
        F: Future<Output = R>,
    {
        let _enter = self.span.enter();

        // Execute within monitored scope
        let result = self.context.scope(future).await;

        // Finalize and record metrics
        self.context.finalize_metrics();
        let metrics = self
            .context
            .final_metrics()
            .expect("Metrics should be finalized");

        // Record metrics to the span
        record_metrics_to_span(&self.span, &metrics, &self.context);

        (result, metrics)
    }

    /// Execute a future and return only the result (metrics are recorded but discarded).
    ///
    /// Use this when you only care about the tracing side effects, not the metrics themselves.
    pub async fn execute_and_discard_metrics<F, R>(self, future: F) -> R
    where
        F: Future<Output = R>,
    {
        let (result, _metrics) = self.execute(future).await;
        result
    }

    /// Get access to the task ID.
    pub fn task_id(&self) -> TaskId {
        self.context.task_id
    }

    /// Get access to the underlying context (for advanced usage).
    pub fn context(&self) -> &TaskRuntimeContext {
        &self.context
    }

    /// Get access to the span (for advanced usage).
    pub fn span(&self) -> &Span {
        &self.span
    }
}

/// Create a tracing span appropriate for the given task type.
fn create_span_for_task(task_id: &TaskId, task_type: &TaskType) -> Span {
    match task_type {
        TaskType::QueryExecution { sql, .. } => {
            tracing::info_span!(
                "query_execution",
                task_id = %task_id,
                task_type = task_type.name(),
                sql = %sql,
                // Plan complexity fields
                plan.total_nodes = field::Empty,
                plan.node_summary = field::Empty,
                // Runtime metrics fields (will be recorded at the end)
                runtime.busy_ms = field::Empty,
                runtime.idle_ms = field::Empty,
                runtime.poll_count = field::Empty,
                runtime.scheduled_count = field::Empty,
                runtime.cpu_utilization = field::Empty,
                runtime.avg_poll_us = field::Empty,
                // Parallelism metrics
                runtime.unique_threads = field::Empty,
                runtime.thread_ids = field::Empty,
                // Wall-clock time
                elapsed_ms = field::Empty,
            )
        }
        TaskType::PlanExecution { is_streaming, .. } => {
            tracing::info_span!(
                "plan_execution",
                task_id = %task_id,
                task_type = task_type.name(),
                is_streaming = is_streaming,
                // Plan complexity fields
                plan.total_nodes = field::Empty,
                plan.node_summary = field::Empty,
                // Runtime metrics fields
                runtime.busy_ms = field::Empty,
                runtime.idle_ms = field::Empty,
                runtime.poll_count = field::Empty,
                runtime.scheduled_count = field::Empty,
                runtime.cpu_utilization = field::Empty,
                runtime.avg_poll_us = field::Empty,
                // Parallelism metrics
                runtime.unique_threads = field::Empty,
                runtime.thread_ids = field::Empty,
                elapsed_ms = field::Empty,
            )
        }
        TaskType::Dump {
            dataset,
            block_range,
            records_extracted,
        } => {
            tracing::info_span!(
                "dump",
                task_id = %task_id,
                task_type = task_type.name(),
                dataset = %dataset,
                block_range.start = block_range.0,
                block_range.end = block_range.1,
                records_extracted = records_extracted,
                // Runtime metrics fields
                runtime.busy_ms = field::Empty,
                runtime.idle_ms = field::Empty,
                runtime.poll_count = field::Empty,
                runtime.scheduled_count = field::Empty,
                runtime.cpu_utilization = field::Empty,
                // Parallelism metrics
                runtime.unique_threads = field::Empty,
                runtime.thread_ids = field::Empty,
                elapsed_ms = field::Empty,
            )
        }
        TaskType::Compact {
            dataset,
            input_files,
            output_files,
        } => {
            tracing::info_span!(
                "compact",
                task_id = %task_id,
                task_type = task_type.name(),
                dataset = %dataset,
                input_files = input_files,
                output_files = output_files,
                // Runtime metrics fields
                runtime.busy_ms = field::Empty,
                runtime.idle_ms = field::Empty,
                runtime.poll_count = field::Empty,
                runtime.scheduled_count = field::Empty,
                runtime.cpu_utilization = field::Empty,
                // Parallelism metrics
                runtime.unique_threads = field::Empty,
                runtime.thread_ids = field::Empty,
                elapsed_ms = field::Empty,
            )
        }
        TaskType::Collect {
            dataset,
            files_collected,
        } => {
            tracing::info_span!(
                "collect",
                task_id = %task_id,
                task_type = task_type.name(),
                dataset = %dataset,
                files_collected = files_collected,
                // Runtime metrics fields
                runtime.busy_ms = field::Empty,
                runtime.idle_ms = field::Empty,
                runtime.poll_count = field::Empty,
                runtime.scheduled_count = field::Empty,
                runtime.cpu_utilization = field::Empty,
                // Parallelism metrics
                runtime.unique_threads = field::Empty,
                runtime.thread_ids = field::Empty,
                elapsed_ms = field::Empty,
            )
        }
        TaskType::Generic {
            operation,
            metadata,
        } => {
            let span = tracing::info_span!(
                "generic_task",
                task_id = %task_id,
                task_type = task_type.name(),
                operation = %operation,
                // Runtime metrics fields
                runtime.busy_ms = field::Empty,
                runtime.idle_ms = field::Empty,
                runtime.poll_count = field::Empty,
                runtime.scheduled_count = field::Empty,
                runtime.cpu_utilization = field::Empty,
                // Parallelism metrics
                runtime.unique_threads = field::Empty,
                runtime.thread_ids = field::Empty,
                elapsed_ms = field::Empty,
            );

            // Record custom metadata as span attributes
            for (key, value) in metadata {
                span.record(key.as_str(), value.as_str());
            }

            span
        }
    }
}

/// Record metrics to the tracing span, including task-specific fields.
fn record_metrics_to_span(span: &Span, metrics: &TaskMetrics, context: &TaskRuntimeContext) {
    // Record standard runtime metrics
    span.record("runtime.busy_ms", metrics.busy_duration_ms());
    span.record("runtime.idle_ms", metrics.idle_duration_ms());
    span.record("runtime.poll_count", metrics.total_poll_count);
    span.record("runtime.scheduled_count", metrics.total_scheduled_count);
    span.record("runtime.cpu_utilization", metrics.cpu_utilization());

    if let Some(avg_poll_us) = metrics.avg_poll_duration_us() {
        span.record("runtime.avg_poll_us", avg_poll_us);
    }

    // Record parallelism metrics
    span.record("runtime.unique_threads", metrics.unique_thread_count);
    let thread_ids_str = format!("{:?}", metrics.thread_ids);
    span.record("runtime.thread_ids", thread_ids_str.as_str());

    let elapsed_ms = context.elapsed().as_secs_f64() * 1000.0;
    span.record("elapsed_ms", elapsed_ms);

    // Record task-specific fields and log completion
    match &context.task_type {
        TaskType::QueryExecution {
            sql,
            plan_node_histogram,
        } => {
            // Record plan complexity metrics
            let total_nodes = crate::plan_histogram::total_nodes(plan_node_histogram);
            let node_summary = crate::plan_histogram::summarize_histogram(plan_node_histogram, 5);

            span.record("plan.total_nodes", total_nodes);
            span.record("plan.node_summary", node_summary.as_str());

            // Log query completion with plan complexity and parallelism
            tracing::info!(
                task_id = %context.task_id,
                elapsed_ms = elapsed_ms,
                busy_ms = metrics.busy_duration_ms(),
                idle_ms = metrics.idle_duration_ms(),
                cpu_utilization = metrics.cpu_utilization(),
                unique_threads = metrics.unique_thread_count,
                plan_nodes = total_nodes,
                plan_summary = %node_summary,
                sql = %crate::task_type::truncate_sql(sql, 100),
                "Query execution completed"
            );
        }
        TaskType::PlanExecution {
            plan_node_histogram,
            is_streaming,
        } => {
            // Record plan complexity metrics
            let total_nodes = crate::plan_histogram::total_nodes(plan_node_histogram);
            let node_summary = crate::plan_histogram::summarize_histogram(plan_node_histogram, 5);

            span.record("plan.total_nodes", total_nodes);
            span.record("plan.node_summary", node_summary.as_str());

            // Log plan execution completion with parallelism
            tracing::info!(
                task_id = %context.task_id,
                elapsed_ms = elapsed_ms,
                cpu_utilization = metrics.cpu_utilization(),
                unique_threads = metrics.unique_thread_count,
                plan_nodes = total_nodes,
                plan_summary = %node_summary,
                is_streaming = is_streaming,
                "Plan execution completed"
            );
        }
        TaskType::Dump {
            dataset,
            block_range,
            records_extracted,
        } => {
            tracing::info!(
                task_id = %context.task_id,
                elapsed_ms = elapsed_ms,
                busy_ms = metrics.busy_duration_ms(),
                idle_ms = metrics.idle_duration_ms(),
                cpu_utilization = metrics.cpu_utilization(),
                unique_threads = metrics.unique_thread_count,
                dataset = %dataset,
                blocks = format!("{}-{}", block_range.0, block_range.1),
                records = ?records_extracted,
                "Dump completed"
            );
        }
        TaskType::Compact {
            dataset,
            input_files,
            output_files,
        } => {
            tracing::info!(
                task_id = %context.task_id,
                elapsed_ms = elapsed_ms,
                cpu_utilization = metrics.cpu_utilization(),
                unique_threads = metrics.unique_thread_count,
                dataset = %dataset,
                input_files = input_files,
                output_files = output_files,
                "Compaction completed"
            );
        }
        TaskType::Collect {
            dataset,
            files_collected,
        } => {
            tracing::info!(
                task_id = %context.task_id,
                elapsed_ms = elapsed_ms,
                cpu_utilization = metrics.cpu_utilization(),
                unique_threads = metrics.unique_thread_count,
                dataset = %dataset,
                files_collected = files_collected,
                "Garbage collection completed"
            );
        }
        TaskType::Generic { operation, .. } => {
            tracing::info!(
                task_id = %context.task_id,
                elapsed_ms = elapsed_ms,
                cpu_utilization = metrics.cpu_utilization(),
                unique_threads = metrics.unique_thread_count,
                operation = %operation,
                "Task completed"
            );
        }
    }
}

/// RAII guard for automatic metrics recording.
///
/// This is an alternative API that uses Drop to automatically record metrics.
/// Useful when you want to instrument a block of code without explicitly
/// calling `execute()`.
///
/// # Example
///
/// ```ignore
/// use monitoring::{InstrumentedTaskGuard, TaskType};
///
/// let task_type = TaskType::from_query("SELECT * FROM blocks", &plan);
/// let _guard = InstrumentedTaskGuard::new(task_type);
///
/// // Your code here...
/// // Metrics will be automatically finalized and recorded on drop
/// ```
pub struct InstrumentedTaskGuard {
    context: TaskRuntimeContext,
    span: Span,
    _entered: tracing::span::EnteredSpan,
}

impl InstrumentedTaskGuard {
    /// Create a new guard and enter the span.
    pub fn new(task_type: TaskType) -> Self {
        let instrumentation = InstrumentedTaskExecution::new(task_type);
        let entered = instrumentation.span.clone().entered();

        Self {
            context: instrumentation.context,
            span: instrumentation.span,
            _entered: entered,
        }
    }

    /// Create a guard for a SQL query (convenience method).
    ///
    /// This is a shorthand for creating a `TaskType::QueryExecution` guard.
    pub fn for_query(sql: impl Into<String>) -> Self {
        let instrumentation = InstrumentedTaskExecution::for_query(sql);
        let entered = instrumentation.span.clone().entered();

        Self {
            context: instrumentation.context,
            span: instrumentation.span,
            _entered: entered,
        }
    }

    /// Get the task ID.
    pub fn task_id(&self) -> TaskId {
        self.context.task_id
    }

    /// Get the runtime context.
    pub fn context(&self) -> &TaskRuntimeContext {
        &self.context
    }

    /// Manually finalize and get metrics (otherwise happens on Drop).
    pub fn finalize(self) -> TaskMetrics {
        self.context.finalize_metrics();
        self.context
            .final_metrics()
            .expect("Metrics should be finalized")
    }
}

impl Drop for InstrumentedTaskGuard {
    fn drop(&mut self) {
        // Finalize metrics and record to span
        self.context.finalize_metrics();
        if let Some(metrics) = self.context.final_metrics() {
            record_metrics_to_span(&self.span, &metrics, &self.context);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[tokio::test]
    async fn test_instrumented_task_execution() {
        let task_type = TaskType::Generic {
            operation: "test_operation".to_string(),
            metadata: HashMap::new(),
        };
        let instrumentation = InstrumentedTaskExecution::new(task_type);

        let (result, metrics) = instrumentation
            .execute(async {
                // Simulate some work
                for _ in 0..10 {
                    tokio::task::yield_now().await;
                }
                42
            })
            .await;

        assert_eq!(result, 42);
        assert!(metrics.total_poll_count > 0);
        assert!(metrics.busy_duration_ms() > 0.0);
    }

    #[tokio::test]
    async fn test_dump_task_instrumentation() {
        let task_type = TaskType::Dump {
            dataset: "eth_blocks".to_string(),
            block_range: (0, 1000),
            records_extracted: None,
        };
        let instrumentation = InstrumentedTaskExecution::new(task_type);

        let (result, metrics) = instrumentation
            .execute(async {
                // Simulate dump work
                tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;
                "dump_completed"
            })
            .await;

        assert_eq!(result, "dump_completed");
        assert!(metrics.total_poll_count > 0);
    }

    #[tokio::test]
    async fn test_for_query_convenience_method() {
        let instrumentation = InstrumentedTaskExecution::for_query("SELECT * FROM blocks");

        let (result, metrics) = instrumentation
            .execute(async {
                // Simulate query work
                for _ in 0..10 {
                    tokio::task::yield_now().await;
                }
                42
            })
            .await;

        assert_eq!(result, 42);
        assert!(metrics.total_poll_count > 0);
        assert!(metrics.busy_duration_ms() > 0.0);
    }

    #[tokio::test]
    async fn test_for_query_with_task_id() {
        let task_id = TaskId::new();
        let instrumentation =
            InstrumentedTaskExecution::for_query_with_task_id(task_id, "SELECT 1");

        assert_eq!(instrumentation.task_id(), task_id);

        let (result, metrics) = instrumentation
            .execute(async {
                tokio::task::yield_now().await;
                "done"
            })
            .await;

        assert_eq!(result, "done");
        assert!(metrics.total_poll_count > 0);
    }

    #[tokio::test]
    async fn test_instrumented_task_guard() {
        let task_type = TaskType::Generic {
            operation: "test_guard".to_string(),
            metadata: HashMap::new(),
        };
        let guard = InstrumentedTaskGuard::new(task_type);
        let task_id = guard.task_id();

        // Simulate work
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }

        // Guard will automatically finalize on drop
        drop(guard);

        // Verify task ID was valid
        assert_ne!(task_id.as_str(), "");
    }

    #[tokio::test]
    async fn test_instrumented_task_guard_for_query() {
        let guard = InstrumentedTaskGuard::for_query("SELECT * FROM logs");
        let task_id = guard.task_id();

        // Simulate query work
        for _ in 0..3 {
            tokio::task::yield_now().await;
        }

        // Note: Guard doesn't use scope(), so it won't track tokio task metrics
        // The guard is primarily for tracing spans with automatic finalization
        drop(guard);

        assert_ne!(task_id.as_str(), "");
    }

    #[tokio::test]
    async fn test_context_propagation_with_task_guard() {
        let task_type = TaskType::Generic {
            operation: "test_propagation".to_string(),
            metadata: HashMap::new(),
        };
        let guard = InstrumentedTaskGuard::new(task_type);
        let expected_id = guard.task_id();

        // Verify context is accessible
        let ctx = TaskRuntimeContext::try_current();
        // Note: Guard doesn't use scope(), so context won't be available via task-local
        // This test documents the current behavior difference
        assert!(
            ctx.is_none(),
            "Guard doesn't propagate context via task-local (use execute() for that)"
        );

        drop(guard);
        assert_ne!(expected_id.as_str(), "");
    }
}

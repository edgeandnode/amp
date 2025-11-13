//! Generic task runtime metrics tracking context.
//!
//! This module provides [`TaskRuntimeContext`] which tracks tokio task metrics
//! for any type of operation using task-local storage, enabling accurate
//! per-task resource attribution even in concurrent scenarios.

use std::{
    collections::HashSet,
    future::Future,
    sync::{Arc, Mutex},
    time::Instant,
};

use tokio_metrics::TaskMonitor;

use crate::{task_id::TaskId, task_type::TaskType};

/// Get the current OS thread ID as a u64.
///
/// This uses platform-specific methods to get a unique identifier for the current thread.
pub(crate) fn current_thread_id() -> u64 {
    // std::thread::current().id() gives us a ThreadId, but we need to extract the numeric value.
    // We use a hash of the thread ID as a stable numeric identifier.
    use std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
    };

    let thread_id = std::thread::current().id();
    let mut hasher = DefaultHasher::new();
    thread_id.hash(&mut hasher);
    hasher.finish()
}

tokio::task_local! {
    /// Task-local storage for the current task context.
    ///
    /// This allows any code within a task execution to access the task's
    /// runtime context without explicit parameter passing. The context is
    /// automatically propagated to spawned tasks.
    pub static TASK_CONTEXT: Arc<TaskRuntimeContext>;
}

/// Snapshot of task metrics at a point in time.
///
/// These metrics are specific to the task tree being monitored and are not
/// affected by other concurrent tasks in the runtime.
#[derive(Debug, Clone)]
pub struct TaskMetrics {
    /// Total time the task spent executing (active work)
    pub total_busy_duration: std::time::Duration,

    /// Total time the task spent waiting/idle
    pub total_idle_duration: std::time::Duration,

    /// Number of times the task was polled
    pub total_poll_count: u64,

    /// Number of times the task was scheduled for execution
    pub total_scheduled_count: u64,

    /// Number of unique OS threads that executed this task
    pub unique_thread_count: usize,

    /// Set of OS thread IDs that executed this task
    pub thread_ids: HashSet<u64>,
}

impl TaskMetrics {
    /// Get the total busy duration in milliseconds for metrics export.
    pub fn busy_duration_ms(&self) -> f64 {
        self.total_busy_duration.as_secs_f64() * 1000.0
    }

    /// Get the total idle duration in milliseconds for metrics export.
    pub fn idle_duration_ms(&self) -> f64 {
        self.total_idle_duration.as_secs_f64() * 1000.0
    }

    /// Calculate the average time per poll in microseconds.
    pub fn avg_poll_duration_us(&self) -> Option<f64> {
        if self.total_poll_count == 0 {
            None
        } else {
            let total_duration = self.total_busy_duration + self.total_idle_duration;
            Some(total_duration.as_micros() as f64 / self.total_poll_count as f64)
        }
    }

    /// Calculate CPU utilization as a percentage (0-100).
    ///
    /// Returns an integer percentage where 100 means 100% CPU busy.
    pub fn cpu_utilization(&self) -> u8 {
        let total = self.total_busy_duration + self.total_idle_duration;
        if total.is_zero() {
            0
        } else {
            let ratio = self.total_busy_duration.as_secs_f64() / total.as_secs_f64();
            (ratio * 100.0).round() as u8
        }
    }

    /// Calculate CPU utilization as a floating point percentage (0.0-100.0).
    ///
    /// Returns a float where 100.0 means 100% CPU busy.
    pub fn cpu_utilization_f64(&self) -> f64 {
        let total = self.total_busy_duration + self.total_idle_duration;
        if total.is_zero() {
            0.0
        } else {
            let ratio = self.total_busy_duration.as_secs_f64() / total.as_secs_f64();
            ratio * 100.0
        }
    }
}

/// Context for tracking runtime metrics during a single task execution.
///
/// This uses `tokio-metrics::TaskMonitor` to track actual per-task metrics,
/// providing accurate resource attribution even when multiple tasks run
/// concurrently. The context is propagated via task-local storage.
///
/// Unlike the older `QueryRuntimeContext`, this is generic and can track
/// any type of operation (queries, dumps, compactions, etc).
///
/// # Concurrent Task Support
///
/// `TaskMonitor` tracks metrics for the specific task tree spawned for this
/// operation, isolating it from other concurrent tasks.
///
/// # Thread Safety
///
/// This type is `Clone` and uses `Arc` internally, making it safe to share
/// across async tasks.
#[derive(Clone)]
pub struct TaskRuntimeContext {
    /// Unique identifier for this task execution
    pub task_id: TaskId,

    /// Type of task being executed with metadata
    pub task_type: TaskType,

    /// Task monitor for collecting per-task metrics
    task_monitor: TaskMonitor,

    /// Wall-clock time when context was created
    start_time: Instant,

    /// Captured task metrics at task completion
    /// 
    /// TODO: Consider using RwLock if read contention becomes an issue
    /// OR consider atomic snapshot approach
    final_metrics: Arc<Mutex<Option<TaskMetrics>>>,

    /// Thread IDs observed during task execution (for parallelism tracking)
    thread_ids: Arc<Mutex<HashSet<u64>>>,
}

impl TaskRuntimeContext {
    /// Create a new runtime context for a task execution.
    ///
    /// This automatically generates a new task ID and creates a fresh
    /// `TaskMonitor` for tracking this task's metrics.
    pub fn new(task_type: TaskType) -> Self {
        Self {
            task_id: TaskId::new(),
            task_type,
            task_monitor: TaskMonitor::new(),
            start_time: Instant::now(),
            final_metrics: Arc::new(Mutex::new(None)),
            thread_ids: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Create a runtime context with a specific task ID.
    ///
    /// Useful when you want to use an existing task ID from another part of the system.
    pub fn with_task_id(task_id: TaskId, task_type: TaskType) -> Self {
        Self {
            task_id,
            task_type,
            task_monitor: TaskMonitor::new(),
            start_time: Instant::now(),
            final_metrics: Arc::new(Mutex::new(None)),
            thread_ids: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Record that the current OS thread is executing this task.
    ///
    /// Call this periodically during task execution to track which threads
    /// are being used. This helps identify parallelism and thread migration.
    fn record_current_thread(&self) {
        let thread_id = current_thread_id();
        self.thread_ids.lock().unwrap().insert(thread_id);
    }

    /// Execute a future within this task's monitoring context.
    ///
    /// This instruments the future with the task monitor and sets up task-local
    /// storage so that any code within the future (or tasks it spawns) can access
    /// this context.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let ctx = TaskRuntimeContext::new(TaskType::Dump {
    ///     dataset: "eth_blocks".to_string(),
    ///     block_range: (0, 1000),
    ///     records_extracted: None,
    /// });
    /// let result = ctx.scope(async {
    ///     // This code and any spawned tasks can access TASK_CONTEXT
    ///     extract_blocks().await
    /// }).await;
    /// ```
    pub async fn scope<F, R>(&self, future: F) -> R
    where
        F: Future<Output = R>,
    {
        let ctx = Arc::new(self.clone());
        let monitored = self.task_monitor.instrument(future);

        TASK_CONTEXT.scope(ctx, monitored).await
    }

    /// Get the current task context from task-local storage.
    ///
    /// Returns `None` if called outside of a `scope()` context.
    ///
    /// # Example
    ///
    /// ```ignore
    /// if let Some(ctx) = TaskRuntimeContext::try_current() {
    ///     tracing::info!(task_id = %ctx.task_id, "Processing task");
    /// }
    /// ```
    pub fn try_current() -> Option<Arc<TaskRuntimeContext>> {
        TASK_CONTEXT.try_with(|ctx| ctx.clone()).ok()
    }

    /// Get current task metrics from the monitor.
    ///
    /// Returns metrics accumulated since the context was created.
    /// Can be called multiple times to track progress.
    pub fn current_metrics(&self) -> TaskMetrics {
        // Record current thread before capturing metrics
        self.record_current_thread();

        // Use cumulative() to get a snapshot without blocking
        let cumulative = self.task_monitor.cumulative();

        // Capture thread tracking information
        let thread_ids = self.thread_ids.lock().unwrap().clone();
        let unique_thread_count = thread_ids.len();

        TaskMetrics {
            total_busy_duration: cumulative.total_poll_duration,
            total_idle_duration: cumulative.total_idle_duration,
            total_poll_count: cumulative.total_poll_count,
            total_scheduled_count: cumulative.total_scheduled_count,
            unique_thread_count,
            thread_ids,
        }
    }

    /// Finalize metrics collection and store final snapshot.
    ///
    /// This should be called after task execution completes. Subsequent calls
    /// to `final_metrics()` will return the captured snapshot.
    pub fn finalize_metrics(&self) {
        let metrics = self.current_metrics();
        *self.final_metrics.lock().unwrap() = Some(metrics);
    }

    /// Get the final metrics snapshot captured at task completion.
    ///
    /// Returns `None` if `finalize_metrics()` hasn't been called yet.
    pub fn final_metrics(&self) -> Option<TaskMetrics> {
        self.final_metrics.lock().unwrap().clone()
    }

    /// Get the wall-clock elapsed time since context creation.
    pub fn elapsed(&self) -> std::time::Duration {
        self.start_time.elapsed()
    }

    /// Get a reference to the task monitor for advanced usage.
    pub fn task_monitor(&self) -> &TaskMonitor {
        &self.task_monitor
    }
}

impl std::fmt::Debug for TaskRuntimeContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskRuntimeContext")
            .field("task_id", &self.task_id)
            .field("task_type", &self.task_type.name())
            .field("elapsed", &self.elapsed())
            .field(
                "has_final_metrics",
                &self.final_metrics.lock().unwrap().is_some(),
            )
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[tokio::test]
    async fn test_task_runtime_context_lifecycle() {
        let ctx = TaskRuntimeContext::new(TaskType::Generic {
            operation: "test".to_string(),
            metadata: HashMap::new(),
        });

        // Execute work within the context
        ctx.scope(async {
            // Access context from within
            let current = TaskRuntimeContext::try_current().expect("Should have context");
            assert_eq!(current.task_id, ctx.task_id);

            // Do some work
            for _ in 0..10 {
                tokio::task::yield_now().await;
            }
        })
        .await;

        // Check metrics were collected
        let metrics = ctx.current_metrics();
        assert!(metrics.total_poll_count > 0, "Should have recorded polls");
    }

    #[tokio::test]
    async fn test_concurrent_tasks() {
        let ctx1 = Arc::new(TaskRuntimeContext::new(TaskType::Dump {
            dataset: "test1".to_string(),
            block_range: (0, 100),
            records_extracted: None,
        }));
        let ctx2 = Arc::new(TaskRuntimeContext::new(TaskType::Dump {
            dataset: "test2".to_string(),
            block_range: (100, 200),
            records_extracted: None,
        }));

        let id1 = ctx1.task_id;
        let id2 = ctx2.task_id;

        // Run two tasks concurrently
        let (_, _) = tokio::join!(
            ctx1.scope(async move {
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                let current = TaskRuntimeContext::try_current().unwrap();
                assert_eq!(current.task_id, id1);
            }),
            ctx2.scope(async move {
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                let current = TaskRuntimeContext::try_current().unwrap();
                assert_eq!(current.task_id, id2);
            })
        );

        // Each should have independent metrics
        let metrics1 = ctx1.current_metrics();
        let metrics2 = ctx2.current_metrics();

        assert!(metrics1.total_poll_count > 0);
        assert!(metrics2.total_poll_count > 0);
    }

    #[tokio::test]
    async fn test_finalize_metrics() {
        let ctx = TaskRuntimeContext::new(TaskType::Compact {
            dataset: "test".to_string(),
            input_files: 10,
            output_files: 1,
        });

        assert!(ctx.final_metrics().is_none());

        ctx.scope(async {
            for _ in 0..5 {
                tokio::task::yield_now().await;
            }
        })
        .await;

        ctx.finalize_metrics();

        let final_metrics = ctx.final_metrics().expect("Should have final metrics");
        assert!(final_metrics.total_poll_count > 0);
    }
}

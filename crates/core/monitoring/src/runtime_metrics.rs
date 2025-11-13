//! Tokio runtime metrics collection and tracking.
//!
//! This module provides utilities for capturing and analyzing tokio runtime metrics
//! at both the global level and per-query level. It enables observability into
//! worker thread utilization, task scheduling, and resource consumption.

use std::time::Duration;

use tokio::runtime::Handle;

/// Snapshot of tokio runtime metrics at a specific point in time.
///
/// This captures the state of the entire tokio runtime, including all worker threads
/// and task scheduling statistics. Snapshots can be compared to calculate deltas
/// for per-query attribution.
#[derive(Debug, Clone)]
pub struct RuntimeMetricsSnapshot {
    /// Number of worker threads in the runtime
    pub workers_count: usize,

    /// Total number of times worker threads parked (aggregate across all workers)
    pub total_park_count: u64,

    /// Total busy duration across all workers (in nanoseconds)
    pub total_busy_duration_ns: u64,
}

impl RuntimeMetricsSnapshot {
    /// Capture current runtime metrics from the tokio runtime handle.
    ///
    /// This aggregates metrics from all worker threads to provide a complete
    /// picture of the runtime's state at this moment.
    pub fn capture(runtime_handle: &Handle) -> Self {
        let metrics = runtime_handle.metrics();
        let workers_count = metrics.num_workers();

        // Aggregate metrics across all workers
        let mut total_busy_duration_ns = 0u64;
        let mut total_park_count = 0u64;

        for worker_id in 0..workers_count {
            total_busy_duration_ns +=
                metrics.worker_total_busy_duration(worker_id).as_nanos() as u64;
            total_park_count += metrics.worker_park_count(worker_id);
        }

        Self {
            workers_count,
            total_park_count,
            total_busy_duration_ns,
        }
    }

    /// Calculate the delta between this snapshot and a previous one.
    ///
    /// This is useful for attributing resource consumption to a specific time window,
    /// such as a single query execution. Returns `None` if this snapshot appears to be
    /// older than the previous one (which would indicate incorrect usage).
    pub fn delta(&self, previous: &Self) -> Option<RuntimeMetricsDelta> {
        // Ensure we're calculating delta in the right direction
        if self.total_park_count < previous.total_park_count {
            return None;
        }

        Some(RuntimeMetricsDelta {
            park_count: self
                .total_park_count
                .saturating_sub(previous.total_park_count),
            busy_duration: Duration::from_nanos(
                self.total_busy_duration_ns
                    .saturating_sub(previous.total_busy_duration_ns),
            ),
        })
    }
}

/// Delta between two runtime metric snapshots.
///
/// Represents the change in runtime metrics over a specific time period,
/// such as a single query execution. All values are differences (end - start).
#[derive(Debug, Clone)]
pub struct RuntimeMetricsDelta {
    /// Number of times worker threads parked during this period
    pub park_count: u64,

    /// Total worker busy time during this period
    pub busy_duration: Duration,
}

impl RuntimeMetricsDelta {
    /// Get the busy duration as milliseconds (f64) for metrics export.
    pub fn busy_duration_ms(&self) -> f64 {
        self.busy_duration.as_secs_f64() * 1000.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_runtime_metrics_snapshot() {
        let handle = Handle::current();
        let snapshot = RuntimeMetricsSnapshot::capture(&handle);

        assert!(snapshot.workers_count > 0);
        // Park count is captured successfully (u64 is always >= 0, just verify capture worked)
        let _ = snapshot.total_park_count;
    }

    #[tokio::test]
    async fn test_runtime_metrics_delta() {
        let handle = Handle::current();

        let snapshot1 = RuntimeMetricsSnapshot::capture(&handle);

        // Do some work to change metrics
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }

        let snapshot2 = RuntimeMetricsSnapshot::capture(&handle);

        let delta = snapshot2.delta(&snapshot1).expect("Delta should be valid");

        // After yielding, busy time should have increased
        assert!(delta.busy_duration.as_nanos() > 0);
    }

    #[tokio::test]
    async fn test_delta_direction() {
        let handle = Handle::current();

        let snapshot1 = RuntimeMetricsSnapshot::capture(&handle);

        // Do enough work to ensure metrics change
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }

        let snapshot2 = RuntimeMetricsSnapshot::capture(&handle);

        // Correct direction should work
        assert!(snapshot2.delta(&snapshot1).is_some());

        // Incorrect direction should return None (only if metrics actually changed)
        if snapshot2.total_park_count > snapshot1.total_park_count {
            assert!(snapshot1.delta(&snapshot2).is_none());
        }
    }
}

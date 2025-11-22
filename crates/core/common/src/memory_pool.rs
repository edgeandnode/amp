//! Custom memory pool implementations for tiered memory management.

use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use datafusion::{
    common::Result,
    error::DataFusionError,
    execution::memory_pool::{
        FairSpillPool, GreedyMemoryPool, MemoryLimit, MemoryPool, MemoryReservation,
        TrackConsumersPool, UnboundedMemoryPool,
    },
};

pub enum MemoryPoolKind {
    Greedy,
    FairSpill,
}

/// If `max_mem_bytes` is zero, an unbounded memory pool is created.
pub fn make_memory_pool(kind: MemoryPoolKind, max_mem_bytes: usize) -> Arc<dyn MemoryPool> {
    let report_top_k_consumers = NonZeroUsize::new(3).unwrap();
    match kind {
        _ if max_mem_bytes == 0 => Arc::new(UnboundedMemoryPool::default()),
        MemoryPoolKind::FairSpill => Arc::new(TrackConsumersPool::new(
            FairSpillPool::new(max_mem_bytes),
            report_top_k_consumers,
        )),
        MemoryPoolKind::Greedy => Arc::new(TrackConsumersPool::new(
            GreedyMemoryPool::new(max_mem_bytes),
            report_top_k_consumers,
        )),
    }
}

fn format_oom_error(err: DataFusionError) -> DataFusionError {
    let single_line = err
        .to_string()
        .lines()
        .map(str::trim)
        .collect::<Vec<_>>()
        .join(" | ");

    DataFusionError::ResourcesExhausted(format!("OOM error: {}", single_line))
}

/// A tiered memory pool that enforces limits at two levels (e.g., global and per-query).
///
/// Both the parent and child pools track the same memory allocations, providing
/// hierarchical accounting. This allows enforcing both system-wide and per-query
/// memory limits simultaneously.
///
/// # Example Use Case
///
/// - Parent: Global system memory limit (e.g., 16GB)
/// - Child: Per-query memory limit (e.g., 2GB)
///
/// When a query attempts to allocate memory:
/// 1. Check if global limit allows it
/// 2. Check if per-query limit allows it
/// 3. Only succeed if both pools allow the allocation
///
/// # Peak Memory Tracking
///
/// The pool tracks the peak memory usage across all grow operations,
/// which can be retrieved via `peak_reserved()`. This is useful for
/// metrics and observability.
#[derive(Debug)]
pub struct TieredMemoryPool {
    parent: Arc<dyn MemoryPool>,
    child: Arc<dyn MemoryPool>,
    peak_reserved: AtomicUsize,
}

impl TieredMemoryPool {
    /// Create a new tiered memory pool with parent and child pools.
    ///
    /// # Arguments
    ///
    /// * `parent` - The parent pool (typically global/system-wide)
    /// * `child` - The child pool (typically per-query or per-context)
    pub fn new(parent: Arc<dyn MemoryPool>, child: Arc<dyn MemoryPool>) -> Self {
        Self {
            parent,
            child,
            peak_reserved: AtomicUsize::new(0),
        }
    }

    /// Returns the peak memory reserved across all grow operations.
    ///
    /// This tracks the maximum value of `reserved()` seen during the lifetime
    /// of this pool, useful for metrics and observability.
    pub fn peak_reserved(&self) -> usize {
        self.peak_reserved.load(Ordering::SeqCst)
    }

    /// Update peak reserved if current reservation is higher
    fn update_peak(&self) {
        let current = self.reserved();
        self.peak_reserved.fetch_max(current, Ordering::SeqCst);
    }
}

impl MemoryPool for TieredMemoryPool {
    fn register(&self, consumer: &datafusion::execution::memory_pool::MemoryConsumer) {
        self.parent.register(consumer);
        self.child.register(consumer);
    }

    fn unregister(&self, consumer: &datafusion::execution::memory_pool::MemoryConsumer) {
        // Unregister in reverse order
        self.child.unregister(consumer);
        self.parent.unregister(consumer);
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        // Both must succeed (infallible operation)
        self.parent.grow(reservation, additional);
        self.child.grow(reservation, additional);
        self.update_peak();
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        // Shrink in reverse order
        self.child.shrink(reservation, shrink);
        self.parent.shrink(reservation, shrink);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        // Try parent first (global limit)
        match self.parent.try_grow(reservation, additional) {
            Ok(()) => {
                // If parent succeeds, try child (per-query limit)
                match self.child.try_grow(reservation, additional) {
                    Ok(()) => {
                        self.update_peak();
                        Ok(())
                    }
                    Err(e) => {
                        // Rollback parent allocation on child failure
                        self.parent.shrink(reservation, additional);
                        Err(format_oom_error(e))
                    }
                }
            }
            Err(e) => Err(format_oom_error(e)),
        }
    }

    fn reserved(&self) -> usize {
        // Return minimum (most restrictive accounting)
        // This represents the effective reserved memory under the stricter limit
        self.parent.reserved().min(self.child.reserved())
    }

    fn memory_limit(&self) -> MemoryLimit {
        use MemoryLimit::*;

        // Return the most restrictive limit
        match (self.parent.memory_limit(), self.child.memory_limit()) {
            (Finite(p), Finite(c)) => Finite(p.min(c)),
            (Finite(p), _) => Finite(p),
            (_, Finite(c)) => Finite(c),
            (Infinite, Infinite) => Infinite,
            _ => Unknown,
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer};

    use super::*;

    #[test]
    fn test_tiered_pool_both_limits() {
        let parent = Arc::new(GreedyMemoryPool::new(1000)) as Arc<dyn MemoryPool>;
        let child = Arc::new(GreedyMemoryPool::new(500)) as Arc<dyn MemoryPool>;
        let tiered =
            Arc::new(TieredMemoryPool::new(parent.clone(), child.clone())) as Arc<dyn MemoryPool>;

        let mut reservation = MemoryConsumer::new("test").register(&tiered);

        // Should succeed: under both limits
        reservation.try_grow(400).unwrap();
        assert_eq!(reservation.size(), 400);
        assert_eq!(parent.reserved(), 400);
        assert_eq!(child.reserved(), 400);

        // Should fail: exceeds child limit (500)
        let result = reservation.try_grow(200);
        assert!(result.is_err());
        assert_eq!(reservation.size(), 400); // Unchanged
        assert_eq!(parent.reserved(), 400); // Rolled back
        assert_eq!(child.reserved(), 400);
    }

    #[test]
    fn test_tiered_pool_parent_limit_stricter() {
        let parent = Arc::new(GreedyMemoryPool::new(300)) as Arc<dyn MemoryPool>;
        let child = Arc::new(GreedyMemoryPool::new(1000)) as Arc<dyn MemoryPool>;
        let tiered =
            Arc::new(TieredMemoryPool::new(parent.clone(), child.clone())) as Arc<dyn MemoryPool>;

        let mut reservation = MemoryConsumer::new("test").register(&tiered);

        // Should succeed: under both limits
        reservation.try_grow(250).unwrap();
        assert_eq!(reservation.size(), 250);

        // Should fail: exceeds parent limit (300)
        let result = reservation.try_grow(100);
        assert!(result.is_err());
        assert_eq!(reservation.size(), 250); // Unchanged
    }

    #[test]
    fn test_tiered_pool_shrink() {
        let parent = Arc::new(GreedyMemoryPool::new(1000)) as Arc<dyn MemoryPool>;
        let child = Arc::new(GreedyMemoryPool::new(500)) as Arc<dyn MemoryPool>;
        let tiered =
            Arc::new(TieredMemoryPool::new(parent.clone(), child.clone())) as Arc<dyn MemoryPool>;

        let mut reservation = MemoryConsumer::new("test").register(&tiered);

        reservation.try_grow(400).unwrap();
        assert_eq!(parent.reserved(), 400);
        assert_eq!(child.reserved(), 400);

        reservation.shrink(200);
        assert_eq!(reservation.size(), 200);
        assert_eq!(parent.reserved(), 200);
        assert_eq!(child.reserved(), 200);
    }

    #[test]
    fn test_tiered_pool_memory_limit() {
        let parent = Arc::new(GreedyMemoryPool::new(1000)) as Arc<dyn MemoryPool>;
        let child = Arc::new(GreedyMemoryPool::new(500)) as Arc<dyn MemoryPool>;
        let tiered = TieredMemoryPool::new(parent, child);

        // Should return the smaller limit
        match tiered.memory_limit() {
            MemoryLimit::Finite(limit) => assert_eq!(limit, 500),
            _ => panic!("Expected finite limit"),
        }
    }

    #[test]
    fn test_tiered_pool_reserved() {
        let parent = Arc::new(GreedyMemoryPool::new(1000)) as Arc<dyn MemoryPool>;
        let child = Arc::new(GreedyMemoryPool::new(500)) as Arc<dyn MemoryPool>;
        let tiered_arc =
            Arc::new(TieredMemoryPool::new(parent.clone(), child.clone())) as Arc<dyn MemoryPool>;

        let mut reservation = MemoryConsumer::new("test").register(&tiered_arc);

        reservation.try_grow(300).unwrap();

        // Both should report 300
        assert_eq!(parent.reserved(), 300);
        assert_eq!(child.reserved(), 300);

        // Tiered should report minimum (which is 300 in both cases)
        let tiered2 = TieredMemoryPool::new(parent, child);
        assert_eq!(tiered2.reserved(), 300);
    }

    #[test]
    fn test_tiered_pool_peak_tracking() {
        let parent = Arc::new(GreedyMemoryPool::new(1000)) as Arc<dyn MemoryPool>;
        let child = Arc::new(GreedyMemoryPool::new(500)) as Arc<dyn MemoryPool>;
        let tiered = Arc::new(TieredMemoryPool::new(parent, child));

        let tiered_for_registration = tiered.clone() as Arc<dyn MemoryPool>;
        let mut reservation = MemoryConsumer::new("test").register(&tiered_for_registration);

        // Initially peak should be 0
        assert_eq!(tiered.peak_reserved(), 0);

        // Grow to 300
        reservation.try_grow(300).unwrap();
        assert_eq!(tiered.peak_reserved(), 300);

        // Grow to 450
        reservation.try_grow(150).unwrap();
        assert_eq!(tiered.peak_reserved(), 450);

        // Shrink to 200 - peak should stay at 450
        reservation.shrink(250);
        assert_eq!(reservation.size(), 200);
        assert_eq!(tiered.peak_reserved(), 450);

        // Grow to 350 - peak should stay at 450 (not exceeded)
        reservation.try_grow(150).unwrap();
        assert_eq!(tiered.peak_reserved(), 450);

        // Grow to 500 - peak should update to 500
        reservation.try_grow(150).unwrap();
        assert_eq!(tiered.peak_reserved(), 500);
    }
}

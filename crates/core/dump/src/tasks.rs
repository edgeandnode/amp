//! Tokio tasks utilities

use std::future::Future;

use monitoring::logging;
use tokio::task::{JoinError, JoinSet};

/// A wrapper around [`JoinSet`] that implements fail-fast semantics.
///
/// When any task fails (or panics), all remaining tasks are immediately aborted.
/// This is useful for groups of interdependent tasks where one failure invalidates the others.
///
/// ## Behavior
/// - **Success**: All tasks must complete successfully
/// - **Failure**: First error/panic aborts all remaining tasks  
/// - **Cancellation**: Individual cancellations are logged but don't abort others
/// - **Drop**: Aborts all tasks if dropped
///
/// All the tasks must have the same return type `T`.
#[derive(Default)]
pub struct FailFastJoinSet<T>(JoinSet<T>);

impl<T> FailFastJoinSet<T> {
    /// Creates a new empty fail-fast join set
    pub fn new() -> Self {
        Self(Default::default())
    }

    /// Spawn a task in the set
    pub fn spawn<F>(&mut self, task: F)
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        self.0.spawn(task);
    }
}

impl<T, E> FailFastJoinSet<Result<T, E>>
where
    T: Send + 'static,
    E: Send + 'static,
{
    /// Waits for all tasks to complete with fail-fast behavior.
    ///
    /// Returns `Ok(())` only if all tasks succeed. On first failure:
    /// - Aborts remaining tasks
    /// - Waits for clean shutdown
    /// - Returns the error
    ///
    /// Task cancellations are logged but don't trigger aborts
    pub async fn try_wait_all(&mut self) -> Result<(), TryWaitAllError<E>> {
        while let Some(result) = self.0.join_next().await {
            match result {
                // One task succeeded, wait for the rest to finish
                Ok(Ok(_)) => continue,
                // One task returned an error, abort the rest of the tasks and
                // wait for them to stop, then return the error
                Ok(Err(err)) => {
                    self.0.shutdown().await;
                    return Err(TryWaitAllError::Error(err));
                }
                // One of the tasks was cancelled, continue
                Err(err) if err.is_cancelled() => {
                    tracing::trace!("task {} was cancelled", err.id());
                    continue;
                }
                // One of the tasks panicked, abort the rest of the tasks and
                // wait for them to stop, then return the error
                Err(err) => {
                    tracing::error!(error = %err, error_source = logging::error_source(&err), "task {} panicked", err.id());
                    self.0.shutdown().await;
                    return Err(TryWaitAllError::Panic(err));
                }
            }
        }
        Ok(())
    }
}

/// The error type returned by [`FailFastJoinSet::try_wait_all`]
///
/// This enum represents the two failure modes when waiting for parallel tasks:
/// either a task explicitly returned an error, or a task panicked unexpectedly.
/// In both cases, all remaining tasks are aborted before returning.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub enum TryWaitAllError<E> {
    /// A task returned an error through its Result type
    ///
    /// This occurs when one of the spawned tasks completes with `Err(e)`.
    /// The fail-fast behavior ensures remaining tasks are immediately aborted.
    ///
    /// The wrapped error `E` is the original error returned by the failed task,
    /// preserving full error context and source chain for debugging.
    Error(E),

    /// A task panicked during execution
    ///
    /// This occurs when a spawned task panics rather than returning normally.
    /// Panics typically indicate programming bugs such as:
    /// - Array index out of bounds
    /// - Unwrap on None/Err
    /// - Explicit panic!() calls
    /// - Stack overflow
    ///
    /// The wrapped `JoinError` contains panic information including
    /// the panic payload if available. Unlike explicit errors, panics
    /// cannot be easily recovered from and usually indicate a bug.
    Panic(JoinError),
}

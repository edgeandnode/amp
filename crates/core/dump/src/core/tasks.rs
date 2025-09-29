//! Tokio tasks utilities

use std::future::Future;

use common::BoxError;
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

impl<T> FailFastJoinSet<Result<T, BoxError>>
where
    T: Send + 'static,
{
    /// Waits for all tasks to complete with fail-fast behavior.
    ///
    /// Returns `Ok(())` only if all tasks succeed. On first failure:
    /// - Aborts remaining tasks
    /// - Waits for clean shutdown  
    /// - Returns the error
    ///
    /// Task cancellations are logged but don't trigger aborts
    pub async fn try_wait_all(&mut self) -> Result<(), TryWaitAllError<BoxError>> {
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
                    tracing::error!(error=?err, "task {} panicked", err.id());
                    self.0.shutdown().await;
                    return Err(TryWaitAllError::Panic(err));
                }
            }
        }
        Ok(())
    }
}

/// The error type returned by [`FailFastJoinSet::try_wait_all`]
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub enum TryWaitAllError<E> {
    /// A task returned an error
    Error(E),
    /// A task panicked
    Panic(JoinError),
}

impl TryWaitAllError<BoxError> {
    /// Convert the error into a [`BoxError`]
    ///
    /// This method overcomes a limitation in Rust's trait system where `Box<dyn Error + Send + Sync>`
    /// cannot always be proven to implement `Error` in generic contexts, even though it does.
    pub fn into_box_error(self) -> BoxError {
        match self {
            TryWaitAllError::Error(err) => err,
            TryWaitAllError::Panic(err) => err.into(),
        }
    }
}

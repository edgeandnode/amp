pub mod algorithm;
pub mod collector;
pub mod compactor;
pub mod error;
pub mod plan;

use std::{
    fmt::{Debug, Display},
    sync::Arc,
    time::Duration,
};

use common::{Timestamp, catalog::physical::PhysicalTable};
use futures::{
    FutureExt, TryFutureExt,
    future::{BoxFuture, ok as ready_ok},
};
use tokio::task::JoinHandle;

pub use crate::compaction::{
    algorithm::{CompactionAlgorithm, SegmentSizeLimit},
    error::{CollectionResult, CollectorError, CompactionResult, CompactorError},
};
use crate::{
    WriterProperties,
    compaction::{collector::Collector, compactor::Compactor, error::CompactionErrorExt},
};

/// Duration collector must wait prior to deleting files
pub const FILE_LOCK_DURATION: Duration = Duration::from_secs(60 * 60); // 1 hour

pub struct NozzleCompactor {
    compaction_task: CompactionTask,
    deletion_task: DeletionTask,
}

impl NozzleCompactor {
    pub fn start(table: Arc<PhysicalTable>, opts: Arc<WriterProperties>) -> Self {
        NozzleCompactor {
            compaction_task: Compactor::start(&table, &opts),
            deletion_task: Collector::start(&table, &opts),
        }
    }

    pub fn try_run(&mut self) {
        self.compaction_task.try_run();

        self.deletion_task.try_run();
    }

    pub fn compaction_completed(&self) -> bool {
        self.compaction_task.is_finished()
    }

    pub fn deletion_completed(&self) -> bool {
        self.deletion_task.is_finished()
    }

    /// Block until the current compaction task is finished
    /// and then trigger another compaction
    pub async fn run_compaction(&mut self) {
        self.compaction_task.join_current_then_spawn_new().await;
    }

    /// Block until the current deletion task is finished
    /// and then trigger another deletion
    pub async fn run_deletion(&mut self) {
        self.deletion_task.join_current_then_spawn_new().await;
    }
}

pub type CompactionTask = NozzleCompactorTask<Compactor>;
pub type DeletionTask = NozzleCompactorTask<Collector>;

pub struct NozzleCompactorTask<T: NozzleCompactorTaskType> {
    task: JoinHandle<Result<T, T::Error>>,
    table: Arc<PhysicalTable>,
    opts: Arc<WriterProperties>,
    previous: Option<Timestamp>,
}

impl<T: NozzleCompactorTaskType> NozzleCompactorTask<T> {
    pub fn abort(&self) {
        self.task.abort();
    }

    pub fn elapsed_since_previous(&self) -> Option<Duration> {
        self.previous.map(|previous| {
            let now = Timestamp::now();
            now.0.saturating_sub(previous.0)
        })
    }

    pub fn is_finished(&self) -> bool {
        self.task.is_finished()
    }

    fn is_ready(&self) -> bool {
        self.is_finished()
            && self
                .elapsed_since_previous()
                // if None, consider it ready
                .map_or(true, |elapsed| elapsed >= T::interval(&self.opts))
            && T::active(&self.opts)
    }

    pub async fn join_current_then_spawn_new(&mut self) {
        let task = &mut self.task;

        let inner = match task
            .map_err(|join_err| T::handle_error(&self.table, &mut self.opts, join_err))
            .await
        {
            Ok(Ok(inner)) | Err(inner) => {
                self.previous = Some(Timestamp::now());
                inner
            }
            Ok(Err(err)) => T::handle_error(&self.table, &mut self.opts, err),
        };
        self.task = tokio::spawn(inner.run());
    }

    fn try_run(&mut self) {
        if self.is_ready() {
            self.join_current_then_spawn_new()
                .now_or_never()
                .expect("We already checked is_finished");
        }
    }
}

pub trait NozzleCompactorTaskType: Debug + Display + Sized + Send + 'static {
    type Error: CompactionErrorExt;

    fn new(table: &Arc<PhysicalTable>, opts: &Arc<WriterProperties>) -> Self;

    /// Run the task
    fn run<'a>(self) -> BoxFuture<'a, Result<Self, Self::Error>>;

    fn interval(opts: &Arc<WriterProperties>) -> Duration;

    fn active(opts: &Arc<WriterProperties>) -> bool;

    fn deactivate(opts: &mut Arc<WriterProperties>);

    /// Handle errors from the previous run
    ///
    /// If the error is recoverable, return `self` to retry
    #[tracing::instrument(skip_all, fields(table = table.table_name()))]
    fn handle_error(
        table: &Arc<PhysicalTable>,
        opts: &mut Arc<WriterProperties>,
        err: impl Into<<Self as NozzleCompactorTaskType>::Error>,
    ) -> Self {
        let this = Self::new(table, opts);
        let err = err.into();
        if err.is_cancellation() {
            Self::deactivate(opts);
            tracing::info!("{this:?} was cancelled");
            return this;
        } else if err.is_debug() {
            tracing::debug!("{err}");
            this
        } else if err.is_informational() {
            tracing::info!("Informational error occurred in {this}: {err}");
            this
        } else if err.is_recoverable() {
            tracing::warn!("Recoverable error occurred in {this}: {err}");
            this
        } else {
            panic!("Unrecoverable error occurred in {this}: {err}");
        }
    }

    #[tracing::instrument(skip_all, fields(table = table.table_name()))]
    fn start(
        table: &Arc<PhysicalTable>,
        opts: &Arc<WriterProperties>,
    ) -> NozzleCompactorTask<Self> {
        let task = tokio::spawn(ready_ok(Self::new(table, opts)));

        let mut this = NozzleCompactorTask {
            task,
            table: Arc::clone(table),
            opts: Arc::clone(opts),
            previous: None,
        };

        this.try_run();

        this
    }
}

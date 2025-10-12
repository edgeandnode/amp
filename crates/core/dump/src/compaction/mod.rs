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

use common::{ParquetFooterCache, Timestamp, catalog::physical::PhysicalTable};
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
    metrics::MetricsRegistry,
};

/// Duration collector must wait prior to deleting files
pub const FILE_LOCK_DURATION: Duration = Duration::from_secs(60 * 60); // 1 hour

pub struct AmpCompactor {
    compaction_task: CompactionTask,
    deletion_task: DeletionTask,
}

impl AmpCompactor {
    pub fn start(
        table: Arc<PhysicalTable>,
        cache: ParquetFooterCache,
        opts: Arc<WriterProperties>,
        metrics: Option<Arc<MetricsRegistry>>,
    ) -> Self {
        AmpCompactor {
            compaction_task: Compactor::start(&table, &cache, &opts, &metrics),
            deletion_task: Collector::start(&table, &cache, &opts, &metrics),
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

pub type CompactionTask = AmpCompactorTask<Compactor>;
pub type DeletionTask = AmpCompactorTask<Collector>;

pub struct AmpCompactorTask<T: AmpCompactorTaskType> {
    task: JoinHandle<Result<T, T::Error>>,
    table: Arc<PhysicalTable>,
    cache: ParquetFooterCache,
    opts: Arc<WriterProperties>,
    metrics: Option<Arc<MetricsRegistry>>,
    previous: Option<Timestamp>,
}

impl<T: AmpCompactorTaskType> AmpCompactorTask<T> {
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
                .is_none_or(|elapsed| elapsed >= T::interval(&self.opts))
            && T::active(&self.opts)
    }

    pub async fn join_current_then_spawn_new(&mut self) {
        let task = &mut self.task;

        let inner = match task
            .map_err(|join_err| {
                T::handle_error(
                    &self.table,
                    &self.cache,
                    &mut self.opts,
                    &self.metrics,
                    join_err,
                )
            })
            .await
        {
            Ok(Ok(inner)) | Err(inner) => {
                self.previous = Some(Timestamp::now());
                inner
            }
            Ok(Err(err)) => {
                T::handle_error(&self.table, &self.cache, &mut self.opts, &self.metrics, err)
            }
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

pub trait AmpCompactorTaskType: Debug + Display + Sized + Send + 'static {
    type Error: CompactionErrorExt;

    fn new(
        table: &Arc<PhysicalTable>,
        cache: &ParquetFooterCache,
        opts: &Arc<WriterProperties>,
        metrics: &Option<Arc<MetricsRegistry>>,
    ) -> Self;

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
        cache: &ParquetFooterCache,
        opts: &mut Arc<WriterProperties>,
        metrics: &Option<Arc<MetricsRegistry>>,
        err: impl Into<<Self as AmpCompactorTaskType>::Error>,
    ) -> Self {
        let this = Self::new(table, cache, opts, metrics);
        let err = err.into();
        if err.is_cancellation() {
            Self::deactivate(opts);
            tracing::info!("{this:?} was cancelled");
            this
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
        cache: &ParquetFooterCache,
        opts: &Arc<WriterProperties>,
        metrics: &Option<Arc<MetricsRegistry>>,
    ) -> AmpCompactorTask<Self> {
        let task = tokio::spawn(ready_ok(Self::new(table, cache, opts, metrics)));

        let mut this = AmpCompactorTask {
            task,
            table: Arc::clone(table),
            cache: cache.clone(),
            opts: Arc::clone(opts),
            previous: None,
            metrics: metrics.clone(),
        };

        this.try_run();

        this
    }
}

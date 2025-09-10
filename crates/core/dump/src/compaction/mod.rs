pub mod collector;
pub mod compactor;
pub mod error;
pub mod group;
pub mod size;

use std::{
    fmt::{Debug, Display, Formatter},
    sync::Arc,
    time::Duration,
};

use common::{
    Timestamp, catalog::physical::PhysicalTable,
    parquet::file::properties::WriterProperties as ParquetWriterProperties,
};
use futures::{FutureExt, TryFutureExt, future::BoxFuture};
use tokio::task::JoinHandle;

use crate::compaction::{collector::Collector, compactor::Compactor, error::CompactionErrorExt};
pub use crate::compaction::{
    error::{CompactorError, CompactionResult, CollectorError, DeletionResult},
    size::{SegmentSize, SegmentSizeLimit},
};

/// Duration collector must wait prior to deleting files
pub const FILE_LOCK_DURATION: Duration = Duration::from_secs(60 * 60); // 1 hour

#[derive(Debug, Clone)]
pub struct CompactionProperties {
    pub active: bool,
    pub compactor_interval: Duration,
    pub collector_interval: Duration,
    pub metadata_concurrency: usize,
    pub write_concurrency: usize,
    pub parquet_writer_props: ParquetWriterProperties,
    pub size_limit: SegmentSizeLimit,
}

impl Display for CompactionProperties {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            " {{ {active}, {compactor_interval}, {collector_interval}, {metadata_concurrency}, {write_concurrency}, {size_limit} }}",
            active = format!("active: {}", self.active),
            compactor_interval = format!("compactor_interval: {:?}", self.compactor_interval),
            collector_interval = format!("collector_interval: {:?}", self.collector_interval),
            metadata_concurrency = format!("metadata_concurrency: {}", self.metadata_concurrency),
            write_concurrency = format!("write_concurrency: {}", self.write_concurrency),
            size_limit = format!("size_limit: {}", self.size_limit),
        )
    }
}
pub struct NozzleCompactor {
    compaction_task: CompactionTask,
    deletion_task: DeletionTask,
}

impl NozzleCompactor {
    pub fn start(table: Arc<PhysicalTable>, opts: Arc<CompactionProperties>) -> Self {
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
        self.compaction_task.spawn().await;
    }

    /// Block until the current deletion task is finished
    /// and then trigger another deletion
    pub async fn run_deletion(&mut self) {
        self.deletion_task.spawn().await;
    }
}

pub type CompactionTask = NozzleCompactorTask<Compactor>;
pub type DeletionTask = NozzleCompactorTask<Collector>;

pub struct NozzleCompactorTask<T: NozzleCompactorTaskType> {
    task: JoinHandle<Result<T, T::Error>>,
    table: Arc<PhysicalTable>,
    opts: Arc<CompactionProperties>,
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
            && self.opts.active
    }

    pub async fn spawn(&mut self) {
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
            self.spawn()
                .now_or_never()
                .expect("We already checked is_finished");
        }
    }
}

pub trait NozzleCompactorTaskType: Debug + Display + Sized + Send + 'static {
    type Error: CompactionErrorExt;

    fn new(table: &Arc<PhysicalTable>, opts: &Arc<CompactionProperties>) -> Self;

    /// Run the task
    fn run<'a>(self) -> BoxFuture<'a, Result<Self, Self::Error>>;

    fn interval(opts: &Arc<CompactionProperties>) -> Duration;

    /// Handle errors from the previous run
    ///
    /// If the error is recoverable, return `self` to retry
    fn handle_error(
        table: &Arc<PhysicalTable>,
        opts: &mut Arc<CompactionProperties>,
        err: impl Into<<Self as NozzleCompactorTaskType>::Error>,
    ) -> Self {
        let this = Self::new(table, opts);
        let err = err.into();
        if err.is_cancellation() {
            let opts = Arc::make_mut(opts);
            opts.active = false;
            tracing::warn!("{this:?} was cancelled");
            return this;
        } else if err.is_recoverable() {
            tracing::warn!("Recoverable error occurred in {this}: {err}");
            this
        } else {
            panic!("Unrecoverable error occurred in {this}: {err}");
        }
    }

    fn start(
        table: &Arc<PhysicalTable>,
        opts: &Arc<CompactionProperties>,
    ) -> NozzleCompactorTask<Self> {
        let task = tokio::spawn(Self::new(table, opts).run());
        NozzleCompactorTask {
            task,
            table: Arc::clone(table),
            opts: Arc::clone(opts),
            previous: None,
        }
    }
}

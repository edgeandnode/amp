pub mod algorithm;
pub mod collector;
pub mod compactor;
pub mod error;
mod overflow;
mod plan;
pub mod size;

use std::{
    fmt::{Debug, Display, Formatter},
    ops::Deref,
    sync::Arc,
    time::Duration,
};

use alloy::transports::BoxFuture;
use common::{
    Timestamp,
    catalog::physical::PhysicalTable,
    config::{CompactionConfig, CompactionTaskConfig as TaskConfig},
};
use futures::{FutureExt, TryFutureExt, future};
use tokio::task::JoinHandle;

pub use crate::compaction::{
    algorithm::{CompactionAlgorithm, TestResult},
    error::{CollectorError, CompactionResult, CompactorError, DeletionResult},
    overflow::Overflow,
    size::{Generation, SegmentSize, SegmentSizeLimit},
};
use crate::{
    ParquetWriterProperties,
    compaction::{collector::Collector, compactor::Compactor, error::CompactionErrorExt},
};

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
            && T::active(&self.opts)
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

pub trait NozzleCompactorTaskType: Debug + Display + Send + Sized + 'static {
    type Error: CompactionErrorExt;

    fn new(table: &Arc<PhysicalTable>, opts: &Arc<CompactionProperties>) -> Self;

    /// Run the task
    fn run(self) -> BoxFuture<'static, Result<Self, Self::Error>>;

    fn interval(opts: &Arc<CompactionProperties>) -> Duration;

    fn active(opts: &Arc<CompactionProperties>) -> bool;

    fn deactivate(opts: &mut Arc<CompactionProperties>);

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
            Self::deactivate(opts);
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
    ) -> NozzleCompactorTask<Self>
    where
        Self: Send,
    {
        let task = tokio::spawn(future::ok(Self::new(table, opts)));
        NozzleCompactorTask {
            task,
            table: Arc::clone(table),
            opts: Arc::clone(opts),
            previous: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CompactionProperties {
    pub compactor: CompactorProperties,
    pub collector: CollectorProperties,
}

impl Display for CompactionProperties {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ compactor: {}, collector: {} }}",
            self.compactor, self.collector
        )
    }
}

#[derive(Debug, Clone)]
pub struct CompactorProperties {
    pub active: bool,
    pub algorithm: CompactionAlgorithm,
    pub min_interval: Duration,
    pub metadata_concurrency: usize,
    pub write_concurrency: usize,
    pub writer: ParquetWriterProperties,
}

impl CompactorProperties {
    fn fallback(writer: ParquetWriterProperties) -> Self {
        CompactorProperties {
            active: false,
            algorithm: CompactionAlgorithm {
                base_cooldown: Duration::from_secs(60),
                upper_bound: SegmentSizeLimit::default_bounded(),
                lower_bound: SegmentSizeLimit::default(),
            },
            min_interval: Duration::ZERO,
            metadata_concurrency: 4,
            write_concurrency: 1,
            writer,
        }
    }
}

impl CompactionProperties {
    pub fn from_config(config: &CompactionConfig, writer: &ParquetWriterProperties) -> Self {
        let compactor = if let TaskConfig::Compactor {
            active,
            ref algorithm,
            min_interval,
            metadata_concurrency,
            write_concurrency,
        } = config.compactor
        {
            CompactorProperties {
                active,
                algorithm: CompactionAlgorithm::from(algorithm),
                min_interval,
                metadata_concurrency,
                write_concurrency,
                writer: writer.clone(),
            }
        } else {
            CompactorProperties::fallback(writer.clone())
        };
        let collector = if let TaskConfig::Collector {
            active,
            min_interval,
            file_lock_timeout,
        } = config.collector
        {
            CollectorProperties {
                active,
                min_interval,
                file_lock_timeout: file_lock_timeout.into(),
            }
        } else {
            CollectorProperties::default()
        };

        CompactionProperties {
            compactor,
            collector,
        }
    }
}

impl Display for CompactorProperties {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ active: {}, algorithm: {}, min_interval: {:?}, metadata_concurrency: {}, write_concurrency: {} }}",
            self.active,
            self.algorithm,
            self.min_interval,
            self.metadata_concurrency,
            self.write_concurrency
        )
    }
}

#[derive(Debug, Clone)]
pub struct CollectorProperties {
    pub active: bool,
    pub file_lock_timeout: FileLockTimeout,
    pub min_interval: Duration,
}

impl Display for CollectorProperties {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ active: {}, file_lock_timeout: {}, min_interval: {:?} }}",
            self.active, self.file_lock_timeout, self.min_interval
        )
    }
}

impl Default for CollectorProperties {
    fn default() -> Self {
        CollectorProperties {
            active: false,
            file_lock_timeout: FileLockTimeout::default(),
            min_interval: Duration::ZERO,
        }
    }
}

/// Timeout duration for file lock during deletion
/// Minimum value is 100 milliseconds
/// Default is 1 hour
#[derive(Debug, Clone)]
pub struct FileLockTimeout(Duration);

impl From<Duration> for FileLockTimeout {
    fn from(value: Duration) -> Self {
        FileLockTimeout(value.max(Duration::from_millis(100)))
    }
}

impl Default for FileLockTimeout {
    fn default() -> Self {
        FileLockTimeout(Duration::from_secs(60 * 60)) // 1 hour
    }
}

impl Deref for FileLockTimeout {
    type Target = Duration;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Display for FileLockTimeout {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

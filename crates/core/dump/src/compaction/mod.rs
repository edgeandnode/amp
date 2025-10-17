mod algorithm;
mod collector;
mod compactor;
mod error;
mod plan;

use std::{
    error::Error,
    fmt::{Display, Formatter, Result as FmtResult},
    sync::{Arc, atomic::Ordering::SeqCst},
};

pub use algorithm::{CompactionAlgorithm, SegmentSizeLimit};
pub use collector::{Collector, CollectorProperties};
use common::{ParquetFooterCache, Timestamp, catalog::physical::PhysicalTable};
pub use compactor::{Compactor, CompactorProperties};
use error::{CollectionResult, CollectorError, CompactionResult, CompactorError};
use futures::FutureExt;
use tokio::task::{JoinError, JoinHandle};

use crate::{WriterProperties, metrics::MetricsRegistry};

pub type TaskResult<T> = Result<T, TaskError>;

#[derive(Debug)]
pub enum TaskError {
    CompactionError(CompactorError),
    CollectionError(CollectorError),
    JoinError(JoinError),
}

impl Display for TaskError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            TaskError::CompactionError(e) => e.fmt(f),
            TaskError::CollectionError(e) => e.fmt(f),
            TaskError::JoinError(e) => e.fmt(f),
        }
    }
}

impl Error for TaskError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            TaskError::CompactionError(e) => e.source(),
            TaskError::CollectionError(e) => e.source(),
            TaskError::JoinError(e) => e.source(),
        }
    }
}

impl From<CollectorError> for TaskError {
    fn from(err: CollectorError) -> Self {
        TaskError::CollectionError(err)
    }
}

impl From<CompactorError> for TaskError {
    fn from(err: CompactorError) -> Self {
        TaskError::CompactionError(err)
    }
}

impl From<JoinError> for TaskError {
    fn from(err: JoinError) -> Self {
        TaskError::JoinError(err)
    }
}

#[derive(Clone, Debug)]
pub struct InnerTask {
    pub compactor: Compactor,
    pub collector: Collector,
    pub props: Arc<WriterProperties>,
    pub table: Arc<PhysicalTable>,
    pub metrics: Option<Arc<MetricsRegistry>>,
    pub previous_collection: Timestamp,
    pub previous_compaction: Timestamp,
}

impl InnerTask {
    pub fn new(
        table: &Arc<PhysicalTable>,
        cache: ParquetFooterCache,
        props: &Arc<WriterProperties>,
        metrics: Option<Arc<MetricsRegistry>>,
    ) -> Self {
        let compactor = Compactor::new(table, cache, props, &metrics);
        let collector = Collector::new(table, props, &metrics);
        let props = Arc::clone(props);
        let table = Arc::clone(table);
        let now = Timestamp::now();
        let previous_collection = now;
        let previous_compaction = now;

        InnerTask {
            compactor,
            collector,
            props,
            table,
            metrics,
            previous_collection,
            previous_compaction,
        }
    }

    pub fn start(
        table: &Arc<PhysicalTable>,
        cache: ParquetFooterCache,
        props: &Arc<WriterProperties>,
        metrics: Option<Arc<MetricsRegistry>>,
    ) -> JoinHandle<Result<Self, TaskError>> {
        let task = InnerTask::new(table, cache, props, metrics);
        tokio::spawn(task.try_run())
    }

    pub fn spawn(self) -> JoinHandle<Result<Self, TaskError>> {
        tokio::spawn(self.run())
    }

    /// Run compaction followed by collection
    ///
    /// This will always run both compaction and collection
    /// regardless of the configured intervals or if either are
    /// enabled.
    pub async fn run(self) -> TaskResult<Self> {
        Ok(self.compact().await?.collect().await?)
    }

    /// Try to run compaction followed by collection
    ///
    /// This will only run compaction and/or collection if the
    /// configured intervals have elapsed for both or either
    /// tasks and if they are enabled. If neither is enabled,
    /// and/or niether respective interval has elapsed this is
    /// a no-op
    pub async fn try_run(self) -> TaskResult<Self> {
        // First try to collect, then try to compact
        self.try_compact().await?.try_collect().await
    }

    pub async fn collect(mut self) -> CollectionResult<Self> {
        self.collector = self.collector.collect().await?;
        self.previous_collection = Timestamp::now();
        Ok(self)
    }

    pub async fn compact(mut self) -> CompactionResult<Self> {
        self.compactor = self.compactor.compact().await?;
        self.previous_compaction = Timestamp::now();
        Ok(self)
    }

    pub async fn try_collect(self) -> TaskResult<Self> {
        // If collection is active and the interval has elapsed, run collection
        if self.props.collector.active.load(SeqCst)
            && Timestamp::now()
                .0
                .saturating_sub(self.previous_collection.0)
                >= self.props.collector.interval
        {
            Ok(self.collect().await?)
        // Otherwise, return self without doing anything
        } else {
            Ok(self)
        }
    }

    pub async fn try_compact(self) -> TaskResult<Self> {
        // If compaction is active and the interval has elapsed, run compaction
        if self.props.compactor.active.load(SeqCst)
            && Timestamp::now()
                .0
                .saturating_sub(self.previous_compaction.0)
                >= self.props.compactor.interval
        {
            Ok(self.compact().await?)
        // Otherwise, return self without doing anything
        } else {
            Ok(self)
        }
    }
}

pub struct AmpCompactor {
    task: Task,
}

pub struct Task {
    inner: JoinHandle<TaskResult<InnerTask>>,
    table: Arc<PhysicalTable>,
    props: Arc<WriterProperties>,
    metrics: Option<Arc<MetricsRegistry>>,
}

impl Task {
    pub fn new(
        inner: JoinHandle<TaskResult<InnerTask>>,
        table: &Arc<PhysicalTable>,
        props: &Arc<WriterProperties>,
        metrics: Option<Arc<MetricsRegistry>>,
    ) -> Self {
        let table = Arc::clone(table);
        let props = Arc::clone(props);
        Self {
            inner,
            table,
            props,
            metrics,
        }
    }

    pub fn start(
        table: &Arc<PhysicalTable>,
        cache: ParquetFooterCache,
        props: &Arc<WriterProperties>,
        metrics: Option<Arc<MetricsRegistry>>,
    ) -> Self {
        let inner = InnerTask::start(table, cache, props, metrics.clone());
        Self::new(inner, table, props, metrics)
    }

    pub fn is_finished(&self) -> bool {
        self.inner.is_finished()
    }
}

impl AmpCompactor {
    pub fn start(
        table: &Arc<PhysicalTable>,
        cache: ParquetFooterCache,
        opts: &Arc<WriterProperties>,
        metrics: Option<Arc<MetricsRegistry>>,
    ) -> Self {
        let inner = Task::start(table, cache, opts, metrics);
        Self { task: inner }
    }

    pub fn is_finished(&self) -> bool {
        self.task.is_finished()
    }

    pub fn try_run(&mut self) -> TaskResult<()> {
        if self.task.is_finished() {
            self.join_current_then_spawn_new()
                .now_or_never()
                .expect("We checked that it was finished")?;
        }
        Ok(())
    }

    pub async fn join_current_then_spawn_new(&mut self) -> TaskResult<()> {
        let handle = &mut self.task.inner;
        let table = &self.task.table;
        let opts = &self.task.props;
        let metrics = &self.task.metrics;

        let inner = match handle.await {
            // Task completed successfully
            Ok(Ok(inner)) => inner,
            // Task ran but failed
            Ok(Err(e)) => {
                return Err(e);
            }
            // Task was aborted due to panic
            Err(e) if e.is_panic() => {
                return Err(TaskError::JoinError(e));
            }
            // Task was cancelled, set active to false for both compactor and collector
            Err(..) => {
                opts.compactor.active.store(false, SeqCst);
                opts.collector.active.store(false, SeqCst);
                let cache = ParquetFooterCache::builder(opts.cache_size_mb * 1024 * 1024).build();
                InnerTask::new(table, cache, opts, metrics.clone())
            }
        };

        self.task.inner = tokio::spawn(inner.run());

        Ok(())
    }
}

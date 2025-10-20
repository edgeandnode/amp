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

pub type TaskResult<T> = Result<T, AmpCompactorTaskError>;

#[derive(Debug)]
pub enum AmpCompactorTaskError {
    Compaction(CompactorError),
    Collection(CollectorError),
    Join(JoinError),
}

impl Display for AmpCompactorTaskError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            AmpCompactorTaskError::Compaction(e) => e.fmt(f),
            AmpCompactorTaskError::Collection(e) => e.fmt(f),
            AmpCompactorTaskError::Join(e) => e.fmt(f),
        }
    }
}

impl Error for AmpCompactorTaskError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            AmpCompactorTaskError::Compaction(e) => e.source(),
            AmpCompactorTaskError::Collection(e) => e.source(),
            AmpCompactorTaskError::Join(e) => e.source(),
        }
    }
}

impl From<CollectorError> for AmpCompactorTaskError {
    fn from(err: CollectorError) -> Self {
        match err {
            CollectorError::JoinError { err } => AmpCompactorTaskError::Join(err),
            other => AmpCompactorTaskError::Collection(other),
        }
    }
}

impl From<CompactorError> for AmpCompactorTaskError {
    fn from(err: CompactorError) -> Self {
        match err {
            CompactorError::JoinError { err } => AmpCompactorTaskError::Join(err),
            other => AmpCompactorTaskError::Compaction(other),
        }
    }
}

impl From<JoinError> for AmpCompactorTaskError {
    fn from(err: JoinError) -> Self {
        AmpCompactorTaskError::Join(err)
    }
}

#[derive(Clone, Debug)]
pub struct AmpCollectorInnerTask {
    pub compactor: Compactor,
    pub collector: Collector,
    pub props: Arc<WriterProperties>,
    pub table: Arc<PhysicalTable>,
    pub metrics: Option<Arc<MetricsRegistry>>,
    pub previous_collection: Timestamp,
    pub previous_compaction: Timestamp,
}

impl AmpCollectorInnerTask {
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
        let previous_collection = Timestamp::now();
        let previous_compaction = Timestamp::now();

        AmpCollectorInnerTask {
            compactor,
            collector,
            props,
            table,
            metrics,
            previous_collection,
            previous_compaction,
        }
    }

    fn start(
        table: &Arc<PhysicalTable>,
        cache: ParquetFooterCache,
        props: &Arc<WriterProperties>,
        metrics: Option<Arc<MetricsRegistry>>,
    ) -> JoinHandle<Result<Self, AmpCompactorTaskError>> {
        let task = AmpCollectorInnerTask::new(table, cache, props, metrics);
        tokio::spawn(task.try_run())
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
    /// and/or neither respective interval has elapsed this is
    /// a no-op
    pub async fn try_run(self) -> TaskResult<Self> {
        Ok(self.try_compact().await?.try_collect().await?)
    }

    async fn collect(mut self) -> CollectionResult<Self> {
        self.collector = self.collector.collect().await?;
        Ok(self)
    }

    async fn compact(mut self) -> CompactionResult<Self> {
        self.compactor = self.compactor.compact().await?;
        Ok(self)
    }

    async fn try_collect(mut self) -> TaskResult<Self> {
        // If collection is active and the interval has elapsed, run collection
        let is_active = self.props.collector.active.load(SeqCst);
        let has_elapsed = Timestamp::now()
            .0
            .saturating_sub(self.previous_collection.0)
            > self.props.collector.interval;
        if is_active && has_elapsed {
            self.previous_collection = Timestamp::now();
            Ok(self.collect().await?)
        // Otherwise, return self without doing anything
        } else {
            Ok(self)
        }
    }

    async fn try_compact(mut self) -> TaskResult<Self> {
        // If compaction is active and the interval has elapsed, run compaction
        let is_active = self.props.compactor.active.load(SeqCst);
        let has_elapsed = Timestamp::now()
            .0
            .saturating_sub(self.previous_compaction.0)
            > self.props.compactor.interval;

        if is_active && has_elapsed {
            self.previous_compaction = Timestamp::now();
            Ok(self.compact().await?)
        // Otherwise, return self without doing anything
        } else {
            Ok(self)
        }
    }
}

pub struct AmpCompactor {
    task: AmpCompactorTask,
}

pub struct AmpCompactorTask {
    inner: JoinHandle<TaskResult<AmpCollectorInnerTask>>,
}

impl AmpCompactorTask {
    fn new(inner: JoinHandle<TaskResult<AmpCollectorInnerTask>>) -> Self {
        Self { inner }
    }

    pub fn start(
        table: &Arc<PhysicalTable>,
        cache: ParquetFooterCache,
        props: &Arc<WriterProperties>,
        metrics: Option<Arc<MetricsRegistry>>,
    ) -> Self {
        let inner = AmpCollectorInnerTask::start(table, cache, props, metrics.clone());
        Self::new(inner)
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
        let inner = AmpCompactorTask::start(table, cache, opts, metrics);
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

        let inner = handle.await??;

        self.task.inner = tokio::spawn(inner.try_run());

        Ok(())
    }
}

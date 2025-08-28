pub mod collector;
pub mod compactor;
pub mod error;
pub mod group;
pub mod size;

use std::{
    fmt::{Display, Formatter},
    sync::Arc,
    time::Duration,
};

use common::{
    Timestamp, catalog::physical::PhysicalTable,
    parquet::file::properties::WriterProperties as ParquetWriterProperties,
};
use futures::{FutureExt, TryFutureExt};
use tokio::task::JoinHandle;

use crate::compaction::{collector::Collector, compactor::Compactor};
pub use crate::compaction::{
    error::{CompactionError, CompactionResult, DeletionError, DeletionResult},
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
        f.debug_map()
            .entry(&"metadata_concurrency", &self.metadata_concurrency)
            .entry(&"write_concurrency", &self.write_concurrency)
            .entry(&"size_limit", &self.size_limit)
            .entry(
                &"collector_interval_secs",
                &self.collector_interval.as_secs(),
            )
            .entry(
                &"compactor_interval_secs",
                &self.compactor_interval.as_secs(),
            )
            .finish()
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
        self.deletion_task.run().await;
    }
}

pub struct CompactionTask {
    task: JoinHandle<CompactionResult<Compactor>>,
    table: Arc<PhysicalTable>,
    opts: Arc<CompactionProperties>,
    previous: Option<Timestamp>,
}

impl CompactionTask {
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
                .map_or(true, |elapsed| elapsed >= self.opts.compactor_interval)
            && self.opts.active
    }

    pub async fn spawn(&mut self) {
        let task = &mut self.task;

        let compactor = match task
            .map_err(CompactionError::join_error(Compactor::new(
                &self.table,
                &self.opts,
            )))
            .await
        {
            Ok(Ok(compactor)) => {
                self.previous = Some(Timestamp::now());
                compactor
            }
            Err(CompactionError::JoinError { compactor, err }) => {
                tracing::error!("{err}");
                compactor
            }
            _ => panic!("Unexpected error while waiting for compaction task"),
        };

        self.task = tokio::spawn(compactor.compact());
    }

    fn try_run(&mut self) {
        if self.is_ready() {
            self.spawn()
                .now_or_never()
                .expect("We already checked is_finished");
        }
    }
}

pub struct DeletionTask {
    task: JoinHandle<DeletionResult<Collector>>,
    table: Arc<PhysicalTable>,
    opts: Arc<CompactionProperties>,
    previous: Option<Timestamp>,
}

impl DeletionTask {
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

    fn ready(&self) -> bool {
        self.is_finished()
            && self
                .elapsed_since_previous()
                // if None, consider it ready
                .map_or(true, |elapsed| elapsed >= self.opts.collector_interval)
            && self.opts.active
    }

    async fn run(&mut self) {
        let task = &mut self.task;

        let collector = match task
            .map_err(DeletionError::join_error(Collector::new(
                &self.table,
                &self.opts,
            )))
            .now_or_never()
            .expect("We already checked is_finished")
        {
            Ok(Ok(collector)) => {
                self.previous = Some(Timestamp::now());
                collector
            }
            Ok(Err(DeletionError::JoinError { collector, err }))
            | Err(DeletionError::JoinError { collector, err }) => {
                tracing::error!("{err}");
                collector
            }
            Ok(Err(DeletionError::FileDeleteError { collector, .. }))
            | Ok(Err(DeletionError::FileStreamError { collector, .. }))
            | Ok(Err(DeletionError::ManifestDeleteError { collector, .. })) => collector,
            _ => unreachable!("Unexpected error while waiting for compaction task"),
        };

        self.task = tokio::spawn(collector.collect());
    }

    fn try_run(&mut self) {
        if self.ready() {
            self.run()
                .now_or_never()
                .expect("We already checked is_finished");
        }
    }
}

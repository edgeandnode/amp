pub mod collector;
pub mod compactor;
pub mod error;
pub mod size;

use std::{sync::Arc, time::Duration};

use common::{
    Timestamp, catalog::physical::PhysicalTable,
    parquet::file::properties::WriterProperties as ParquetWriterProperties,
};
use futures::TryFutureExt;
use metadata_db::MetadataDb;
use tokio::task::JoinHandle;

use crate::compaction::{
    collector::Collector,
    compactor::{CompactionProperties, Compactor},
};
pub use crate::compaction::{
    error::{CompactionError, CompactionResult, DeletionError, DeletionResult},
    size::{SegmentSize, SegmentSizeLimit},
};

/// Duration collector must wait prior to deleting files
pub const FILE_LOCK_DURATION: Duration = Duration::from_secs(60 * 60); // 1 hour
// Duration Compactor waits between compaction attempts
pub const COMPACTOR_INTERVAL: Duration = Duration::from_secs(0); // No wait by default
/// Duration collector waits between collection attempts
pub const COLLECTOR_INTERVAL: Duration = Duration::from_secs(30 * 60); // 30 minutes

/// Maximum number of distinct Block numbers in a single partition (100,000)
pub const FILE_SIZE_LIMIT_BLOCKS: SegmentSizeLimit = SegmentSizeLimit::BLOCKS();
/// Maximum size of data in a single partition in bytes (2 GB). Total does
/// not include metadata, indexes, etc. so the actual size of a partition
/// may be larger than this limit.
pub const FILE_SIZE_LIMIT_BYTES: SegmentSizeLimit = SegmentSizeLimit::BYTES();
/// Maximum number of rows in a single partition (1,000,000)
pub const FILE_SIZE_LIMIT_ROWS: SegmentSizeLimit = SegmentSizeLimit::ROWS();

pub struct NozzleCompactor {
    compaction_task: CompactionTask,
    deletion_task: DeletionTask,
}

impl NozzleCompactor {
    pub fn start(
        table: Arc<PhysicalTable>,
        opts: &ParquetWriterProperties,
        size_limit: SegmentSizeLimit,
    ) -> Self {
        let opts = CompactionProperties::new(10, opts, size_limit);
        let metadata_db = Arc::clone(&table.metadata_db);

        let compaction_task = Compactor::start(&table, opts);
        let deletion_task = Collector::start(table, metadata_db);

        NozzleCompactor {
            compaction_task,
            deletion_task,
        }
    }

    pub async fn try_run(&mut self) {
        // TODO: Update both try_run methods to use `now_or_never` to avoid waiting
        self.compaction_task.try_run().await;

        self.deletion_task.try_run().await;
    }
}

pub struct CompactionTask {
    task: JoinHandle<CompactionResult<Compactor>>,
    table: Arc<PhysicalTable>,
    opts: CompactionProperties,
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
    }

    async fn try_run(&mut self) {
        if self.is_ready() {
            let task = &mut self.task;

            let future = match task
                .map_err(CompactionError::join_error(Compactor::new(
                    &self.table,
                    &self.opts,
                )))
                .await
            {
                Ok(Ok(compactor)) => compactor,
                Err(CompactionError::JoinError { compactor, err }) => {
                    tracing::error!("{err}");
                    compactor
                }
                _ => panic!("Unexpected error while waiting for compaction task"),
            }
            .compact();

            self.task = tokio::spawn(future);
        }
    }
}

pub struct DeletionTask {
    task: JoinHandle<DeletionResult<Collector>>,
    table: Arc<PhysicalTable>,
    metadata_db: Arc<MetadataDb>,
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
                .map_or(true, |elapsed| elapsed >= COLLECTOR_INTERVAL)
    }

    async fn try_run(&mut self) {
        if self.ready() {
            let task = &mut self.task;

            let collector = match task.await.map_err(DeletionError::join_error(Collector::new(
                &self.table,
                &self.metadata_db,
            ))) {
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
    }
}

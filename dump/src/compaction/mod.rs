pub mod collector;
pub mod compactor;
pub mod error;
pub mod limit;

use std::{sync::Arc, time::Duration};

use common::{
    Timestamp, catalog::physical::PhysicalTable,
    parquet::file::properties::WriterProperties as ParquetWriterProperties,
};
use futures::TryFutureExt;
use metadata_db::MetadataDb;
use tokio::task::JoinHandle;

use crate::compaction::{collector::Collector, compactor::Compactor};
pub use crate::compaction::{
    error::{CompactionError, CompactionResult, DeletionError, DeletionResult},
    limit::{FileSize, FileSizeLimit, FileSizeLimitKind},
};

/// Duration collector must wait prior to deleting files
pub const FILE_LOCK_DURATION: Duration = Duration::from_secs(60 * 60); // 1 hour
/// Duration collector waits between collection attempts
pub const COLLECTOR_INTERVAL: Duration = Duration::from_secs(30 * 60); // 30 minutes

/// Maximum number of distinct Block numbers in a single partition (100,000)
pub const FILE_SIZE_LIMIT_BLOCKS: FileSizeLimit = FileSizeLimit::BLOCK();
/// Maximum size of data in a single partition in bytes (2 GB). Total does
/// not include metadata, indexes, etc. so the actual size of a partition
/// may be larger than this limit.
pub const FILE_SIZE_LIMIT_BYTES: FileSizeLimit = FileSizeLimit::BYTE();
/// Maximum number of rows in a single partition (1,000,000)
pub const FILE_SIZE_LIMIT_ROWS: FileSizeLimit = FileSizeLimit::ROW();

pub struct NozzleCompactor {
    compaction_task: CompactionTask,
    deletion_task: DeletionTask,
}

impl NozzleCompactor {
    pub fn start(
        table: Arc<PhysicalTable>,
        metadata_db: Arc<MetadataDb>,
        opts: &ParquetWriterProperties,
        size_limit: FileSizeLimit,
    ) -> Self {
        let compaction_task = Compactor::start(table.clone(), opts, size_limit);
        let deletion_task = Collector::start(table, metadata_db);

        NozzleCompactor {
            compaction_task,
            deletion_task,
        }
    }

    pub async fn try_run(&mut self) {
        self.compaction_task.try_run().await;

        self.deletion_task.try_run().await;
    }
}

pub struct CompactionTask {
    task: JoinHandle<CompactionResult<Compactor>>,
    table: Arc<PhysicalTable>,
    opts: ParquetWriterProperties,
    size_limit: FileSizeLimit,
}

impl CompactionTask {
    fn is_finished(&self) -> bool {
        self.task.is_finished()
    }

    pub(super) async fn try_run(&mut self) {
        if self.is_finished() {
            let task = &mut self.task;

            let future = match task
                .map_err(CompactionError::join_error(Compactor::new(
                    &self.table,
                    &self.opts,
                    self.size_limit,
                )))
                .await
            {
                Ok(Ok(compactor)) => compactor,
                Err(CompactionError::JoinError { compactor, err }) => {
                    tracing::error!("{err}");
                    compactor
                }
                _ => unreachable!("Unexpected error while waiting for compaction task"),
            }
            .compact();

            self.task = tokio::spawn(future);
        }
    }
}

pub(super) struct DeletionTask {
    table: Arc<PhysicalTable>,
    metadata_db: Arc<MetadataDb>,
    task: JoinHandle<DeletionResult<Collector>>,
    previous: Option<Timestamp>,
}

impl DeletionTask {
    fn elapsed_since_previous(&self) -> Option<Duration> {
        self.previous.map(|previous| {
            let now = Timestamp::now();
            now.0.saturating_sub(previous.0)
        })
    }

    pub(super) fn is_finished(&self) -> bool {
        self.task.is_finished()
    }

    fn ready(&self) -> bool {
        self.is_finished()
            && self
                .elapsed_since_previous()
                .map_or(false, |elapsed| elapsed >= COLLECTOR_INTERVAL)
    }

    pub(super) async fn try_run(&mut self) {
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

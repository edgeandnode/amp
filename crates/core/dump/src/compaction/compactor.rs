use std::{
    fmt::{Debug, Display, Formatter},
    sync::Arc,
    time::Duration,
};

use alloy::transports::BoxFuture;
use common::{catalog::physical::PhysicalTable, metadata::segments::BlockRange};
use futures::{FutureExt, StreamExt, TryStreamExt};
use metadata_db::MetadataDb;

use crate::{
    compaction::{
        CompactionProperties, CompactionResult, CompactorError, NozzleCompactorTaskType,
        SegmentSize,
        plan::{CompactionFile, CompactionPlan},
    },
    parquet_writer::{ParquetFileWriter, ParquetFileWriterOutput},
};

#[derive(Clone)]
pub struct Compactor {
    pub(super) table: Arc<PhysicalTable>,
    pub(super) opts: Arc<CompactionProperties>,
}

impl Debug for Compactor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Compactor {{ table: {} }}", self.table.table_ref())
    }
}

impl Display for Compactor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Compactor {{ opts: {}, table: {} }}",
            self.opts,
            self.table.table_ref()
        )
    }
}

impl Compactor {
    #[tracing::instrument(skip_all, err, fields(table=%self.table.table_ref(), algorithm=%self.opts.algorithm.kind()))]
    pub(super) async fn compact(self) -> CompactionResult<Self> {
        let table = Arc::clone(&self.table);
        let opts = Arc::clone(&self.opts);

        // await: We need to await the PhysicalTable::segments method
        let mut join_set = CompactionPlan::from_table(table, opts)
            .await?
            .try_compact_all();

        // await: We need to await all the compaction tasks to finish before returning
        while let Some(result) = join_set.next().await {
            match result {
                Ok(_) => {}
                Err(err) => {
                    tracing::error!("{err}");
                }
            }
        }

        Ok(self)
    }
}

impl NozzleCompactorTaskType for Compactor {
    type Error = CompactorError;

    fn new(table: &Arc<PhysicalTable>, opts: &Arc<CompactionProperties>) -> Self {
        Compactor {
            table: Arc::clone(table),
            opts: Arc::clone(opts),
        }
    }

    fn run(self) -> BoxFuture<'static, Result<Self, Self::Error>> {
        self.compact().boxed()
    }

    fn interval(opts: &Arc<CompactionProperties>) -> std::time::Duration {
        opts.compactor_interval
    }

    fn active(opts: &Arc<CompactionProperties>) -> bool {
        opts.compactor_active
    }

    fn deactivate(opts: &mut Arc<CompactionProperties>) {
        Arc::make_mut(opts).compactor_active = false;
    }
}

pub struct CompactionGroup {
    pub opts: Arc<CompactionProperties>,
    pub size: SegmentSize,
    pub streams: Vec<CompactionFile>,
    pub table: Arc<PhysicalTable>,
}

impl CompactionGroup {
    pub fn new_empty(opts: &Arc<CompactionProperties>, table: &Arc<PhysicalTable>) -> Self {
        CompactionGroup {
            opts: Arc::clone(opts),
            size: SegmentSize::default(),
            streams: Vec::new(),
            table: Arc::clone(table),
        }
    }

    pub fn push(&mut self, file: CompactionFile) {
        self.size += file.size;
        self.streams.push(file);
    }

    pub fn is_empty_or_singleton(&self) -> bool {
        self.streams.len() <= 1
    }

    async fn write_and_finish(self) -> CompactionResult<ParquetFileWriterOutput> {
        let range = {
            let start_range = &self
                .streams
                .first()
                .expect("At least one stream in group")
                .range;

            let end_range = &self
                .streams
                .last()
                .expect("At least one stream in group")
                .range;

            let network = start_range.network.to_owned();
            let numbers = start_range.start()..=end_range.end();

            BlockRange {
                network,
                numbers,
                hash: end_range.hash,
                prev_hash: start_range.prev_hash,
            }
        };

        let mut writer = ParquetFileWriter::new(
            Arc::clone(&self.table),
            self.opts.parquet_writer_props.clone(),
            range.start(),
        )
        .map_err(CompactorError::create_writer_error(
            &self.opts.parquet_writer_props,
        ))?;

        let mut parent_ids = Vec::with_capacity(self.streams.len());
        for mut file in self.streams {
            while let Some(ref batch) = file.sendable_stream.try_next().await? {
                writer.write(batch).await?;
            }
            parent_ids.push(file.file_id);
        }
        // Increment generation for new file
        let generation = self.size.generation + 1;

        writer
            .close(range, parent_ids, generation)
            .await
            .map_err(|err| CompactorError::FileWriteError { err })
    }

    #[tracing::instrument(skip_all, err, fields(table=%self.table.table_ref(), algorithm=%self.opts.algorithm.kind()))]
    pub async fn compact(self) -> CompactionResult<()> {
        let number_of_files = self.streams.len();
        let metadata_db = self.table.metadata_db();
        let duration = self.opts.file_lock_duration;

        let output = self.write_and_finish().await?;

        output
            .commit_metadata(Arc::clone(&metadata_db))
            .await
            .map_err(CompactorError::metadata_commit_error)?;

        output
            .upsert_gc_manifest(Arc::clone(&metadata_db), duration)
            .await
            .map_err(CompactorError::manifest_update_error(&output.parent_ids))?;

        tracing::info!(
            "Compacted {} files into new file {}",
            number_of_files,
            output.object_meta.location,
        );

        Ok(())
    }
}

impl ParquetFileWriterOutput {
    async fn commit_metadata(
        &self,
        metadata_db: Arc<MetadataDb>,
    ) -> Result<(), metadata_db::Error> {
        let location_id = self.location_id;
        let file_name = self.object_meta.location.filename().unwrap().to_string();
        let object_size = self.object_meta.size;
        let object_e_tag = self.object_meta.e_tag.clone();
        let object_version = self.object_meta.version.clone();
        let parquet_meta = serde_json::to_value(&self.parquet_meta).unwrap();
        let footer = &self.footer;

        metadata_db
            .register_file(
                location_id,
                file_name,
                object_size,
                object_e_tag,
                object_version,
                parquet_meta,
                footer,
            )
            .await
    }

    async fn upsert_gc_manifest(
        &self,
        metadata_db: Arc<MetadataDb>,
        duration: Duration,
    ) -> Result<(), metadata_db::Error> {
        metadata_db
            .upsert_gc_manifest(self.location_id, &self.parent_ids, duration)
            .await
    }
}

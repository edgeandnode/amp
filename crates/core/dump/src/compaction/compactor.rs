use std::{
    fmt::{Debug, Display, Formatter},
    sync::Arc,
    time::Duration,
};

use common::{catalog::physical::PhysicalTable, metadata::segments::BlockRange};
use futures::{FutureExt, StreamExt, TryStreamExt, future::BoxFuture, stream};
use metadata_db::{FileId, MetadataDb};

use crate::{
    compaction::{
        CompactionProperties, CompactionResult, CompactorError, NozzleCompactorTaskType,
        group::{CompactionFile, CompactionGroupGenerator},
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
    #[tracing::instrument(skip_all, err)]
    pub(super) async fn compact(self) -> CompactionResult<Self> {
        let table = Arc::clone(&self.table);
        let opts = Arc::clone(&self.opts);

        // await: We need to await the PhysicalTable::segments method
        let compaction_stream = CompactionGroupGenerator::from_table(table, opts).await?;

        // await: We need to collect the stream
        let compaction_groups = compaction_stream.into_compaction_groups().await;

        tracing::info!(
            "Created {} compaction groups for table {}",
            compaction_groups.len(),
            self.table.table_ref()
        );

        let mut join_set = stream::iter(compaction_groups)
            .map(|group| group.compact())
            .buffer_unordered(1);

        // await: We need to await the completion of compaction tasks
        while let Some(result) = join_set.next().await {
            match result {
                // Happy path, compaction succeeded
                Ok(file_ids) => {
                    tracing::debug!("Compaction succeeded. FileIds: {file_ids:?}");
                    continue;
                }
                // Error occurred during compaction, trace it and move on
                Err(err) => tracing::error!("{err}"),
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

    fn run<'a>(self) -> BoxFuture<'a, Result<Self, Self::Error>> {
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
    pub streams: Vec<CompactionFile>,
    pub table: Arc<PhysicalTable>,
    pub opts: Arc<CompactionProperties>,
}

impl CompactionGroup {
    pub fn new(
        streams: Vec<CompactionFile>,
        table: &Arc<PhysicalTable>,
        opts: &Arc<CompactionProperties>,
    ) -> CompactionResult<Self> {
        Ok(Self {
            streams,
            table: Arc::clone(table),
            opts: Arc::clone(opts),
        })
    }

    async fn write_and_finish(mut self) -> CompactionResult<ParquetFileWriterOutput> {
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

        let mut file_ids = Vec::with_capacity(self.streams.len());

        for file in self.streams.iter_mut() {
            file_ids.push(file.file_id);
            while let Some(ref batch) = file.stream.try_next().await? {
                writer.write(batch).await?;
            }
        }

        writer
            .close(range, file_ids)
            .await
            .map_err(|err| CompactorError::FileWriteError { err })
    }

    pub async fn compact(self) -> CompactionResult<Vec<FileId>> {
        let metadata_db = self.table.metadata_db().clone();
        let duration = self.opts.file_lock_duration;

        let output = self.write_and_finish().await?;

        output
            .commit_metadata(metadata_db.clone())
            .await
            .map_err(CompactorError::metadata_commit_error)?;

        output
            .upsert_gc_manifest(Arc::new(metadata_db), duration)
            .await
            .map_err(CompactorError::manifest_update_error(&output.parent_ids))?;

        Ok(output.parent_ids)
    }
}

impl ParquetFileWriterOutput {
    async fn commit_metadata(&self, metadata_db: MetadataDb) -> Result<(), metadata_db::Error> {
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

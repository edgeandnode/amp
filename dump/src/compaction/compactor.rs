use std::{sync::Arc, time::Duration};

use common::{
    catalog::physical::PhysicalTable,
    metadata::{parquet::ParquetMeta, segments::BlockRange},
};
use futures::{FutureExt, StreamExt, TryStreamExt, future, stream};
use metadata_db::{FileId, FooterBytes, LocationId, MetadataDb};
use object_store::{ObjectMeta, path::Path};

use crate::{
    compaction::{
        CompactionError, CompactionProperties, CompactionResult, CompactionTask,
        group::{CompactionFile, CompactionGroupGenerator},
    },
    parquet_writer::ParquetFileWriter,
};

#[derive(Clone, Debug)]
pub struct Compactor {
    pub(super) table: Arc<PhysicalTable>,
    pub(super) opts: Arc<CompactionProperties>,
}
// INTEGRATE WITH THE FAILFAST JOINSET
impl Compactor {
    /// Starts the compaction task by spawning a new asynchronous task
    /// that returns a new compactor.
    pub fn start(table: &Arc<PhysicalTable>, opts: &Arc<CompactionProperties>) -> CompactionTask {
        CompactionTask {
            task: tokio::spawn(future::ok(Compactor::new(table, opts)).boxed()),
            table: Arc::clone(table),
            opts: Arc::clone(opts),
            previous: None,
        }
    }

    pub(super) fn new(table: &Arc<PhysicalTable>, opts: &Arc<CompactionProperties>) -> Self {
        Compactor {
            table: Arc::clone(table),
            opts: Arc::clone(opts),
        }
    }

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
            .map(|group: CompactionGroup| group.compact())
            .buffer_unordered(self.opts.write_concurrency);

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

    async fn write_and_finish(mut self) -> CompactionResult<CompactionWriterOutput> {
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
        .map_err(CompactionError::create_writer_error(
            &self.table,
            &self.opts.parquet_writer_props,
        ))?;

        let mut file_ids = Vec::with_capacity(self.streams.len());

        for file in self.streams.iter_mut() {
            file_ids.push(file.file_id);
            while let Some(ref batch) = file.stream.try_next().await? {
                writer.write(batch).await?;
            }
        }

        let (parquet_meta, object_meta, footer) = writer
            .close(range, &file_ids)
            .await
            .map_err(|err| CompactionError::FileWriteError { err })?;

        let location_id = self.table.location_id();

        Ok(CompactionWriterOutput::new(
            location_id,
            parquet_meta,
            object_meta,
            file_ids,
            footer,
        ))
    }

    pub async fn compact(self) -> CompactionResult<Vec<FileId>> {
        let metadata_db = self.table.metadata_db();
        let duration = self.opts.file_lock_duration;

        let output = self.write_and_finish().await?;

        output
            .commit_metadata(Arc::clone(&metadata_db))
            .await
            .map_err(CompactionError::metadata_commit_error(
                output.location.as_ref(),
            ))?;

        output
            .upsert_gc_manifest(Arc::clone(&metadata_db), duration)
            .await
            .map_err(CompactionError::manifest_update_error(&output.file_ids))?;

        Ok(output.file_ids)
    }
}

#[derive(Debug)]
pub struct CompactionWriterOutput {
    pub location_id: LocationId,
    pub file_ids: Vec<FileId>,
    pub location: Path,
    pub file_name: String,
    pub parquet_meta: serde_json::Value,
    pub object_size: u64,
    pub object_e_tag: Option<String>,
    pub object_version: Option<String>,
    pub footer: FooterBytes,
}

impl CompactionWriterOutput {
    fn new(
        location_id: LocationId,
        parquet_meta: ParquetMeta,
        ObjectMeta {
            location,
            size: object_size,
            e_tag: object_e_tag,
            version: object_version,
            ..
        }: ObjectMeta,
        file_ids: Vec<FileId>,
        footer: FooterBytes,
    ) -> Self {
        let file_name = parquet_meta.filename.to_string();
        let parquet_meta = serde_json::to_value(parquet_meta)
            .expect("Failed to serialize parquet metadata to JSON");

        CompactionWriterOutput {
            location_id,
            file_ids,
            location,
            file_name,
            parquet_meta,
            object_size,
            object_e_tag,
            object_version,
            footer,
        }
    }

    async fn commit_metadata(
        &self,
        metadata_db: Arc<MetadataDb>,
    ) -> Result<(), metadata_db::Error> {
        let location_id = self.location_id;
        let file_name = self.file_name.clone();
        let object_size = self.object_size;
        let object_e_tag = self.object_e_tag.clone();
        let object_version = self.object_version.clone();
        let parquet_meta = self.parquet_meta.clone();
        let footer = self.footer.clone();

        metadata_db
            .insert_metadata(
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
            .upsert_gc_manifest(self.location_id, &self.file_ids, duration)
            .await
    }
}

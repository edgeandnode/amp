use std::sync::Arc;

use amp_data_store::{DataStore, file_name::FileName};
use common::{
    BoxError, Timestamp,
    arrow::array::RecordBatch,
    catalog::physical::PhysicalTable,
    metadata::{
        Generation, extract_footer_bytes_from_file,
        parquet::{
            GENERATION_METADATA_KEY, PARENT_FILE_ID_METADATA_KEY, PARQUET_METADATA_KEY, ParquetMeta,
        },
        segments::BlockRange,
    },
    parquet::{
        arrow::AsyncArrowWriter, errors::ParquetError,
        file::properties::WriterProperties as ParquetWriterProperties, format::KeyValue,
    },
};
use metadata_db::{
    LocationId, MetadataDb,
    files::{FileId, FooterBytes},
};
use object_store::{ObjectMeta, buffered::BufWriter};
use tracing::{debug, instrument, trace};
use url::Url;

pub async fn commit_metadata(
    metadata_db: &MetadataDb,
    parquet_meta: ParquetMeta,
    ObjectMeta {
        size: object_size,
        e_tag: object_e_tag,
        version: object_version,
        ..
    }: ObjectMeta,
    location_id: LocationId,
    url: &Url,
    footer: FooterBytes,
) -> Result<(), BoxError> {
    let file_name = parquet_meta.filename.clone();
    let parquet_meta = serde_json::to_value(parquet_meta)?;
    metadata_db::files::register(
        metadata_db,
        location_id,
        url,
        file_name,
        object_size,
        object_e_tag,
        object_version,
        parquet_meta,
        &footer,
    )
    .await?;

    // Notify that the dataset has been changed
    trace!("notifying location change for location_id: {}", location_id);
    metadata_db::physical_table::send_location_change_notif(metadata_db, location_id).await?;

    Ok(())
}

pub struct ParquetFileWriter {
    store: DataStore,
    writer: AsyncArrowWriter<BufWriter>,
    filename: FileName,
    table: Arc<PhysicalTable>,
    max_row_group_bytes: usize,
}

impl ParquetFileWriter {
    pub fn new(
        store: DataStore,
        writer: BufWriter,
        filename: FileName,
        table: Arc<PhysicalTable>,
        max_row_group_bytes: usize,
        prop: ParquetWriterProperties,
    ) -> Result<Self, BoxError> {
        let writer = AsyncArrowWriter::try_new(writer, table.schema(), Some(prop))?;
        Ok(Self {
            store,
            writer,
            filename,
            table,
            max_row_group_bytes,
        })
    }

    pub async fn write(&mut self, batch: &RecordBatch) -> Result<(), ParquetError> {
        self.writer.write(batch).await?;

        // Criteria: If adding another batch of this size would exceed the max row group size, flush now.
        let forecasted_size = self.writer.in_progress_size() + batch.get_array_memory_size();
        if forecasted_size >= self.max_row_group_bytes {
            trace!(
                "flushing row group for {} (in-progress size: {} bytes)",
                self.filename,
                self.writer.in_progress_size()
            );
            self.writer.flush().await?;
        }

        Ok(())
    }

    #[must_use = "Dropping without closing the writer will result in an incomplete Parquet file."]
    #[instrument(skip_all, fields(table = %self.table.table_ref_compact(), location = %self.table.location_id()), err)]
    pub async fn close(
        mut self,
        range: BlockRange,
        parent_ids: Vec<FileId>,
        generation: Generation,
    ) -> Result<ParquetFileWriterOutput, BoxError> {
        self.writer.flush().await?;

        let parquet_meta = ParquetMeta {
            table: self.table.table_name().to_string(),
            filename: self.filename.clone(),
            created_at: Timestamp::now(),
            ranges: vec![range.clone()],
        };

        let kv_metadata = KeyValue::new(
            PARQUET_METADATA_KEY.to_string(),
            serde_json::to_string(&parquet_meta)?,
        );
        self.writer.append_key_value_metadata(kv_metadata);

        let parent_file_id_metadata = KeyValue::new(
            PARENT_FILE_ID_METADATA_KEY.to_string(),
            serde_json::to_string(&parent_ids)?,
        );
        self.writer
            .append_key_value_metadata(parent_file_id_metadata);

        let generation_metadata =
            KeyValue::new(GENERATION_METADATA_KEY.to_string(), generation.to_string());
        self.writer.append_key_value_metadata(generation_metadata);

        let meta = self.writer.close().await?;

        debug!(
            "wrote {} for range {} to {}, row count {}",
            self.filename,
            range.start(),
            range.end(),
            meta.num_rows,
        );

        let object_meta = self
            .store
            .head_revision_file_in_object_store(self.table.path(), &self.filename)
            .await?;

        let footer = extract_footer_bytes_from_file(&self.store, &object_meta).await?;

        let location_id = self.table.location_id();
        let url = self.table.url().clone();

        Ok(ParquetFileWriterOutput {
            parquet_meta,
            object_meta,
            location_id,
            url,
            parent_ids,
            footer,
        })
    }

    // This is calculate as:
    // size of row groups flushed to storage + encoded (but uncompressed) size of the in progress row group
    pub fn bytes_written(&self) -> usize {
        self.writer.bytes_written() + self.writer.in_progress_size()
    }
}

pub struct ParquetFileWriterOutput {
    pub(crate) parquet_meta: ParquetMeta,
    pub(crate) object_meta: ObjectMeta,
    pub(crate) location_id: LocationId,
    pub(crate) url: Url,
    pub(crate) parent_ids: Vec<FileId>,
    pub(crate) footer: FooterBytes,
}

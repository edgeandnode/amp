use std::sync::Arc;

use common::{
    BlockNum, BoxError, Timestamp,
    arrow::array::RecordBatch,
    catalog::physical::PhysicalTable,
    metadata::{
        Generation, extract_footer_bytes_from_file,
        parquet::{
            GENERATION_METADATA_KEY, PARENT_FILE_ID_METADATA_KEY, PARQUET_METADATA_KEY, ParquetMeta,
        },
        segments::BlockRange,
    },
    parquet::{arrow::AsyncArrowWriter, errors::ParquetError, format::KeyValue},
};
use metadata_db::{FileId, FooterBytes, LocationId, MetadataDb};
use object_store::{ObjectMeta, buffered::BufWriter, path::Path};
use rand::RngCore as _;
use tracing::{debug, instrument, trace};
use url::Url;

use crate::WriterProperties;

pub async fn commit_metadata(
    metadata_db: &MetadataDb,
    parquet_meta: ParquetMeta,
    ObjectMeta {
        location,
        size: object_size,
        e_tag: object_e_tag,
        version: object_version,
        ..
    }: ObjectMeta,
    location_id: LocationId,
    footer: FooterBytes,
) -> Result<(), BoxError> {
    let file_path = location.as_ref().to_string();
    let parquet_meta = serde_json::to_value(parquet_meta)?;
    metadata_db
        .register_file(
            location_id,
            file_path,
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
    writer: AsyncArrowWriter<BufWriter>,
    file_url: Url,
    filename: String,
    table: Arc<PhysicalTable>,
    max_row_group_bytes: usize,
}

impl ParquetFileWriter {
    pub fn new(
        table: Arc<PhysicalTable>,
        opts: &WriterProperties,
        start: BlockNum,
        max_row_group_bytes: usize,
    ) -> Result<ParquetFileWriter, BoxError> {
        let filename = {
            // Pad `start` to 9 digits for lexicographical sorting.
            // Add a 64-bit hex value from a crytpo RNG to avoid name conflicts from chain reorgs.
            format!("{:09}-{:016x}.parquet", start, rand::rng().next_u64())
        };
        let file_url = table.url().join(&filename)?;
        let file_path = Path::from_url_path(file_url.path())?;
        let object_writer = BufWriter::new(table.object_store(), file_path);
        let writer =
            AsyncArrowWriter::try_new(object_writer, table.schema(), Some(opts.parquet.clone()))?;
        Ok(ParquetFileWriter {
            writer,
            file_url,
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
    #[instrument(skip_all, fields(location = %self.table.location_id()), err)]
    pub async fn close(
        mut self,
        range: BlockRange,
        parent_ids: Vec<FileId>,
        generation: Generation,
    ) -> Result<ParquetFileWriterOutput, BoxError> {
        self.writer.flush().await?;

        debug!(
            "wrote {} for range {} to {}",
            self.filename,
            range.start(),
            range.end(),
        );

        let parquet_meta = ParquetMeta {
            table: self.table.table_name().to_string(),
            filename: self.filename,
            created_at: Timestamp::now(),
            ranges: vec![range],
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

        self.writer.close().await?;

        let location = Path::from_url_path(self.file_url.path())?;
        let object_meta = self.table.object_store().head(&location).await?;

        let footer =
            extract_footer_bytes_from_file(&object_meta, self.table.object_store()).await?;

        let location_id = self.table.location_id();

        Ok(ParquetFileWriterOutput {
            parquet_meta,
            object_meta,
            location_id,
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
    pub(crate) parent_ids: Vec<FileId>,
    pub(crate) footer: FooterBytes,
}

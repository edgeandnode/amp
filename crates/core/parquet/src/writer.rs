use amp_data_store::{DataStore, HeadInObjectStoreError, PhyTableRevision, file_name::FileName};
use datafusion::{
    arrow::{array::RecordBatch, datatypes::SchemaRef},
    parquet::{
        errors::ParquetError,
        file::{metadata::KeyValue, properties::WriterProperties as ParquetWriterProperties},
    },
};
use datasets_common::{block_range::BlockRange, table_name::TableName};
use metadata_db::{
    files::{FileId, FooterBytes},
    physical_table_revision::LocationId,
};
use object_store::{ObjectMeta, buffered::BufWriter};
use parquet_ext::arrow::async_writer::AsyncArrowWriter;
use tracing::Instrument;
use url::Url;

use crate::{
    footer::extract_footer_bytes_from_file,
    generation::Generation,
    meta::{
        GENERATION_METADATA_KEY, PARENT_FILE_ID_METADATA_KEY, PARQUET_METADATA_KEY, ParquetMeta,
    },
    retry::RetryableErrorExt,
    timestamp::Timestamp,
};

/// Identifies the storage target for a Parquet file: which table revision to write into and where.
pub struct WriterTarget<'a> {
    pub table_name: &'a TableName,
    pub location_id: LocationId,
    pub revision: &'a PhyTableRevision,
    pub url: &'a Url,
}

/// Writes RecordBatches into a Parquet file on the object store, embedding Amp metadata on close.
pub struct ParquetFileWriter {
    store: DataStore,
    writer: AsyncArrowWriter<BufWriter>,
    filename: FileName,
    table_ref_compact: String,
    table_name: TableName,
    location_id: LocationId,
    revision: PhyTableRevision,
    url: Url,
    max_row_group_bytes: usize,
    rows_written: usize,
}

impl ParquetFileWriter {
    /// Creates a new writer that will stream record batches into the given buffered writer.
    #[expect(clippy::too_many_arguments)]
    pub fn new<'a>(
        store: DataStore,
        writer: BufWriter,
        filename: FileName,
        schema: SchemaRef,
        table_ref_compact: impl AsRef<str>,
        target: impl Into<WriterTarget<'a>>,
        max_row_group_bytes: usize,
        prop: ParquetWriterProperties,
    ) -> Result<Self, ParquetError> {
        let target = target.into();
        let writer = AsyncArrowWriter::try_new(writer, schema, Some(prop))?;
        Ok(Self {
            store,
            writer,
            filename,
            table_ref_compact: table_ref_compact.as_ref().to_owned(),
            table_name: target.table_name.clone(),
            location_id: target.location_id,
            revision: target.revision.clone(),
            url: target.url.clone(),
            max_row_group_bytes,
            rows_written: 0,
        })
    }

    /// Appends a record batch, flushing the row group when it approaches the configured size limit.
    #[tracing::instrument(skip_all, fields(rows = batch.num_rows(), table = %self.table_ref_compact))]
    pub async fn write(&mut self, batch: &RecordBatch) -> Result<(), ParquetError> {
        self.rows_written += batch.num_rows();
        self.writer.write(batch).await?;

        // Criteria: If adding another batch of this size would exceed the max row group size, flush now.
        let forecasted_size = self.writer.in_progress_size() + batch.get_array_memory_size();
        if forecasted_size >= self.max_row_group_bytes {
            tracing::trace!(
                "flushing row group for {} (in-progress size: {} bytes)",
                self.filename,
                self.writer.in_progress_size()
            );
            self.writer.flush().await?;
        }

        Ok(())
    }

    /// Flushes remaining data, writes Amp metadata into the Parquet footer, and returns the output.
    #[must_use = "Dropping without closing the writer will result in an incomplete Parquet file."]
    #[tracing::instrument(skip_all, fields(table = %self.table_ref_compact, location = %self.location_id), err)]
    pub async fn close(
        mut self,
        range: BlockRange,
        parent_ids: Vec<FileId>,
        generation: Generation,
    ) -> Result<ParquetFileWriterOutput, ParquetFileWriterCloseError> {
        async {
            self.writer
                .flush()
                .await
                .map_err(ParquetFileWriterCloseError::Flush)
        }
        .instrument(tracing::info_span!("flush_remaining"))
        .await?;

        let parquet_meta = ParquetMeta {
            table: self.table_name.to_string(),
            filename: self.filename.clone(),
            created_at: Timestamp::now(),
            ranges: vec![range.clone()],
            watermark: None,
        };

        let kv_metadata = KeyValue::new(
            PARQUET_METADATA_KEY.to_string(),
            serde_json::to_string(&parquet_meta)
                .map_err(ParquetFileWriterCloseError::SerializeKeyValueMetadata)?,
        );
        self.writer.append_key_value_metadata(kv_metadata);

        let parent_file_id_metadata = KeyValue::new(
            PARENT_FILE_ID_METADATA_KEY.to_string(),
            serde_json::to_string(&parent_ids)
                .map_err(ParquetFileWriterCloseError::SerializeKeyValueMetadata)?,
        );
        self.writer
            .append_key_value_metadata(parent_file_id_metadata);

        let generation_metadata =
            KeyValue::new(GENERATION_METADATA_KEY.to_string(), generation.to_string());
        self.writer.append_key_value_metadata(generation_metadata);

        let meta = async {
            self.writer
                .close()
                .await
                .map_err(ParquetFileWriterCloseError::Close)
        }
        .instrument(tracing::info_span!("finalize_parquet"))
        .await?;

        let object_meta = self
            .store
            .head_revision_file_in_object_store(&self.revision, &self.filename)
            .await
            .map_err(ParquetFileWriterCloseError::HeadObject)?;

        tracing::debug!(
            "wrote {} for range {} to {}, row count {}, size {}",
            self.filename,
            range.start(),
            range.end(),
            meta.file_metadata().num_rows(),
            format_size(object_meta.size),
        );

        let footer = extract_footer_bytes_from_file(&self.store, &object_meta)
            .await
            .map_err(ParquetFileWriterCloseError::ExtractFooter)?;

        Ok(ParquetFileWriterOutput {
            parquet_meta,
            object_meta,
            location_id: self.location_id,
            url: self.url,
            parent_ids,
            footer,
        })
    }

    /// Total bytes written: flushed row groups plus the in-progress row group's encoded size.
    pub fn bytes_written(&self) -> usize {
        self.writer.bytes_written() + self.writer.in_progress_size()
    }

    /// Total number of rows written across all batches.
    pub fn rows_written(&self) -> usize {
        self.rows_written
    }
}

/// Formats a byte count as a human-readable string (B, KiB, MiB, or GiB).
fn format_size(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;
    if bytes >= GB {
        format!("{:.2} GiB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MiB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KiB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}

/// Output produced by closing a [`ParquetFileWriter`], containing all data needed to commit the file.
pub struct ParquetFileWriterOutput {
    pub parquet_meta: ParquetMeta,
    pub object_meta: ObjectMeta,
    pub location_id: LocationId,
    pub url: Url,
    pub parent_ids: Vec<FileId>,
    pub footer: FooterBytes,
}

/// Errors that occur when closing a parquet file writer
///
/// This error type is used by `ParquetFileWriter::close()`.
#[derive(Debug, thiserror::Error)]
pub enum ParquetFileWriterCloseError {
    /// Failed to flush pending data to the writer
    ///
    /// This error occurs when the writer cannot flush buffered data before closing.
    /// Data may be partially written or corrupted.
    ///
    /// Common causes:
    /// - Insufficient disk space
    /// - I/O error during write
    /// - Writer in an invalid state
    #[error("Failed to flush writer")]
    Flush(#[source] ParquetError),

    /// Failed to serialize key-value metadata to JSON
    ///
    /// This error occurs when converting parquet metadata or parent file IDs
    /// to JSON string for embedding in the parquet file's key-value metadata.
    ///
    /// Common causes:
    /// - Invalid UTF-8 in metadata fields
    /// - Serialization of non-serializable types
    #[error("Failed to serialize key value metadata")]
    SerializeKeyValueMetadata(#[source] serde_json::Error),

    /// Failed to finalize and close the parquet writer
    ///
    /// This error occurs when the writer fails to write the file footer
    /// and finalize the parquet file structure.
    ///
    /// Common causes:
    /// - I/O error during footer write
    /// - Insufficient disk space
    /// - Writer already closed or in invalid state
    #[error("Failed to close writer")]
    Close(#[source] ParquetError),

    /// Failed to retrieve object metadata from object store
    ///
    /// This error occurs when fetching metadata (size, etag, etc.) for the
    /// newly written parquet file from the object store fails.
    ///
    /// Common causes:
    /// - Object store connectivity issues
    /// - File not found (write failed silently)
    /// - Permission denied
    #[error("Failed to get object metadata")]
    HeadObject(#[source] HeadInObjectStoreError),

    /// Failed to extract footer bytes from the parquet file
    ///
    /// This error occurs when reading the parquet footer from the written file
    /// for caching purposes fails.
    ///
    /// Common causes:
    /// - Corrupted parquet file
    /// - I/O error during read
    /// - Invalid parquet file structure
    #[error("Failed to extract footer bytes")]
    ExtractFooter(#[source] ParquetError),
}

impl RetryableErrorExt for ParquetFileWriterCloseError {
    /// Returns `true` for I/O errors that may succeed on retry; serialization errors are fatal.
    fn is_retryable(&self) -> bool {
        match self {
            Self::Flush(_) => true,
            Self::SerializeKeyValueMetadata(_) => false,
            Self::Close(_) => true,
            Self::HeadObject(_) => true,
            Self::ExtractFooter(_) => true,
        }
    }
}

use std::sync::Arc;

use amp_data_store::{DataStore, HeadInObjectStoreError, file_name::FileName};
use common::{
    BlockRange, Timestamp,
    arrow::array::RecordBatch,
    catalog::physical::PhysicalTable,
    metadata::{
        Generation, extract_footer_bytes_from_file,
        parquet::{
            GENERATION_METADATA_KEY, PARENT_FILE_ID_METADATA_KEY, PARQUET_METADATA_KEY, ParquetMeta,
        },
    },
    parquet::{
        arrow::AsyncArrowWriter,
        errors::ParquetError,
        file::{metadata::KeyValue, properties::WriterProperties as ParquetWriterProperties},
    },
};
use metadata_db::{
    LocationId, MetadataDb,
    files::{FileId, FooterBytes},
};
use object_store::{ObjectMeta, buffered::BufWriter};
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
) -> Result<(), CommitMetadataError> {
    let file_name = parquet_meta.filename.clone();
    let parquet_meta = serde_json::to_value(parquet_meta)
        .map_err(CommitMetadataError::SerializeParquetMetadata)?;
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
    .await
    .map_err(CommitMetadataError::Register)?;

    // Notify that the dataset has been changed
    tracing::trace!("notifying location change for location_id: {}", location_id);
    metadata_db::physical_table::send_location_change_notif(metadata_db, location_id)
        .await
        .map_err(CommitMetadataError::Notify)?;

    Ok(())
}

/// Errors that occur when committing file metadata to the database
///
/// This error type is used by `commit_metadata()`.
#[derive(Debug, thiserror::Error)]
pub enum CommitMetadataError {
    /// Failed to register file metadata in the database
    ///
    /// This error occurs when inserting the file metadata record into the
    /// metadata database fails.
    ///
    /// Common causes:
    /// - Database connection lost
    /// - Constraint violation (duplicate file ID)
    /// - Transaction timeout
    #[error("Failed to register file metadata")]
    Register(#[source] metadata_db::Error),

    /// Failed to serialize parquet metadata to JSON
    ///
    /// This error occurs when converting the parquet metadata structure
    /// to a JSON string for storage in the database.
    ///
    /// Common causes:
    /// - Invalid UTF-8 in metadata fields
    /// - Serialization of non-serializable types
    #[error("Failed to serialize parquet metadata")]
    SerializeParquetMetadata(#[source] serde_json::Error),

    /// Failed to notify subscribers of location change
    ///
    /// This error occurs when publishing a notification about the new file
    /// to the database notification channel fails.
    ///
    /// Common causes:
    /// - Database connection lost
    /// - Notification channel unavailable
    /// - Payload too large
    #[error("Failed to notify location change")]
    Notify(#[source] metadata_db::Error),
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
    ) -> Result<Self, ParquetError> {
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
            tracing::trace!(
                "flushing row group for {} (in-progress size: {} bytes)",
                self.filename,
                self.writer.in_progress_size()
            );
            self.writer.flush().await?;
        }

        Ok(())
    }

    #[must_use = "Dropping without closing the writer will result in an incomplete Parquet file."]
    #[tracing::instrument(skip_all, fields(table = %self.table.table_ref_compact(), location = %self.table.location_id()), err)]
    pub async fn close(
        mut self,
        range: BlockRange,
        parent_ids: Vec<FileId>,
        generation: Generation,
    ) -> Result<ParquetFileWriterOutput, ParquetFileWriterCloseError> {
        self.writer
            .flush()
            .await
            .map_err(ParquetFileWriterCloseError::Flush)?;

        let parquet_meta = ParquetMeta {
            table: self.table.table_name().to_string(),
            filename: self.filename.clone(),
            created_at: Timestamp::now(),
            ranges: vec![range.clone()],
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

        let meta = self
            .writer
            .close()
            .await
            .map_err(ParquetFileWriterCloseError::Close)?;

        tracing::debug!(
            "wrote {} for range {} to {}, row count {}",
            self.filename,
            range.start(),
            range.end(),
            meta.file_metadata().num_rows(),
        );

        let object_meta = self
            .store
            .head_revision_file_in_object_store(self.table.revision(), &self.filename)
            .await
            .map_err(ParquetFileWriterCloseError::HeadObject)?;

        let footer = extract_footer_bytes_from_file(&self.store, &object_meta)
            .await
            .map_err(ParquetFileWriterCloseError::ExtractFooter)?;

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

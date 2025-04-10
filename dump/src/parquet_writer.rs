use std::collections::BTreeMap;
use std::io::{BufReader, Read};
use std::sync::Arc;

use common::arrow::array::RecordBatch;
use common::arrow_helpers::RecordBatchExt;
use common::catalog::physical::PhysicalTable;
use common::meta_tables::scanned_ranges::{self, NozzleMetadata, ScannedRange};
use common::multirange::MultiRange;
use common::parquet::errors::ParquetError;
use common::parquet::format::KeyValue;
use common::{parquet, BlockNum, BoxError, QueryContext, TableRows, Timestamp};
use futures::TryStreamExt;
use log::debug;
use metadata_db::MetadataDb;
use object_store::buffered::BufWriter;
use object_store::path::Path;
use object_store::{GetOptions, GetRange, GetResult, GetResultPayload, ObjectMeta, ObjectStore};
use parquet::arrow::AsyncArrowWriter;
use parquet::file::properties::WriterProperties as ParquetWriterProperties;
use url::Url;

pub struct DatasetWriter {
    writers: BTreeMap<String, TableWriter>,

    metadata_db: Option<Arc<MetadataDb>>,
}

impl DatasetWriter {
    /// Expects `dataset_ctx` to contain a single dataset and `scanned_ranges_by_table` to contain
    /// one entry per table in that dataset.
    pub fn new(
        dataset_ctx: Arc<QueryContext>,
        metadata_db: Option<Arc<MetadataDb>>,
        opts: ParquetWriterProperties,
        start: BlockNum,
        end: BlockNum,
        partition_size: u64,
        scanned_ranges_by_table: BTreeMap<String, MultiRange>,
    ) -> Result<Self, BoxError> {
        let mut writers = BTreeMap::new();
        for table in dataset_ctx.catalog().all_tables() {
            // Unwrap: `scanned_ranges_by_table` contains an entry for each table.
            let table_name = table.table_name();
            let scanned_ranges = scanned_ranges_by_table.get(table_name).unwrap().clone();
            let writer = TableWriter::new(
                table.clone(),
                opts.clone(),
                partition_size,
                scanned_ranges,
                start,
                end,
                metadata_db.clone(),
            )?;
            writers.insert(table_name.to_string(), writer);
        }
        Ok(DatasetWriter {
            writers,
            metadata_db,
        })
    }

    pub async fn write(&mut self, table_rows: TableRows) -> Result<(), BoxError> {
        if table_rows.is_empty() {
            return Ok(());
        }

        let table_name = table_rows.table.name.as_str();

        let writer = self.writers.get_mut(table_name).unwrap();
        let _scanned_range = writer.write(&table_rows).await?;

        Ok(())
    }

    /// Close and flush all pending writes.
    pub async fn close(self) -> Result<(), BoxError> {
        for (_, writer) in self.writers {
            let location_id = writer.table.location_id();
            let metadata_db = self.metadata_db.clone();
            let table_ref = writer.table.table_ref().to_string();
            let data_size = writer.current_file.as_ref().unwrap().bytes_written();
            let nozzle_metadata = writer.close().await?;

            match (location_id, metadata_db) {
                (Some(location_id), Some(metadata_db)) => {
                    if let Some(nozzle_metadata) = nozzle_metadata {
                        insert_scanned_range(
                            nozzle_metadata,
                            metadata_db,
                            location_id,
                            data_size as i64,
                        )
                        .await?
                    }
                }
                (None, None) => {}
                _ => {
                    panic!("inconsistent metadata state for {}", table_ref)
                }
            }
        }

        Ok(())
    }
}

pub async fn insert_scanned_range(
    (scanned_range, object_meta, size_hint): NozzleMetadata,
    metadata_db: Arc<MetadataDb>,
    location_id: i64,
    data_size: i64,
) -> Result<(), BoxError> {
    let file_name = scanned_range.filename.clone();
    let range_start = scanned_range.range_start as i64;
    let range_end = scanned_range.range_end as i64;
    let secs = scanned_range.created_at.0.as_secs() as i64;
    let nanos = scanned_range.created_at.0.subsec_nanos() as i32;

    metadata_db
        .insert_nozzle_metadata(
            location_id,
            file_name.clone(),
            range_start,
            range_end,
            object_meta,
            data_size,
            size_hint as i64,
            (secs, nanos),
        )
        .await?;

    Ok(())
}

struct TableWriter {
    table: PhysicalTable,
    opts: ParquetWriterProperties,
    partition_size: u64,

    /// The ranges of block numbers that this writer is responsible for.
    /// Organized as a stack, where the top range is the next one to be written.
    ranges_to_write: Vec<(u64, u64)>,

    current_range: Option<(u64, u64)>,
    current_file: Option<ParquetFileWriter>,

    metadata_db: Option<Arc<MetadataDb>>,
}

impl TableWriter {
    pub fn new(
        table: PhysicalTable,
        opts: ParquetWriterProperties,
        partition_size: u64,
        scanned_ranges: MultiRange,
        start: BlockNum,
        end: BlockNum,
        metadata_db: Option<Arc<MetadataDb>>,
    ) -> Result<TableWriter, BoxError> {
        let ranges_to_write = {
            let mut ranges = scanned_ranges.complement(start, end).ranges;
            ranges.reverse();
            ranges
        };

        let mut this = TableWriter {
            table,
            opts,
            ranges_to_write,
            partition_size,
            current_range: None,
            current_file: None,
            metadata_db,
        };
        this.next_range()?;
        Ok(this)
    }

    pub async fn write(
        &mut self,
        table_rows: &TableRows,
    ) -> Result<Option<NozzleMetadata>, BoxError> {
        assert_eq!(table_rows.table.name, self.table.table_name());

        let mut metadata = None;

        let block_num = table_rows.block_num()?;

        // The block is past the current range, so we need to close the current file and start a new one.
        if self
            .current_range
            .is_some_and(|(range_start, ..)| range_start < block_num)
        {
            // Unwrap: `current_range` is `Some` by `is_some_and`.
            let end = self.current_range.unwrap().1;
            // Unwrap: If `current_range` is `Some` then `current_file` is also `Some`.
            metadata = Some(self.current_file.take().unwrap().close(end).await?);
            self.next_range()?;
        }

        // For the rest of the function, since `is_finished` is false, we can unwrap `current_range`
        // and `current_file`.
        if self.is_finished() {
            // There are no more ranges to write.
            return Ok(metadata);
        }

        // If the block stream has not yet reached the current range, then skip this block.
        if block_num < self.current_range.unwrap().0 {
            return Ok(metadata);
        }

        let bytes_written = self.current_file.as_ref().unwrap().bytes_written();

        // Check if we need to create a new part file for the table.
        //
        // TODO: Try switching to `ArrowWriter::memory_size()` once
        // https://github.com/apache/arrow-rs/pull/5967 is merged and released.
        if bytes_written >= self.partition_size {
            // `scanned_range` would be `Some` if we have had just created a new a file above, so no
            // bytes would have been written yet.
            assert!(metadata.is_none());

            // Close the current file at `block_num - 1`, the highest block height scanned by it.
            let end = block_num - 1;
            let file_to_close = self.current_file.take().unwrap();
            metadata = Some(file_to_close.close(end).await?);
            let metadata_db = self.metadata_db.clone();
            let location_id = self.table.location_id();
            let table_ref = self.table.table_ref();

            match (metadata_db, location_id) {
                (Some(metadata_db), Some(location_id)) => {
                    // Unwrap: metadata must be Some here
                    insert_scanned_range(
                        metadata.clone().unwrap(),
                        metadata_db,
                        location_id,
                        bytes_written as i64,
                    )
                    .await?
                }
                (None, None) => {}
                _ => {
                    panic!("inconsistent metadata state for {}", table_ref)
                }
            }

            // The current range was partially written, so we need to split it.
            let end = self.current_range.unwrap().1;
            self.current_range = Some((block_num, end));
            self.current_file = Some(ParquetFileWriter::new(
                self.table.clone(),
                self.opts.clone(),
                block_num,
            )?);
        }

        let rows = &table_rows.rows;
        self.current_file.as_mut().unwrap().write(rows).await?;

        Ok(metadata)
    }

    fn next_range(&mut self) -> Result<(), BoxError> {
        // Assert that the current file has been closed.
        assert!(self.current_file.is_none());

        self.current_range = self.ranges_to_write.pop();
        self.current_file = match self.current_range {
            Some((start, _)) => Some(ParquetFileWriter::new(
                self.table.clone(),
                self.opts.clone(),
                start,
            )?),
            None => None,
        };
        Ok(())
    }

    fn is_finished(&self) -> bool {
        match (&self.current_range, &self.current_file) {
            (Some(_), Some(_)) => false,
            (None, None) => {
                // If there is no current range and file, then there should be no ranges to write.
                assert!(self.ranges_to_write.is_empty());
                true
            }
            _ => panic!("inconsistent table writer state"),
        }
    }

    async fn close(self) -> Result<Option<NozzleMetadata>, BoxError> {
        // We should be closing the last range.
        assert!(self.ranges_to_write.is_empty());

        if let (Some(range), Some(file)) = (self.current_range, self.current_file) {
            let end = range.1;
            file.close(end).await.map(Some)
        } else {
            Ok(None)
        }
    }
}

pub struct ParquetFileWriter {
    writer: AsyncArrowWriter<BufWriter>,
    file_url: Url,
    filename: String,

    table: PhysicalTable,

    // The first block number in the range that this writer is responsible for.
    start: BlockNum,

    // Sum of `get_slice_memory_size` for all data written. Does not correspond to the actual size of
    // the written file, particularly because this is uncompressed.
    bytes_written: u64,
}

impl ParquetFileWriter {
    pub fn new(
        table: PhysicalTable,
        opts: ParquetWriterProperties,
        start: BlockNum,
    ) -> Result<ParquetFileWriter, BoxError> {
        let filename = {
            // Pad `start` to 9 digits for lexicographical sorting.
            let padded_start = format!("{:09}", start);
            format!("{padded_start}.parquet")
        };
        let file_url = table.url().join(&filename)?;
        let file_path = Path::from_url_path(file_url.path())?;
        let object_writer = BufWriter::new(table.object_store(), file_path);
        let writer = AsyncArrowWriter::try_new(object_writer, table.schema(), Some(opts))?;
        Ok(ParquetFileWriter {
            writer,
            start,
            table,
            file_url,
            filename,
            bytes_written: 0,
        })
    }

    pub async fn write(&mut self, batch: &RecordBatch) -> Result<(), ParquetError> {
        // Calculate the size of the batch in bytes. `get_slice_memory_size from RecordBatchExt` is the most precise way.
        self.bytes_written += batch.get_slice_memory_size() as u64;

        self.writer.write(batch).await
    }

    #[must_use]
    pub async fn close(
        mut self,
        end: BlockNum,
    ) -> Result<(ScannedRange, ObjectMeta, usize), BoxError> {
        if end < self.start {
            return Err(
                format!("end block {} must be after start block {}", end, self.start).into(),
            );
        }

        self.writer.flush().await?;

        debug!(
            "wrote {} for range {} to {}",
            self.file_url, self.start, end
        );

        let scanned_range = ScannedRange {
            table: self.table.table_name().to_string(),
            range_start: self.start,
            range_end: end,
            filename: self.filename,
            created_at: Timestamp::now(),
        };

        let scanned_range_key = scanned_ranges::METADATA_KEY.to_string();
        let scanned_range_value = serde_json::to_string(&scanned_range)?;

        let kv_metadata = KeyValue::new(scanned_range_key, scanned_range_value);

        self.writer.append_key_value_metadata(kv_metadata);

        self.writer.close().await?;

        let location = Path::from_url_path(self.file_url.path())?;
        let (object_meta, size_hint) =
            get_object_metadata_and_size_hint(self.table.object_store(), location).await?;

        Ok((scanned_range, object_meta, size_hint))
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }
}

/// Reads the last 8 bytes of the file to get the size hint for the parquet metadata.
/// Returns the object metadata and the size hint.
/// 
/// The size hint is the length of the metadata in bytes and can be used to eliminate a
/// round trip to the object store.
async fn get_object_metadata_and_size_hint(
    store: Arc<dyn ObjectStore>,
    location: Path,
) -> Result<(ObjectMeta, usize), BoxError> {
    let mut options = GetOptions::default();
    options.range = Some(GetRange::Suffix(parquet::file::FOOTER_SIZE));

    let GetResult { payload, meta, .. } = store.get_opts(&location, options).await?;

    let footer = match payload {
        GetResultPayload::Stream(stream) => {
            stream
                .try_fold(Vec::with_capacity(8), |mut acc, chunk| async move {
                    acc.extend_from_slice(&chunk);
                    Ok(acc)
                })
                .await
        }
        GetResultPayload::File(f, _) => {
            let mut buf = Vec::with_capacity(8);
            let mut buf_reader = BufReader::new(f);
            buf_reader.read_to_end(buf.as_mut())?;
            Ok(buf)
        }
    }?;

    let mut tail_buf: [u8; 8] = [0; 8];
    tail_buf.copy_from_slice(&footer[footer.len() - 8..]);

    let tail = parquet::file::metadata::ParquetMetaDataReader::decode_footer_tail(&tail_buf)?;
    let size_hint = tail.metadata_length();

    Ok((meta, size_hint))
}

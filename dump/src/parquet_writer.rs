use std::{collections::BTreeMap, sync::Arc};

use common::{
    arrow::array::RecordBatch,
    catalog::physical::PhysicalTable,
    meta_tables::scanned_ranges::{self, ScannedRange},
    multirange::MultiRange,
    parquet,
    parquet::{errors::ParquetError, format::KeyValue},
    BlockNum, BoxError, QueryContext, TableRows, Timestamp,
};
use metadata_db::MetadataDb;
use object_store::{buffered::BufWriter, path::Path};
use parquet::{
    arrow::AsyncArrowWriter, file::properties::WriterProperties as ParquetWriterProperties,
};
use tracing::debug;
use url::Url;

const MAX_PARTITION_BLOCK_RANGE: u64 = 1_000_000;

/// Only used for raw datasets.
pub struct RawDatasetWriter {
    writers: BTreeMap<String, TableWriter>,

    metadata_db: Arc<MetadataDb>,
}

impl RawDatasetWriter {
    /// Expects `dataset_ctx` to contain a single dataset and `scanned_ranges_by_table` to contain
    /// one entry per table in that dataset.
    pub fn new(
        dataset_ctx: Arc<QueryContext>,
        metadata_db: Arc<MetadataDb>,
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
            )?;
            writers.insert(table_name.to_string(), writer);
        }
        Ok(RawDatasetWriter {
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
        let scanned_range = writer.write(&table_rows).await?;

        if let Some(scanned_range) = scanned_range {
            let location_id = writer.table.location_id();
            let metadata_db = &self.metadata_db;

            commit_metadata(scanned_range, metadata_db.clone(), location_id).await?;
        }

        Ok(())
    }

    /// Close and flush all pending writes.
    pub async fn close(self) -> Result<(), BoxError> {
        for (_, writer) in self.writers {
            let location_id = writer.table.location_id();
            let metadata_db = self.metadata_db.clone();

            let scanned_range = writer.close().await?;

            if let Some(scanned_range) = scanned_range {
                commit_metadata(scanned_range, metadata_db, location_id).await?
            }
        }

        Ok(())
    }
}

pub async fn commit_metadata(
    scanned_range: ScannedRange,
    metadata_db: Arc<MetadataDb>,
    location_id: i64,
) -> Result<(), BoxError> {
    let file_name = scanned_range.filename.clone();
    let scanned_range = serde_json::to_value(scanned_range)?;

    metadata_db
        .insert_scanned_range(location_id, file_name, scanned_range)
        .await?;

    // Notify that the dataset has been changed
    let change_tracking_channel = common::stream_helpers::change_tracking_pg_channel(location_id);
    debug!(
        "notified change tracking channel {}",
        change_tracking_channel
    );
    metadata_db.notify(&change_tracking_channel, "").await?;

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
}

impl TableWriter {
    pub fn new(
        table: PhysicalTable,
        opts: ParquetWriterProperties,
        partition_size: u64,
        scanned_ranges: MultiRange,
        start: BlockNum,
        end: BlockNum,
    ) -> Result<TableWriter, BoxError> {
        let ranges_to_write = {
            // Limit maximum range size to 1_000_000 blocks.
            let mut ranges = scanned_ranges
                .complement(start, end)
                .split_with_max(MAX_PARTITION_BLOCK_RANGE);
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
        };
        this.next_range()?;
        Ok(this)
    }

    pub async fn write(
        &mut self,
        table_rows: &TableRows,
    ) -> Result<Option<ScannedRange>, BoxError> {
        assert_eq!(table_rows.table.name, self.table.table_name());

        let mut scanned_range = None;

        let block_num = table_rows.block_num()?;

        // The block is past the current range, so we need to close the current file and start a new one.
        if self.current_range.is_some_and(|r| r.1 < block_num) {
            // Unwrap: `current_range` is `Some` by `is_some_and`.
            let end = self.current_range.unwrap().1;
            // Unwrap: If `current_range` is `Some` then `current_file` is also `Some`.
            scanned_range = Some(self.current_file.take().unwrap().close(end).await?);
            self.next_range()?;
        }

        // For the rest of the function, since `is_finished` is false, we can unwrap `current_range`
        // and `current_file`.
        if self.is_finished() {
            // There are no more ranges to write.
            return Ok(scanned_range);
        }

        // If the block stream has not yet reached the current range, then skip this block.
        if block_num < self.current_range.unwrap().0 {
            return Ok(scanned_range);
        }

        let bytes_written = self.current_file.as_ref().unwrap().bytes_written();

        // Check if we need to create a new part file before writing this batch of rows, because the
        // size of the current row group already exceeds the configured max `partition_size`.
        if bytes_written >= self.partition_size as usize {
            // `scanned_range` would be `Some` if we have had just created a new a file above, so no
            // bytes would have been written yet.
            assert!(scanned_range.is_none());

            // Close the current file at `block_num - 1`, the highest block height scanned by it.
            let end = block_num - 1;
            let file_to_close = self.current_file.take().unwrap();
            scanned_range = Some(file_to_close.close(end).await?);

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

        Ok(scanned_range)
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

    async fn close(self) -> Result<Option<ScannedRange>, BoxError> {
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
        let writer = AsyncArrowWriter::try_new(object_writer, table.schema(), Some(opts.clone()))?;
        Ok(ParquetFileWriter {
            writer,
            start,
            table,
            file_url,
            filename,
        })
    }

    pub async fn write(&mut self, batch: &RecordBatch) -> Result<(), ParquetError> {
        self.writer.write(batch).await
    }

    #[must_use]
    pub async fn close(mut self, end: BlockNum) -> Result<ScannedRange, BoxError> {
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

        Ok(scanned_range)
    }

    // This is calculate as:
    // size of row groups flushed to storage + encoded (but uncompressed) size of the in progress row group
    pub fn bytes_written(&self) -> usize {
        self.writer.bytes_written() + self.writer.in_progress_size()
    }
}

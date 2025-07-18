use std::{collections::BTreeMap, ops::RangeInclusive, sync::Arc};

pub use common::parquet::file::properties::WriterProperties as ParquetWriterProperties;
use common::{
    BlockNum, BoxError, QueryContext, RawTableRows, Timestamp,
    arrow::array::RecordBatch,
    catalog::physical::PhysicalTable,
    metadata::{
        extract_footer_bytes_from_file,
        parquet::{PARQUET_METADATA_KEY, ParquetMeta},
        segments::BlockRange,
    },
    parquet::{arrow::AsyncArrowWriter, errors::ParquetError, format::KeyValue},
};
use metadata_db::{FooterBytes, MetadataDb};
use object_store::{ObjectMeta, buffered::BufWriter, path::Path};
use rand::RngCore as _;
use tracing::debug;
use url::Url;

const MAX_PARTITION_BLOCK_RANGE: u64 = 1_000_000;

/// Only used for raw datasets.
pub struct RawDatasetWriter {
    writers: BTreeMap<String, RawTableWriter>,

    metadata_db: Arc<MetadataDb>,
}

impl RawDatasetWriter {
    /// Expects `dataset_ctx` to contain a single dataset and `block_ranges_by_table` to contain
    /// one entry per table in that dataset.
    pub fn new(
        dataset_ctx: Arc<QueryContext>,
        metadata_db: Arc<MetadataDb>,
        opts: ParquetWriterProperties,
        partition_size: u64,
        missing_ranges_by_table: BTreeMap<String, Vec<RangeInclusive<BlockNum>>>,
    ) -> Result<Self, BoxError> {
        let mut writers = BTreeMap::new();
        for table in dataset_ctx.catalog().tables() {
            // Unwrap: `missing_ranges_by_table` contains an entry for each table.
            let table_name = table.table_name();
            let ranges = missing_ranges_by_table.get(table_name).unwrap().clone();
            let writer = RawTableWriter::new(table.clone(), opts.clone(), partition_size, ranges)?;
            writers.insert(table_name.to_string(), writer);
        }
        Ok(RawDatasetWriter {
            writers,
            metadata_db,
        })
    }

    pub async fn write(&mut self, table_rows: RawTableRows) -> Result<(), BoxError> {
        let table_name = table_rows.table.name();
        let writer = self.writers.get_mut(table_name).unwrap();
        if let Some((parquet_meta, object_meta, footer)) = writer.write(table_rows).await? {
            let location_id = writer.table.location_id();
            commit_metadata(
                &self.metadata_db,
                parquet_meta,
                object_meta,
                location_id,
                footer,
            )
            .await?;
        }

        Ok(())
    }

    /// Close and flush all pending writes.
    pub async fn close(self) -> Result<(), BoxError> {
        for (_, writer) in self.writers {
            let location_id = writer.table.location_id();
            if let Some((parquet_meta, object_meta, footer)) = writer.close().await? {
                commit_metadata(
                    &self.metadata_db,
                    parquet_meta,
                    object_meta,
                    location_id,
                    footer,
                )
                .await?
            }
        }

        Ok(())
    }
}

pub async fn commit_metadata(
    metadata_db: &MetadataDb,
    parquet_meta: ParquetMeta,
    ObjectMeta {
        size: object_size,
        e_tag: object_e_tag,
        version: object_version,
        ..
    }: ObjectMeta,
    location_id: i64,
    footer: FooterBytes,
) -> Result<(), BoxError> {
    let file_name = parquet_meta.filename.clone();
    let parquet_meta = serde_json::to_value(parquet_meta)?;
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
        .await?;

    // Notify that the dataset has been changed
    debug!("notifying location change for location_id: {}", location_id);
    metadata_db.notify_location_change(location_id).await?;

    Ok(())
}

struct RawTableWriter {
    table: Arc<PhysicalTable>,
    opts: ParquetWriterProperties,
    partition_size: u64,

    /// The ranges of block numbers that this writer is responsible for.
    /// Organized as a stack, where the top range is the one being written.
    ranges_to_write: Vec<RangeInclusive<BlockNum>>,

    current_file: Option<ParquetFileWriter>,
    current_range: Option<BlockRange>,
}

impl RawTableWriter {
    pub fn new(
        table: Arc<PhysicalTable>,
        opts: ParquetWriterProperties,
        partition_size: u64,
        missing_ranges: Vec<RangeInclusive<BlockNum>>,
    ) -> Result<Self, BoxError> {
        let ranges_to_write = limit_ranges(missing_ranges, MAX_PARTITION_BLOCK_RANGE);
        let current_file = match ranges_to_write.last() {
            Some(range) => Some(ParquetFileWriter::new(
                table.clone(),
                opts.clone(),
                *range.start(),
            )?),
            None => None,
        };
        Ok(Self {
            table,
            opts,
            ranges_to_write,
            partition_size,
            current_file,
            current_range: None,
        })
    }

    pub async fn write(
        &mut self,
        table_rows: RawTableRows,
    ) -> Result<Option<(ParquetMeta, ObjectMeta, FooterBytes)>, BoxError> {
        assert_eq!(table_rows.table.name(), self.table.table_name());

        let mut parquet_meta = None;
        let block_num = table_rows.block_num();

        // The block is past the current range, so we need to close the current file and start a new one.
        if self
            .ranges_to_write
            .last()
            .is_some_and(|r| *r.end() < block_num)
        {
            parquet_meta = Some(self.close_current_file().await?);
            assert!(self.current_file.is_none());
            self.ranges_to_write.pop();
            self.current_file = match self.ranges_to_write.last() {
                Some(range) => Some(ParquetFileWriter::new(
                    self.table.clone(),
                    self.opts.clone(),
                    *range.start(),
                )?),
                None => None,
            };
        }

        if self.ranges_to_write.is_empty() {
            // There are no more ranges to write.
            assert!(self.current_file.is_none());
            assert!(self.current_range.is_none());
            return Ok(parquet_meta);
        }

        // If the block stream has not yet reached the current range, then skip this block.
        if block_num < *self.ranges_to_write.last().unwrap().start() {
            return Ok(parquet_meta);
        }

        let bytes_written = self.current_file.as_ref().unwrap().bytes_written();

        // Check if we need to create a new part file before writing this batch of rows, because the
        // size of the current row group already exceeds the configured max `partition_size`.
        if bytes_written >= self.partition_size as usize {
            // `parquet_meta` would be `Some` if we have had just created a new a file above, so no
            // bytes would have been written yet.
            assert!(parquet_meta.is_none());

            // Close the current file at `block_num - 1`, the highest block height scanned by it.
            assert_eq!(
                *self.current_range.as_ref().unwrap().numbers.end(),
                block_num - 1
            );
            parquet_meta = Some(self.close_current_file().await?);

            // The current range was partially written, so we need to split it.
            let range = self.ranges_to_write.pop().unwrap();
            self.ranges_to_write.push(block_num..=*range.end());
            self.current_file = Some(ParquetFileWriter::new(
                self.table.clone(),
                self.opts.clone(),
                block_num,
            )?);
        }

        let rows = &table_rows.rows;
        self.current_file.as_mut().unwrap().write(rows).await?;
        self.current_range = match self.current_range.take() {
            None => Some(table_rows.range),
            Some(range) => {
                assert_eq!(&range.network, &table_rows.range.network);
                Some(BlockRange {
                    numbers: *range.numbers.start()..=*table_rows.range.numbers.end(),
                    network: range.network,
                    hash: table_rows.range.hash,
                    prev_hash: range.prev_hash,
                })
            }
        };

        Ok(parquet_meta)
    }

    async fn close(mut self) -> Result<Option<(ParquetMeta, ObjectMeta, FooterBytes)>, BoxError> {
        if self.current_file.is_none() {
            assert!(self.ranges_to_write.is_empty());
            return Ok(None);
        }
        // We should be closing the last range.
        assert!(self.ranges_to_write.len() == 1);
        self.close_current_file().await.map(Some)
    }

    async fn close_current_file(
        &mut self,
    ) -> Result<(ParquetMeta, ObjectMeta, FooterBytes), BoxError> {
        assert!(self.current_file.is_some());
        let file = self.current_file.take().unwrap();
        let range = self.current_range.take().unwrap();
        file.close(range).await
    }
}

pub struct ParquetFileWriter {
    writer: AsyncArrowWriter<BufWriter>,
    file_url: Url,
    filename: String,
    table: Arc<PhysicalTable>,
}

impl ParquetFileWriter {
    pub fn new(
        table: Arc<PhysicalTable>,
        opts: ParquetWriterProperties,
        start: BlockNum,
    ) -> Result<ParquetFileWriter, BoxError> {
        // TODO: We need to make file names unique when we start handling non-finalized blocks.
        let filename = {
            // Pad `start` to 9 digits for lexicographical sorting.
            // Add a 64-bit hex value from a crytpo RNG to avoid name conflicts from chain reorgs.
            format!("{:09}-{:016x}.parquet", start, rand::rng().next_u64())
        };
        let file_url = table.url().join(&filename)?;
        let file_path = Path::from_url_path(file_url.path())?;
        let object_writer = BufWriter::new(table.object_store(), file_path);
        let writer = AsyncArrowWriter::try_new(object_writer, table.schema(), Some(opts.clone()))?;
        Ok(ParquetFileWriter {
            writer,
            file_url,
            filename,
            table,
        })
    }

    pub async fn write(&mut self, batch: &RecordBatch) -> Result<(), ParquetError> {
        self.writer.write(batch).await
    }

    #[must_use]
    pub async fn close(
        mut self,
        range: BlockRange,
    ) -> Result<(ParquetMeta, ObjectMeta, FooterBytes), BoxError> {
        self.writer.flush().await?;

        debug!(
            "wrote {} for range {} to {}",
            self.file_url,
            range.numbers.start(),
            range.numbers.end(),
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
        self.writer.close().await?;

        let location = Path::from_url_path(self.file_url.path())?;
        let object_meta = self.table.object_store().head(&location).await?;

        let footer =
            extract_footer_bytes_from_file(&object_meta, self.table.object_store()).await?;

        Ok((parquet_meta, object_meta, footer))
    }

    // This is calculate as:
    // size of row groups flushed to storage + encoded (but uncompressed) size of the in progress row group
    pub fn bytes_written(&self) -> usize {
        self.writer.bytes_written() + self.writer.in_progress_size()
    }
}

fn limit_ranges(
    mut ranges: Vec<RangeInclusive<BlockNum>>,
    max_len: u64,
) -> Vec<RangeInclusive<BlockNum>> {
    assert!(max_len > 0);
    let mut index = 0;
    while index < ranges.len() {
        let (start, end) = ranges[index].clone().into_inner();
        assert!(start <= end);
        let len = (end - start) + 1;
        if len > max_len {
            let new_end = start + max_len - 1;
            ranges.insert(index, start..=new_end);
            ranges[index + 1] = (new_end + 1)..=end;
        }
        index += 1;
    }
    ranges
}

#[cfg(test)]
mod test {
    #[test]
    fn limit_ranges() {
        // shorter than max_len
        assert_eq!(super::limit_ranges(vec![1..=5], 10), vec![1..=5]);
        // exactly equal to max_len
        assert_eq!(super::limit_ranges(vec![1..=5], 5), vec![1..=5]);
        // needs to split one range
        assert_eq!(
            super::limit_ranges(vec![1..=10], 4),
            vec![1..=4, 5..=8, 9..=10]
        );
        // multiple ranges with only one needing split
        assert_eq!(
            super::limit_ranges(vec![1..=10, 15..=18], 4),
            vec![1..=4, 5..=8, 9..=10, 15..=18]
        );
    }
}

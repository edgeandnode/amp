use std::{collections::BTreeMap, ops::RangeInclusive, sync::Arc};

use common::{
    BlockNum, BoxError, RawTableRows, Store,
    arrow::array::RecordBatch,
    catalog::physical::{Catalog, PhysicalTable},
    metadata::{FileName, Generation, segments::BlockRange},
};
use datasets_common::table_name::TableName;
use metadata_db::MetadataDb;

use crate::{
    WriterProperties,
    compaction::AmpCompactor,
    metrics,
    parquet_writer::{ParquetFileWriter, ParquetFileWriterOutput, commit_metadata},
};

const MAX_PARTITION_BLOCK_RANGE: u64 = 1_000_000;

/// Only used for raw datasets.
pub struct RawDatasetWriter {
    writers: BTreeMap<TableName, RawTableWriter>,
    metadata_db: MetadataDb,
}

impl RawDatasetWriter {
    /// Expects `dataset_ctx` to contain a single dataset and `block_ranges_by_table` to contain
    /// one entry per table in that dataset.
    pub fn new(
        catalog: Catalog,
        metadata_db: MetadataDb,
        store: Store,
        opts: Arc<WriterProperties>,
        missing_ranges_by_table: BTreeMap<TableName, Vec<RangeInclusive<BlockNum>>>,
        compactors_by_table: BTreeMap<TableName, Arc<AmpCompactor>>,
        metrics: Option<Arc<metrics::MetricsRegistry>>,
    ) -> Result<Self, BoxError> {
        let mut writers = BTreeMap::new();
        for table in catalog.tables() {
            // Unwrap: `missing_ranges_by_table` contains an entry for each table.
            let table_name = table.table_name();
            let ranges = missing_ranges_by_table.get(table_name).unwrap().clone();
            let compactor = Arc::clone(compactors_by_table.get(table_name).unwrap());
            let writer = RawTableWriter::new(
                table.clone(),
                store.clone(),
                compactor,
                opts.clone(),
                ranges,
                metrics.clone(),
            )?;
            writers.insert(table_name.clone(), writer);
        }
        Ok(RawDatasetWriter {
            writers,
            metadata_db,
        })
    }

    pub async fn write(&mut self, table_rows: RawTableRows) -> Result<(), BoxError> {
        let table_name = table_rows.table.name();
        let writer = self.writers.get_mut(table_name).unwrap();
        if let Some(ParquetFileWriterOutput {
            parquet_meta,
            object_meta,
            footer,
            url,
            ..
        }) = writer.write(table_rows).await?
        {
            let location_id = writer.table.location_id();
            commit_metadata(
                &self.metadata_db,
                parquet_meta,
                object_meta,
                location_id,
                &url,
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
            if let Some(ParquetFileWriterOutput {
                parquet_meta,
                object_meta,
                footer,
                url,
                ..
            }) = writer.close().await?
            {
                commit_metadata(
                    &self.metadata_db,
                    parquet_meta,
                    object_meta,
                    location_id,
                    &url,
                    footer,
                )
                .await?
            }
        }

        Ok(())
    }
}

struct RawTableWriter {
    table: Arc<PhysicalTable>,
    store: Store,
    opts: Arc<WriterProperties>,

    /// The ranges of block numbers that this writer is responsible for.
    /// Organized as a stack, where the top range is the one being written.
    ranges_to_write: Vec<RangeInclusive<BlockNum>>,

    current_file: Option<ParquetFileWriter>,
    current_range: Option<BlockRange>,

    metrics: Option<Arc<metrics::MetricsRegistry>>,

    compactor: Arc<AmpCompactor>,
}

impl RawTableWriter {
    pub fn new(
        table: Arc<PhysicalTable>,
        store: Store,
        compactor: Arc<AmpCompactor>,
        opts: Arc<WriterProperties>,
        missing_ranges: Vec<RangeInclusive<BlockNum>>,
        metrics: Option<Arc<metrics::MetricsRegistry>>,
    ) -> Result<Self, BoxError> {
        let mut ranges_to_write = limit_ranges(missing_ranges, MAX_PARTITION_BLOCK_RANGE);
        ranges_to_write.reverse();
        let current_file = match ranges_to_write.last() {
            Some(range) => {
                let filename = FileName::new_with_random_suffix(*range.start());
                let buf_writer = store.create_revision_file_writer(table.path(), &filename);
                Some(ParquetFileWriter::new(
                    store.clone(),
                    buf_writer,
                    filename,
                    table.clone(),
                    opts.max_row_group_bytes,
                    opts.parquet.clone(),
                )?)
            }
            None => None,
        };

        Ok(Self {
            table,
            store,
            opts,
            ranges_to_write,
            current_file,
            current_range: None,
            metrics,
            compactor,
        })
    }

    pub async fn write(
        &mut self,
        table_rows: RawTableRows,
    ) -> Result<Option<ParquetFileWriterOutput>, BoxError> {
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
            let new_file = self
                .ranges_to_write
                .last()
                .map(|range| {
                    let filename = FileName::new_with_random_suffix(*range.start());
                    let buf_writer = self
                        .store
                        .create_revision_file_writer(self.table.path(), &filename);
                    ParquetFileWriter::new(
                        self.store.clone(),
                        buf_writer,
                        filename,
                        self.table.clone(),
                        self.opts.max_row_group_bytes,
                        self.opts.parquet.clone(),
                    )
                })
                .transpose()?;
            self.current_file = new_file;
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

        // A reorg is detected if the incoming block prev_hash does not match the hash of the
        // block range previously written. This means we need to split the segment to ensure all
        // blocks within a segment form a valid chain.
        let reorg = match (
            self.current_range.as_ref().map(|r| &r.hash),
            table_rows.range.prev_hash.as_ref(),
        ) {
            (Some(a), Some(b)) => a != b,
            _ => false,
        };
        // We also split the segment if we have reached the configured max `partition_size`.
        let partition_size_exceeded = self.current_file.as_ref().unwrap().bytes_written()
            >= self.opts.partition.0.bytes as usize;
        if reorg || partition_size_exceeded {
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
            let filename = FileName::new_with_random_suffix(block_num);
            let buf_writer = self
                .store
                .create_revision_file_writer(self.table.path(), &filename);
            let new_file = Some(ParquetFileWriter::new(
                self.store.clone(),
                buf_writer,
                filename,
                self.table.clone(),
                self.opts.max_row_group_bytes,
                self.opts.parquet.clone(),
            )?);
            self.current_file = new_file;
        }

        let rows = &table_rows.rows;
        self.current_file.as_mut().unwrap().write(rows).await?;

        self.post_write_metrics(rows);

        self.current_range = match self.current_range.take() {
            None => Some(table_rows.range),
            Some(range) => {
                assert_eq!(&range.network, &table_rows.range.network);
                Some(BlockRange {
                    numbers: range.start()..=table_rows.range.end(),
                    network: range.network,
                    hash: table_rows.range.hash,
                    prev_hash: range.prev_hash,
                })
            }
        };

        Ok(parquet_meta)
    }

    async fn close(mut self) -> Result<Option<ParquetFileWriterOutput>, BoxError> {
        if self.current_file.is_none() {
            assert!(self.ranges_to_write.is_empty());
            return Ok(None);
        }
        // We should be closing the last range.
        assert_eq!(self.ranges_to_write.len(), 1);
        self.close_current_file().await.map(Some)
    }

    async fn close_current_file(&mut self) -> Result<ParquetFileWriterOutput, BoxError> {
        assert!(self.current_file.is_some());
        let file = self.current_file.take().unwrap();
        let range = self.current_range.take().unwrap();

        let metadata = file.close(range, vec![], Generation::default()).await?;

        if let Some(ref metrics) = self.metrics {
            let table_name = self.table.table_name().to_string();
            let location_id = self.table.location_id();
            metrics.record_file_written(table_name, *location_id);
        }

        self.compactor.try_run()?;

        Ok(metadata)
    }

    fn post_write_metrics(&self, rows: &RecordBatch) {
        if let Some(ref metrics) = self.metrics {
            let num_bytes: u64 = rows.get_array_memory_size().try_into().unwrap();
            let table_name = self.table.table_name().to_string();
            let location_id = self.table.location_id();
            metrics.record_write_call(num_bytes, table_name, *location_id);
        }
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

use std::collections::BTreeMap;
use std::sync::Arc;

use common::arrow::array::RecordBatch;
use common::catalog::physical::PhysicalTable;
use common::meta_tables::scanned_ranges::{self, ScannedRange, ScannedRangeRowsBuilder};
use common::parquet::errors::ParquetError;
use common::{parquet, BlockNum, BoxError, QueryContext, Store, TableRows, Timestamp};
use datafusion::sql::TableReference;
use log::debug;
use object_store::buffered::BufWriter;
use object_store::path::Path;
use parquet::arrow::AsyncArrowWriter;
use parquet::file::properties::WriterProperties as ParquetWriterProperties;

fn path_for_part(table_path: &str, start_block: u64) -> String {
    // Pad `start` to 9 digits.
    let padded_start = format!("{:09}", start_block);

    format!("{}{}.parquet", table_path, padded_start)
}

pub struct DatasetWriter {
    writers: BTreeMap<String, ParquetWriter>,
    tables: BTreeMap<String, PhysicalTable>,

    opts: ParquetWriterProperties,
    store: Arc<Store>,
    partition_size: u64,

    // The scanned ranges waiting to be written to the `__scanned_ranges` table.
    scanned_range_batch: ScannedRangeRowsBuilder,

    // For inserting into `__scanned_ranges`
    dataset_ctx: Arc<QueryContext>,
}

impl DatasetWriter {
    pub fn new(
        dataset_ctx: Arc<QueryContext>,
        opts: ParquetWriterProperties,
        start: BlockNum,
        partition_size: u64,
    ) -> Result<Self, BoxError> {
        let mut writers = BTreeMap::new();
        let mut tables = BTreeMap::new();

        // This only handles a single dataset.
        let store = dataset_ctx.catalog().datasets()[0].data_store();

        for table in dataset_ctx.catalog().all_tables() {
            tables.insert(table.table_name().to_string(), table.clone());
            let writer = ParquetWriter::new(&store, table.clone(), opts.clone(), start)?;
            writers.insert(table.table_name().to_string(), writer);
        }
        Ok(DatasetWriter {
            dataset_ctx,
            writers,
            tables,
            opts,
            store,
            partition_size,
            scanned_range_batch: ScannedRangeRowsBuilder::new(),
        })
    }

    pub async fn write(&mut self, table_rows: TableRows) -> Result<(), BoxError> {
        let table = &table_rows.table;
        let block_num = table_rows.block_num()?;

        let bytes_written = self
            .writers
            .get(table.name.as_str())
            .unwrap()
            .bytes_written();

        // Check if we need to create a new part file for the table.
        //
        // TODO: Try switching to `ArrowWriter::memory_size()` once
        // https://github.com/apache/arrow-rs/pull/5967 is merged and released.
        if bytes_written >= self.partition_size {
            let physical_table = self.tables.get(table.name.as_str()).unwrap().clone();
            let new_writer =
                ParquetWriter::new(&self.store, physical_table, self.opts.clone(), block_num)?;
            let old_writer = self.writers.insert(table.name.clone(), new_writer).unwrap();

            // The `__scanned_ranges` optimization works best if ranges are adjacent, even if the
            // tables themselves are sparse and don't have data for all block numbers. So we start
            // the new range at `block_num` and close the previous one at `block_num - 1`.
            let scanned_range = old_writer.close(block_num - 1).await?;
            self.scanned_range_batch.append(&scanned_range);

            // Periodically flush the scanned ranges so the dump process can resume efficiently
            if self.scanned_range_batch.len() >= 10 {
                flush_scanned_ranges(&self.dataset_ctx, &mut self.scanned_range_batch).await?;
            }
        }

        let writer = self.writers.get_mut(table.name.as_str()).unwrap();
        writer.write(&table_rows.rows).await?;

        Ok(())
    }

    /// Flush and close all pending writes.
    pub async fn close(mut self, end: BlockNum) -> Result<(), BoxError> {
        for (_, writer) in self.writers {
            let scanned_range = writer.close(end).await?;
            self.scanned_range_batch.append(&scanned_range);
        }
        flush_scanned_ranges(&self.dataset_ctx, &mut self.scanned_range_batch).await
    }
}

async fn flush_scanned_ranges(
    ctx: &QueryContext,
    ranges: &mut ScannedRangeRowsBuilder,
) -> Result<(), BoxError> {
    let batch = ranges.build();
    let dataset_name = {
        let datasets = ctx.catalog().datasets();
        assert!(datasets.len() == 1, "expected one dataset");
        datasets[0].name().to_string()
    };

    debug!(
        "flushing {} scanned ranges for dataset {dataset_name}",
        batch.num_rows()
    );

    // Build a datafusion logical plan to insert the `batch` into the `__scanned_ranges` table.
    let table_ref = TableReference::partial(dataset_name, scanned_ranges::TABLE_NAME);
    ctx.meta_insert_into(table_ref, batch).await?;

    Ok(())
}

pub struct ParquetWriter {
    writer: AsyncArrowWriter<BufWriter>,
    path: Path,

    table: PhysicalTable,

    // The first block number in the range that this writer is responsible for.
    start: BlockNum,

    // Sum of `get_slice_memory_size` for all data written. Does not correspond to the actual size of
    // the written file, particularly because this is uncompressed.
    bytes_written: u64,
}

impl ParquetWriter {
    pub fn new(
        store: &Store,
        table: PhysicalTable,
        opts: ParquetWriterProperties,
        start: BlockNum,
    ) -> Result<ParquetWriter, BoxError> {
        let path = Path::parse(path_for_part(table.path(), start))?;
        let object_writer = BufWriter::new(store.prefixed_store(), path.clone());
        let writer = AsyncArrowWriter::try_new(object_writer, table.schema(), Some(opts))?;
        Ok(ParquetWriter {
            writer,
            start,
            table,
            path,
            bytes_written: 0,
        })
    }

    pub async fn write(&mut self, batch: &RecordBatch) -> Result<(), ParquetError> {
        // Calculate the size of the batch in bytes. `get_slice_memory_size` is the most precise way.
        self.bytes_written += batch
            .columns()
            .iter()
            .map(|c| c.to_data().get_slice_memory_size().unwrap())
            .sum::<usize>() as u64;

        self.writer.write(batch).await
    }

    #[must_use]
    pub async fn close(self, end: BlockNum) -> Result<ScannedRange, BoxError> {
        if end < self.start {
            return Err(
                format!("end block {} must be after start block {}", end, self.start).into(),
            );
        }

        self.writer.close().await?;

        debug!("wrote {} for range {} to {}", self.path, self.start, end);

        // Unwrap: The path is a file.
        let filename = self.path.filename().unwrap().to_string();
        let scanned_range = ScannedRange {
            table: self.table.table_name().to_string(),
            range_start: self.start,
            range_end: end,
            filename,
            created_at: Timestamp::now(),
        };
        Ok(scanned_range)
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }
}

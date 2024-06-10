use std::collections::BTreeMap;
use std::sync::Arc;

use common::arrow::array::RecordBatch;
use common::dataset_context::TableUrl;
use common::meta_tables::scanned_ranges::{self, ScannedRange, ScannedRangeRowsBuilder};
use common::parquet::errors::ParquetError;
use common::{parquet, BlockNum, BoxError, QueryContext, Table, TableRows, Timestamp};
use object_store::buffered::BufWriter;
use object_store::path::Path;
use object_store::ObjectStore;
use parquet::arrow::AsyncArrowWriter;
use parquet::file::properties::WriterProperties as ParquetWriterProperties;
use url::Url;

fn path_for_part(table_location: &Url, start_block: u64) -> String {
    // Pad `start` to 9 digits.
    let padded_start = format!("{:09}", start_block);

    if table_location.path().ends_with('/') {
        format!("{}{}.parquet", table_location.path(), padded_start)
    } else {
        format!("{}/{}.parquet", table_location.path(), padded_start)
    }
}

pub struct DatasetWriter {
    writers: BTreeMap<String, ParquetWriter>,
    urls: BTreeMap<String, TableUrl>,

    opts: ParquetWriterProperties,
    store: Arc<dyn ObjectStore>,
    partition_size: u64,

    // The scanned ranges waiting to be written to the `__scanned_ranges` table.
    scanned_range_batch: ScannedRangeRowsBuilder,

    // For inserting into `__scanned_ranges`
    dataset_ctx: Arc<QueryContext>,
}

impl DatasetWriter {
    pub async fn new(
        dataset_ctx: Arc<QueryContext>,
        opts: ParquetWriterProperties,
        start: BlockNum,
        partition_size: u64,
    ) -> Result<Self, BoxError> {
        let mut writers = BTreeMap::new();
        let mut urls = BTreeMap::new();
        let store = dataset_ctx.object_store()?;
        for table in dataset_ctx.table_urls() {
            urls.insert(table.name().to_string(), table.clone());
            let writer = ParquetWriter::new(&store, &table, opts.clone(), start).await?;
            writers.insert(table.name().to_string(), writer);
        }
        Ok(DatasetWriter {
            dataset_ctx,
            writers,
            urls,
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
        if bytes_written >= self.partition_size {
            let table_url = self.urls.get(table.name.as_str()).unwrap();
            let new_writer =
                ParquetWriter::new(&self.store, table_url, self.opts.clone(), block_num).await?;
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
    use datafusion::common::ToDFSchema;
    use datafusion::datasource::{DefaultTableSource, MemTable};
    use datafusion::logical_expr::{DmlStatement, LogicalPlan, TableScan, WriteOp};

    let batch = ranges.flush()?;

    // Build a datafusion logical plan to insert the `batch` into the `__scanned_ranges` table.
    let table = scanned_ranges::table();
    let inserted_values = {
        let mem_table = MemTable::try_new(table.schema.clone(), vec![vec![batch]])?;
        let table_source = Arc::new(DefaultTableSource::new(Arc::new(mem_table)));
        let table_scan =
            TableScan::try_new("temp_scanned_range_input", table_source, None, vec![], None)?;
        Arc::new(LogicalPlan::TableScan(table_scan))
    };
    let insert_plan = LogicalPlan::Dml(DmlStatement {
        table_name: table.name.into(),
        table_schema: table.schema.to_dfschema_ref()?,
        op: WriteOp::InsertInto,
        input: inserted_values,
    });

    // Execute plan against meta ctx
    ctx.meta_execute_plan(insert_plan).await?;

    Ok(())
}

pub struct ParquetWriter {
    writer: AsyncArrowWriter<BufWriter>,

    table: Table,

    // The first block number in the range that this writer is responsible for.
    start: BlockNum,

    // Sum of `get_slice_memory_size` for all data written. Does not correspond to the actual size of
    // the written file, particularly because this is uncompressed.
    bytes_written: u64,
}

impl ParquetWriter {
    pub async fn new(
        store: &Arc<dyn ObjectStore>,
        table: &TableUrl,
        opts: ParquetWriterProperties,
        start: BlockNum,
    ) -> Result<ParquetWriter, BoxError> {
        let path = Path::parse(&path_for_part(&table.url, start))?;
        let object_writer = BufWriter::new(store.clone(), path);

        // Watch https://github.com/apache/arrow-datafusion/issues/9493 for a higher level, parallel
        // API for parquet writing.
        let table = &table.table;
        let writer = AsyncArrowWriter::try_new(object_writer, table.schema.clone(), Some(opts))?;
        Ok(ParquetWriter {
            writer,
            start,
            table: table.clone(),
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

    pub async fn close(self, end: BlockNum) -> Result<ScannedRange, BoxError> {
        self.writer.close().await?;

        if end < self.start {
            return Err(
                format!("end block {} must be after start block {}", end, self.start).into(),
            );
        }
        let scanned_range = ScannedRange {
            table: self.table.name.clone(),
            range_start: self.start,
            range_end: end,
            created_at: Timestamp::now(),
        };
        Ok(scanned_range)
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }
}

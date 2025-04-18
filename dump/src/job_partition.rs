use common::multirange::MultiRange;
use common::parquet::file::properties::WriterProperties as ParquetWriterProperties;
use common::{BlockStreamer, BoxError, QueryContext};
use log::info;
use metadata_db::MetadataDb;
use std::collections::BTreeMap;
use std::{sync::Arc, time::Instant};

use crate::parquet_writer::DatasetWriter;

pub struct JobPartition<T: BlockStreamer> {
    pub dataset_ctx: Arc<QueryContext>,
    pub block_streamer: T,
    pub multirange: MultiRange,
    pub id: u32,
    pub parquet_opts: ParquetWriterProperties,

    // The target size of each table partition file in bytes. This is measured as the estimated
    // uncompressed size of the partition. Once the size is reached, a new part file is created. Note
    // that different tables may have a different number of partitions for a same block range.
    // Lighter tables will have less parts than heavier tables.
    pub partition_size: u64,

    pub scanned_ranges_by_table: BTreeMap<String, MultiRange>,

    pub metadata_db: Option<Arc<MetadataDb>>,
}

impl<S: BlockStreamer> JobPartition<S> {
    // - Spawns a task to fetch blocks from the `client`.
    // - Returns a future that will read that block stream and write a parquet file to the object store.
    pub async fn run(self: Arc<Self>) -> Result<(), BoxError> {
        info!(
            "job partition #{} ranges to scan: {}",
            self.id, self.multirange
        );

        // The ranges are run sequentially by design, as parallelism is controlled by the number of jobs.
        for range in &self.multirange.ranges {
            info!(
                "job partition #{} starting scan for range [{}, {}]",
                self.id, range.0, range.1
            );
            let start_time = Instant::now();

            self.run_range(range.0, range.1).await?;

            info!(
                "job partition #{} finished scan for range [{}, {}] in {} minutes",
                self.id,
                range.0,
                range.1,
                start_time.elapsed().as_secs() / 60
            );
        }
        Ok(())
    }

    async fn run_range(&self, start: u64, end: u64) -> Result<(), BoxError> {
        let (mut extractor, extractor_join_handle) = {
            let block_streamer = self.block_streamer.clone();
            let (tx, rx) = tokio::sync::mpsc::channel(100);
            let extractor_task = block_streamer.block_stream(start, end, tx);
            (rx, tokio::spawn(extractor_task))
        };

        let mut writer = DatasetWriter::new(
            self.dataset_ctx.clone(),
            self.metadata_db.clone(),
            self.parquet_opts.clone(),
            start,
            end,
            self.partition_size,
            self.scanned_ranges_by_table.clone(),
        )?;

        while let Some(dataset_rows) = extractor.recv().await {
            for table_rows in dataset_rows {
                writer.write(table_rows).await?;
            }
        }

        // The extraction task stopped sending blocks, so it must have terminated. Here we wait for it to
        // finish and check for any errors and panics.
        log::debug!("Waiting for job partition #{} to finish", self.id);
        extractor_join_handle.await??;

        // Close the last part file for each table, checking for any errors.
        writer.close().await?;

        Ok(())
    }
}

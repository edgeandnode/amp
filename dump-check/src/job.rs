use std::{collections::HashMap, ops::RangeInclusive, sync::Arc};

use common::{
    arrow::{
        array::{AsArray, RecordBatch},
        datatypes::UInt64Type,
    },
    query_context::QueryContext,
    BlockStreamer, BoxError, Dataset, BLOCK_NUM,
};
use futures::{future::join_all, StreamExt as _, TryStreamExt as _};

use crate::metrics::METRICS;

pub struct Job<T: BlockStreamer> {
    #[allow(unused)]
    pub dataset: Dataset,
    pub block_streamer: T,
    pub start: u64,
    pub end: u64,

    pub batch_size: u64,
    pub ctx: Arc<QueryContext>,
}

// Validate buffered vector of dataset batches against existing data in the object store.
async fn validate_batches(
    ctx: Arc<QueryContext>,
    table_name: &str,
    block_range: RangeInclusive<u64>,
    fbatches: &Vec<RecordBatch>,
) -> Result<(), BoxError> {
    let mut record_stream = ctx
        .execute_sql(&format!(
            "select * from {} where block_num >= {} and block_num <= {} order by block_num asc",
            table_name,
            block_range.start(),
            block_range.end()
        ))
        .await
        .map_err(|e| format!("failed to run existing blocks query: {e}"))?;

    let mut total_processed = 0;
    let mut i = 0;
    let mut j = 0;
    let total_rows = fbatches.iter().fold(0, |acc, batch| acc + batch.num_rows());

    while let Some(qbatch) = record_stream.next().await {
        let qbatch: RecordBatch = qbatch?;
        if fbatches[0].num_columns() != qbatch.num_columns() {
            return Err(format!(
                "column count in range {}..{} in table `{}` does not match",
                block_range.start(),
                block_range.end(),
                table_name
            )
            .into());
        }
        let mut row = 0;
        while row < qbatch.num_rows() && i < fbatches.len() {
            let to_slice = (fbatches[i].num_rows() - j).min(qbatch.num_rows() - row);
            if fbatches[i].slice(j, to_slice) != qbatch.slice(row, to_slice) {
                let block_num = fbatches[i]
                    .column_by_name(BLOCK_NUM)
                    .ok_or("missing block_num column")?
                    .as_primitive::<UInt64Type>()
                    .value(j);
                return Err(
                    format!("mismatch in block {} in table `{}`", block_num, table_name).into(),
                );
            }
            total_processed += to_slice;
            j += to_slice;
            row += to_slice;
            if j == fbatches[i].num_rows() {
                j = 0;
                i += 1;
            }
        }
    }
    if total_processed != total_rows {
        return Err(format!(
            "missing {} block(s) in range {}..{} in table `{}`",
            total_rows - total_processed,
            block_range.start(),
            block_range.end(),
            table_name
        )
        .into());
    }
    Ok(())
}

// Spawning a job:
// - Creates a stream to fetch blocks from the `client`.
// - Returns a future that will validate firehose blocks against existing data.
pub async fn run_job(job: Job<impl BlockStreamer>) -> Result<(), BoxError> {
    let stream = {
        let block_streamer = job.block_streamer.clone();
        block_streamer.block_stream(job.start, job.end).await
    };

    let mut table_map: HashMap<String, Vec<RecordBatch>> = HashMap::new();
    let mut batch_start = job.start;

    let mut stream = std::pin::pin!(stream);
    while let Some(dataset_rows) = stream.try_next().await? {
        METRICS.blocks_read.inc();

        let block_num = dataset_rows.block_num();
        for table_rows in dataset_rows {
            let bytes = table_rows
                .rows
                .columns()
                .iter()
                .map(|c| c.to_data().get_slice_memory_size().unwrap())
                .sum::<usize>();
            METRICS.bytes_read.inc_by(bytes as f64);

            table_map
                .entry(format!(
                    "{}.{}",
                    job.dataset.name,
                    table_rows.table.name.clone()
                ))
                .or_insert_with(Vec::new)
                .push(table_rows.rows);
        }

        if block_num % job.batch_size == 0 || block_num == job.end {
            let futures = table_map.iter().map(|(table_name, batches)| {
                validate_batches(
                    job.ctx.clone(),
                    &table_name,
                    RangeInclusive::new(batch_start, block_num),
                    batches,
                )
            });
            for res in join_all(futures).await {
                res?;
            }
            table_map.clear();
            batch_start = block_num + 1;
        }
    }

    Ok(())
}

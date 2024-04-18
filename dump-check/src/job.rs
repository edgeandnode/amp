use anyhow::{anyhow, Context as _};
use common::arrow::array::{AsArray, RecordBatch};
use common::arrow::datatypes::UInt64Type;
use common::dataset_context::DatasetContext;
use common::{BlockStreamer, DataSet};
use futures::{FutureExt, StreamExt as _};
use object_store::ObjectStore;
use std::collections::HashMap;
use std::ops::RangeInclusive;
use std::{sync::Arc, time::Instant};

pub struct Job<T: BlockStreamer> {
    pub dataset: DataSet,
    pub block_streamer: T,
    pub start: u64,
    pub end: u64,
    pub job_id: u8,
    pub batch_size: u64,
    pub store: Arc<dyn ObjectStore>,
    pub ctx: Arc<DatasetContext>,
}


async fn validate_batches(ctx: Arc<DatasetContext>, table_name: String, block_range: RangeInclusive<u64>, fbatches: Vec<RecordBatch>) -> Result<(), anyhow::Error> {

    let mut record_stream = ctx
        .execute_sql(&format!(
            "select * from {} where block_num >= {} and block_num <= {} order by block_num asc",
            table_name,
            block_range.start(),
            block_range.end()
        ))
        .await
        .context("failed to run existing blocks query")?;

    let mut rows_left = fbatches.iter().fold(0, |acc, batch| acc + batch.num_rows());
    let mut looping = 0;
    let mut fi = 0;
    let mut fj = 0;
    while let Some(qbatch) = record_stream.next().await {
        let qbatch: RecordBatch = qbatch?;
        if fbatches[0].num_columns() != qbatch.num_columns() {
            return Err(anyhow!("column count in range {}..{} in table `{}` does not match", block_range.start(), block_range.end(), table_name));
        }
        let mut qr = 0;
    'l: for i in fi..fbatches.len() {
            for j in fj..fbatches[i].num_rows() {
                for k in 0..fbatches[i].num_columns() {
                    if fbatches[i].column(k).slice(j, 1) != qbatch.column(k).slice(qr, 1) {
                        let block_num = qbatch.column(0).as_primitive::<UInt64Type>().value(qr);
                        return Err(anyhow!("column {} in block {} in table `{}` does not match", k, block_num, table_name));
                    }
                }
                qr += 1;
                if qr >= qbatch.num_rows() {
                    // remember where we stopped
                    fi = i;
                    fj = j+1;
                    break 'l;
                }
            }
        }
        rows_left -= qr;
        looping += 1;
        if looping >= 2 {
            println!("looping {looping} times");
        }

    }
    if rows_left != 0 {
        return Err(anyhow!("missing {} block(s) in range {}..{} in table `{}`", rows_left, block_range.start(), block_range.end(), table_name));
    }
    Ok(())
}


// Spawning a job:
// - Spawns a task to fetch blocks from the `client`.
// - Returns a future that will read that block stream and write a parquet file to the object store.
pub async fn run_job(job: Job<impl BlockStreamer>) -> Result<(), anyhow::Error> {
    let start_time = Instant::now();

    let (mut firehose, firehose_join_handle) = {
        let start_block = job.start;
        let end_block = job.end;
        let block_streamer = job.block_streamer.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let firehose_task = block_streamer.block_stream(start_block, end_block, tx);
        (rx, tokio::spawn(firehose_task))
    };

    let mut table_map: HashMap<String, Vec<RecordBatch>>  = HashMap::new();
    let mut batch_start = job.start;

    while let Some(dataset_rows) = firehose.recv().await {
        if dataset_rows.is_empty() {
            continue;
        }

        let block_num = dataset_rows.block_num()?;

        if block_num % 10000 == 0 {
            println!(
                "Reached block {} @ worker #{}, at {}:{:02}",
                block_num,
                job.job_id,
                start_time.elapsed().as_secs() / 60,
                start_time.elapsed().as_secs() % 60
            );
        }


        for table_rows in dataset_rows {
            if table_rows.is_empty() {
                continue;
            }

            let record_batch = table_rows.rows;
            let table = table_rows.table;

            table_map
                .entry(table.name.clone())
                .or_insert_with(Vec::new)
                .push(record_batch);
        }

        if block_num % job.batch_size == 0 || block_num == job.end {
            for (table_name, batches) in table_map.iter_mut() {
                validate_batches(job.ctx.clone(), table_name.clone(), RangeInclusive::new(batch_start, block_num), batches.clone()).await?;
                batches.clear();
                // tokio::spawn(async move {
                //     validate_batch(ctx, table_name, start_block, block_number, fbatch).await.unwrap();
                // });
            }
            batch_start = block_num + 1;
        }
    }
    // The Firehose task stopped sending blocks, so it must have terminated. Here we check if it
    // terminated with any errors or panics.
    firehose_join_handle.now_or_never().unwrap()??;

    Ok(())
}

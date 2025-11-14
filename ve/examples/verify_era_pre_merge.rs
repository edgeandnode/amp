//! Fetch one pre-Merge epoch from Ampd and validate it against the
//! header accumulator.
//!
//! Requirements
//! - Ampd dataset `eth_firehose.blocks` with columns defined in [`BlockRow`]
//! - AMPD_BLOCKS_API (default http://localhost:1602)
//!

use alloy::primitives::{B256, U256};
use amp_client::AmpClient;
use anyhow::{anyhow, Result};
use arrow_flight::sql::client::FlightSqlServiceClient;
use futures::StreamExt;
use tonic::transport::Endpoint;
use typed_arrow::prelude::*;
use typed_arrow::Decimal128;
use vee::{Epoch, EraValidator, ExtHeaderRecord};

#[derive(typed_arrow::Record)]
struct BlockRow {
    pub block_num: u64,                              // UInt64
    pub block_hash: [u8; 32],                        // FixedSizeBinary(32)
    pub total_difficulty: Option<Decimal128<38, 0>>, // Decimal128(38, 0), nullable
}

#[tokio::main]
async fn main() -> Result<()> {
    // First epoch [0, 8191]
    const EPOCH_SIZE: u64 = 8192;
    const EPOCH_INDEX: u64 = 0;
    let start = EPOCH_INDEX * EPOCH_SIZE;
    let end = start + EPOCH_SIZE - 1;

    let sql = format!(
        "SELECT block_num, hash, total_difficulty \
     FROM \"my_namespace/eth_firehose\".blocks \
     WHERE block_num BETWEEN {} AND {} \
     ORDER BY block_num ASC",
        start, end
    );

    let ep =
        std::env::var("NOZZLE_BLOCKS_API").unwrap_or_else(|_| "http://localhost:1602".to_string());
    let endpoint = Endpoint::from_shared(ep)?.timeout(std::time::Duration::from_secs(60));
    let channel = endpoint.connect().await?;
    let flight_client = FlightSqlServiceClient::new(channel);
    let mut client = AmpClient::from_client(flight_client);
    let mut ext = Vec::<ExtHeaderRecord>::with_capacity(EPOCH_SIZE as usize);

    let mut batches = client.query(sql).await?;

    while let Some(batch_res) = batches.next().await {
        let data = batch_res?; // RecordBatch

        // Zero-copy typed views over the batch
        let views = data.iter_views::<BlockRow>()?;

        for row in views.try_flatten()? {
            let block_number = row.block_num;
            let block_hash = B256::from_slice(row.block_hash);

            let td = row
                .total_difficulty
                .as_ref()
                .expect("total_difficulty should not be null here");

            let d_i128: i128 = td.value();
            let total_difficulty = U256::from(d_i128 as u128);

            ext.push(ExtHeaderRecord {
                block_number,
                block_hash,
                total_difficulty,
                full_header: None,
            });
        }
    }

    // Sanity checks
    if ext.len() != EPOCH_SIZE as usize {
        return Err(anyhow!("row count {} != {}", ext.len(), EPOCH_SIZE));
    }
    if ext.first().unwrap().block_number != start || ext.last().unwrap().block_number != end {
        return Err(anyhow!(
            "range mismatch: got {}..{} expected {}..{}",
            ext.first().unwrap().block_number,
            ext.last().unwrap().block_number,
            start,
            end
        ));
    }
    for w in ext.windows(2) {
        if w[1].block_number != w[0].block_number + 1 {
            return Err(anyhow!("gap after block {}", w[0].block_number));
        }
    }

    // Build epoch and validate
    let epoch = Epoch::try_from(ext).expect("epoch construction failed");
    let validator = EraValidator::default();
    let root = validator.validate_era(&epoch)?;
    println!("Validated epoch {} with root 0x{:x}", epoch.number(), root);

    Ok(())
}

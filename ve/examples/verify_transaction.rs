use typed_arrow::prelude::*;

use alloy::consensus::Header;
use alloy::primitives::{B256, U256};
use amp_client::AmpClient;
use anyhow::{anyhow, Result};
use arrow_flight::sql::client::FlightSqlServiceClient;
use futures::StreamExt;
use tonic::transport::Endpoint;
use typed_arrow::Decimal128;

#[derive(typed_arrow::Record)]
pub struct TxRow {
    pub block_hash: [u8; 32],
    pub block_num: u64,
    pub timestamp_ns: i64,
    pub tx_index: u32,
    pub tx_hash: [u8; 32],
    pub to: Option<[u8; 20]>,
    pub nonce: u64,
    pub gas_price: Option<Decimal128<38, 0>>,
    pub gas_limit: u64,
    pub value: Option<Decimal128<38, 0>>,
    pub input: Vec<u8>,
    pub v: Vec<u8>,
    pub r: Vec<u8>,
    pub s: Vec<u8>,
    pub gas_used: u64,
    pub r#type: i32,
    pub max_fee_per_gas: Option<Decimal128<38, 0>>,
    pub max_priority_fee_per_gas: Option<Decimal128<38, 0>>,
    pub from: [u8; 20],
    pub status: i32,
    pub return_data: Vec<u8>,
    pub public_key: Vec<u8>,
    pub begin_ordinal: u64,
    pub end_ordinal: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Pick a small range to test.
    const START_BLOCK: u64 = 0;
    const COUNT: u64 = 16;
    let end_block = START_BLOCK + COUNT - 1;

    let sql = format!(
        r#"
    SELECT
        block_num,
        tx_index,
        tx_hash,
        "to",
        nonce,
        gas_price,
        gas_limit,
        value,
        input,
        v,
        r,
        s,
        "type",
        max_fee_per_gas,
        max_priority_fee_per_gas,
        "from",
        status,
        return_data,
        public_key,
        begin_ordinal,
        end_ordinal,
        block_hash,
        CAST("timestamp" AS BIGINT) AS timestamp_ns
    FROM "my_namespace/eth_firehose".transactions
    WHERE block_num BETWEEN {start} AND {end}
    ORDER BY block_num ASC, tx_index ASC
    "#,
        start = START_BLOCK,
        end = end_block,
    );

    let ep =
        std::env::var("AMPD_BLOCKS_API").unwrap_or_else(|_| "http://localhost:1602".to_string());
    let endpoint = Endpoint::from_shared(ep)?.timeout(std::time::Duration::from_secs(60));
    let channel = endpoint.connect().await?;
    let flight_client = FlightSqlServiceClient::new(channel);
    let mut client = AmpClient::from_client(flight_client);

    let mut batches = client.query(sql).await?;

    Ok(())
}

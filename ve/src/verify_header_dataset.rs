use alloy::consensus::Header;
use alloy::primitives::{Address, Bloom, FixedBytes, B256, U256};
use amp_client::AmpClient;
use anyhow::{anyhow, Result};
use arrow_flight::sql::client::FlightSqlServiceClient;
use datasets_common::reference::Reference;
use futures::StreamExt;
use tonic::transport::Endpoint;
use typed_arrow::prelude::*;
use typed_arrow::Decimal128;

#[derive(typed_arrow::Record)]
pub struct BlockRow {
    pub block_num: u64, // UInt64
    pub timestamp_ns: i64,
    pub hash: [u8; 32], // FixedSizeBinary(32)
    pub parent_hash: [u8; 32],
    pub ommers_hash: [u8; 32],
    pub miner: [u8; 20],
    pub state_root: [u8; 32],
    pub transactions_root: [u8; 32],
    pub receipt_root: [u8; 32],
    pub logs_bloom: Vec<u8>,           // Binary
    pub difficulty: Decimal128<38, 0>, // Decimal128
    pub total_difficulty: Option<Decimal128<38, 0>>,
    pub gas_limit: u64,
    pub gas_used: u64,
    pub extra_data: Vec<u8>, // Binary
    pub mix_hash: [u8; 32],
    pub nonce: u64,
    pub base_fee_per_gas: Option<Decimal128<38, 0>>,
    pub withdrawals_root: Option<[u8; 32]>,
    pub blob_gas_used: Option<u64>,
    pub excess_blob_gas: Option<u64>,
    pub parent_beacon_root: Option<[u8; 32]>,
}

fn dec128_to_u256(d: &Decimal128<38, 0>) -> U256 {
    let v: i128 = d.value();
    let mut be = [0u8; 32];
    be[16..].copy_from_slice(&v.to_be_bytes());
    U256::from_be_bytes(be)
}

impl<'a> TryFrom<&BlockRowView<'a>> for Header {
    type Error = ();

    fn try_from(r: &BlockRowView<'a>) -> Result<Self, Self::Error> {
        let parent_hash = FixedBytes::from_slice(r.parent_hash);
        let ommers_hash = FixedBytes::from_slice(r.ommers_hash);
        let beneficiary = Address::from_slice(r.miner);
        let state_root = FixedBytes::from_slice(r.state_root);
        let transactions_root = FixedBytes::from_slice(r.transactions_root);
        let receipts_root = FixedBytes::from_slice(r.receipt_root);

        let logs_bloom = {
            let mut arr = [0u8; 256];
            if r.logs_bloom.len() != 256 {
                return Err(());
            }
            arr.copy_from_slice(r.logs_bloom);
            Bloom(FixedBytes(arr))
        };

        let difficulty = dec128_to_u256(&r.difficulty);

        let base_fee_per_gas = r
            .base_fee_per_gas
            .as_ref()
            .map(|d| dec128_to_u256(d).to::<u64>());

        let withdrawals_root = r.withdrawals_root.map(|w| FixedBytes::from_slice(&w));

        // Post-Cancun fields (EIP-4844): pass through as-is so None for pre-Cancun
        // and Some(value) for post-Cancun blocks.
        let blob_gas_used = r.blob_gas_used;
        let excess_blob_gas = r.excess_blob_gas;

        let parent_beacon_block_root = r.parent_beacon_root.map(|p| FixedBytes::from_slice(p));

        Ok(Header {
            parent_hash,
            ommers_hash,
            beneficiary,
            state_root,
            transactions_root,
            receipts_root,
            logs_bloom,
            difficulty,
            number: r.block_num,
            gas_limit: r.gas_limit,
            gas_used: r.gas_used,
            timestamp: (r.timestamp_ns / 1_000_000_000) as u64,
            extra_data: r.extra_data.to_vec().into(),
            mix_hash: FixedBytes::from_slice(r.mix_hash),
            nonce: FixedBytes::from_slice(&r.nonce.to_be_bytes()),
            base_fee_per_gas,
            withdrawals_root,
            blob_gas_used,
            excess_blob_gas,
            parent_beacon_block_root,
            requests_hash: None,
        })
    }
}

/// Verify header validity for the first `limit` blocks of the dataset's `blocks` table.
pub async fn verify_header_for_dataset(reference: &Reference, limit: u64) -> Result<()> {
    // Resolve Flight SQL endpoint from env, matching example default.
    let ep =
        std::env::var("AMPD_BLOCKS_API").unwrap_or_else(|_| "http://localhost:1602".to_string());
    let endpoint = Endpoint::from_shared(ep)?.timeout(std::time::Duration::from_secs(60));
    let channel = endpoint.connect().await?;
    let flight_client = FlightSqlServiceClient::new(channel);
    let mut client = AmpClient::from_client(flight_client);

    // Use the dataset FQN (namespace/name) as schema and query the blocks table.
    let fqn = reference.as_fqn().to_string();

    let sql = format!(
        r#"
        SELECT
            block_num,
            CAST(timestamp AS BIGINT) AS timestamp_ns,
            hash,
            parent_hash,
            ommers_hash,
            miner,
            state_root,
            transactions_root,
            receipt_root,
            logs_bloom,
            difficulty,
            total_difficulty,
            gas_limit,
            gas_used,
            extra_data,
            mix_hash,
            nonce,
            base_fee_per_gas,
            withdrawals_root,
            blob_gas_used,
            excess_blob_gas,
            parent_beacon_root
        FROM "{schema}".blocks
        ORDER BY block_num ASC
        LIMIT {limit}
        "#,
        schema = fqn,
        limit = limit
    );

    let mut batches = client.query(sql).await?;

    let mut first_seen: Option<u64> = None;
    let mut last_seen: Option<u64> = None;
    let mut verified = 0usize;

    while let Some(batch_res) = batches.next().await {
        let data = batch_res?; // RecordBatch
        let views = data.iter_views::<BlockRow>()?;

        for row in views.try_flatten()? {
            let block_num = row.block_num;

            if first_seen.is_none() {
                first_seen = Some(block_num);
            }
            last_seen = Some(block_num);

            // Convert AMPD row → alloy Header
            let header = Header::try_from(&row).map_err(|_| anyhow!("header conversion failed"))?;

            // Compute hash from header
            let computed = header.hash_slow(); // B256
            let recorded = B256::from_slice(row.hash); // row.hash is [u8; 32]

            if computed != recorded {
                return Err(anyhow!(
                    "hash mismatch at block {}: computed={}, recorded={}",
                    block_num,
                    computed,
                    recorded
                ));
            }

            verified += 1;
        }
    }

    let first = first_seen.unwrap_or(0);
    let last = last_seen.unwrap_or(0);

    println!(
        "VE: Verified header hash for {} blocks in range [{}..={}] (dataset: {})",
        verified, first, last, reference
    );

    Ok(())
}

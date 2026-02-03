use std::{collections::BTreeMap, ops::RangeInclusive};

use alloy::primitives::{
    Address, B64, B256, BlockHash, BlockNumber, Bloom, Bytes, Signature, TxKind, U256,
};
use amp_client::AmpClient;
use anyhow::Context as _;
use futures::StreamExt as _;
use typed_arrow::{AsViewsIterator as _, Decimal128, List};

/// EIP-7702 authorization tuple: (chain_id, address, nonce, y_parity, r, s)
pub type AuthorizationTuple = (u64, Address, u64, bool, B256, B256);

#[derive(Clone, Debug)]
pub struct Block {
    pub number: BlockNumber,
    pub timestamp: u64,
    pub hash: BlockHash,
    pub parent_hash: BlockHash,
    pub ommers_hash: BlockHash,
    pub miner: Address,
    pub state_root: BlockHash,
    pub transactions_root: BlockHash,
    pub receipts_root: BlockHash,
    pub logs_bloom: Bytes,
    pub difficulty: U256,
    pub total_difficulty: Option<U256>,
    pub gas_limit: u64,
    pub gas_used: u64,
    pub extra_data: Bytes,
    pub mix_hash: BlockHash,
    pub nonce: u64,
    pub base_fee_per_gas: Option<u64>,
    pub withdrawals_root: Option<BlockHash>,
    pub blob_gas_used: Option<u64>,
    pub excess_blob_gas: Option<u64>,
    pub parent_beacon_root: Option<BlockHash>,
    pub requests_hash: Option<BlockHash>,
    pub transactions: Vec<Transaction>,
    pub logs: Vec<Log>,
}

#[derive(Clone, Debug)]
pub struct Transaction {
    pub block_number: BlockNumber,
    pub tx_index: u32,
    pub tx_hash: B256,
    pub tx_type: i32,
    pub nonce: u64,
    pub gas_price: Option<u128>,
    pub max_fee_per_gas: Option<u128>,
    pub max_priority_fee_per_gas: Option<u128>,
    pub max_fee_per_blob_gas: Option<u128>,
    pub gas_limit: u64,
    pub to: Option<Address>,
    pub value: U256,
    pub input: Bytes,
    pub r: B256,
    pub s: B256,
    pub v_parity: bool,
    pub chain_id: Option<u64>,
    pub from: Address,
    pub access_list: Option<Vec<(Address, Vec<B256>)>>,
    pub blob_versioned_hashes: Option<Vec<B256>>,
    pub gas_used: u64,
    pub status: bool,
    pub state_root: Option<B256>,
    pub authorization_list: Option<Vec<AuthorizationTuple>>,
}

#[derive(Clone, Debug)]
pub struct Log {
    pub block_number: BlockNumber,
    pub tx_index: u32,
    pub log_index: u32,
    pub address: Address,
    pub topics: Vec<B256>,
    pub data: Bytes,
}

impl From<&Block> for alloy::consensus::Header {
    fn from(value: &Block) -> Self {
        Self {
            parent_hash: value.parent_hash,
            ommers_hash: value.ommers_hash,
            beneficiary: value.miner,
            state_root: value.state_root,
            transactions_root: value.transactions_root,
            receipts_root: value.receipts_root,
            logs_bloom: Bloom::from_slice(&value.logs_bloom),
            difficulty: value.difficulty,
            number: value.number,
            gas_limit: value.gas_limit,
            gas_used: value.gas_used,
            timestamp: value.timestamp,
            extra_data: value.extra_data.clone(),
            mix_hash: value.mix_hash,
            nonce: B64::from(value.nonce),
            base_fee_per_gas: value.base_fee_per_gas,
            withdrawals_root: value.withdrawals_root,
            blob_gas_used: value.blob_gas_used,
            excess_blob_gas: value.excess_blob_gas,
            parent_beacon_block_root: value.parent_beacon_root,
            requests_hash: value.requests_hash,
        }
    }
}

impl Transaction {
    fn signature(&self) -> Signature {
        Signature::from_scalars_and_parity(self.r, self.s, self.v_parity)
    }

    fn access_list(&self) -> alloy::eips::eip2930::AccessList {
        self.access_list
            .as_ref()
            .map(|access_list| {
                alloy::rpc::types::AccessList(
                    access_list
                        .iter()
                        .map(
                            |(address, storage_keys)| alloy::rpc::types::AccessListItem {
                                address: *address,
                                storage_keys: storage_keys.clone(),
                            },
                        )
                        .collect(),
                )
            })
            .unwrap_or_default()
    }

    fn kind(&self) -> TxKind {
        self.to.map(TxKind::Call).unwrap_or(TxKind::Create)
    }

    pub fn to_tx_envelope(&self) -> anyhow::Result<alloy::consensus::TxEnvelope> {
        use alloy::consensus::{Signed, TxEnvelope, TxLegacy};

        match self.tx_type {
            0 => {
                let tx = TxLegacy {
                    chain_id: self.chain_id,
                    nonce: self.nonce,
                    gas_price: self.gas_price.ok_or_else(|| {
                        anyhow::anyhow!("missing required field 'gas_price' for tx type 0")
                    })?,
                    gas_limit: self.gas_limit,
                    to: self.kind(),
                    value: self.value,
                    input: self.input.clone(),
                };
                Ok(TxEnvelope::Legacy(Signed::new_unchecked(
                    tx,
                    self.signature(),
                    self.tx_hash,
                )))
            }
            1 => {
                let tx = alloy::consensus::TxEip2930 {
                    chain_id: self.chain_id.ok_or_else(|| {
                        anyhow::anyhow!("missing required field 'chain_id' for tx type 1")
                    })?,
                    nonce: self.nonce,
                    gas_price: self.gas_price.ok_or_else(|| {
                        anyhow::anyhow!("missing required field 'gas_price' for tx type 1")
                    })?,
                    gas_limit: self.gas_limit,
                    to: self.kind(),
                    value: self.value,
                    input: self.input.clone(),
                    access_list: self.access_list(),
                };
                Ok(TxEnvelope::Eip2930(Signed::new_unchecked(
                    tx,
                    self.signature(),
                    self.tx_hash,
                )))
            }
            2 => {
                let tx = alloy::consensus::TxEip1559 {
                    chain_id: self.chain_id.ok_or_else(|| {
                        anyhow::anyhow!("missing required field 'chain_id' for tx type 2")
                    })?,
                    nonce: self.nonce,
                    max_fee_per_gas: self.max_fee_per_gas.ok_or_else(|| {
                        anyhow::anyhow!("missing required field 'max_fee_per_gas' for tx type 2")
                    })?,
                    max_priority_fee_per_gas: self.max_priority_fee_per_gas.ok_or_else(|| {
                        anyhow::anyhow!(
                            "missing required field 'max_priority_fee_per_gas' for tx type 2"
                        )
                    })?,
                    gas_limit: self.gas_limit,
                    to: self.kind(),
                    value: self.value,
                    input: self.input.clone(),
                    access_list: self.access_list(),
                };
                Ok(TxEnvelope::Eip1559(Signed::new_unchecked(
                    tx,
                    self.signature(),
                    self.tx_hash,
                )))
            }
            3 => {
                let tx = alloy::consensus::TxEip4844 {
                    chain_id: self.chain_id.ok_or_else(|| {
                        anyhow::anyhow!("missing required field 'chain_id' for tx type 3")
                    })?,
                    nonce: self.nonce,
                    gas_limit: self.gas_limit,
                    max_fee_per_gas: self.max_fee_per_gas.ok_or_else(|| {
                        anyhow::anyhow!("missing required field 'max_fee_per_gas' for tx type 3")
                    })?,
                    max_priority_fee_per_gas: self.max_priority_fee_per_gas.ok_or_else(|| {
                        anyhow::anyhow!("missing required field 'max_priority_fee_per_gas' for tx type 3")
                    })?,
                    to: self.to.ok_or_else(|| {
                        anyhow::anyhow!("missing required field 'to' for tx type 3 (blob txs must call a contract)")
                    })?,
                    value: self.value,
                    access_list: self.access_list(),
                    blob_versioned_hashes: self
                    .blob_versioned_hashes
                    .as_ref()
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "missing required field 'blob_versioned_hashes' for tx type 3"
                        )
                    })?
                    .clone(),
                    max_fee_per_blob_gas: self.max_fee_per_blob_gas.ok_or_else(|| {
                        anyhow::anyhow!("missing required field 'max_fee_per_blob_gas' for tx type 3")
                    })?,
                    input: self.input.clone(),
                };
                Ok(TxEnvelope::Eip4844(Signed::new_unchecked(
                    tx.into(),
                    self.signature(),
                    self.tx_hash,
                )))
            }
            4 => {
                let authorization_list = self
                    .authorization_list
                    .as_ref()
                    .ok_or_else(|| {
                        anyhow::anyhow!("missing required field 'authorization_list' for tx type 4")
                    })?
                    .iter()
                    .map(|(chain_id, address, nonce, y_parity, r, s)| {
                        alloy::eips::eip7702::SignedAuthorization::new_unchecked(
                            alloy::eips::eip7702::Authorization {
                                chain_id: U256::from(*chain_id),
                                address: *address,
                                nonce: *nonce,
                            },
                            if *y_parity { 1 } else { 0 },
                            U256::from_be_bytes(r.0),
                            U256::from_be_bytes(s.0),
                        )
                    })
                    .collect();

                let tx = alloy::consensus::TxEip7702 {
                    chain_id: self.chain_id.ok_or_else(|| {
                        anyhow::anyhow!("missing required field 'chain_id' for tx type 4")
                    })?,
                    nonce: self.nonce,
                    max_fee_per_gas: self.max_fee_per_gas.ok_or_else(|| {
                        anyhow::anyhow!("missing required field 'max_fee_per_gas' for tx type 4")
                    })?,
                    max_priority_fee_per_gas: self.max_priority_fee_per_gas.ok_or_else(|| {
                        anyhow::anyhow!(
                            "missing required field 'max_priority_fee_per_gas' for tx type 4"
                        )
                    })?,
                    gas_limit: self.gas_limit,
                    to: self.to.ok_or_else(|| {
                        anyhow::anyhow!("missing required field 'to' for tx type 4")
                    })?,
                    value: self.value,
                    input: self.input.clone(),
                    access_list: self.access_list(),
                    authorization_list,
                };

                Ok(TxEnvelope::Eip7702(Signed::new_unchecked(
                    tx,
                    self.signature(),
                    self.tx_hash,
                )))
            }
            _ => anyhow::bail!("unknown tx type: {}", self.tx_type),
        }
    }
}

pub async fn fetch_blocks(
    amp: &mut AmpClient,
    dataset: &str,
    range: RangeInclusive<BlockNumber>,
) -> anyhow::Result<Vec<Block>> {
    #[derive(typed_arrow::Record)]
    struct BlockRecordView {
        _block_num: u64,
        block_num: u64,
        timestamp: typed_arrow::Timestamp<typed_arrow::Nanosecond>,
        hash: [u8; 32],
        parent_hash: [u8; 32],
        ommers_hash: [u8; 32],
        miner: [u8; 20],
        state_root: [u8; 32],
        transactions_root: [u8; 32],
        receipt_root: [u8; 32],
        logs_bloom: Vec<u8>,
        difficulty: Decimal128<38, 0>,
        total_difficulty: Option<Decimal128<38, 0>>,
        gas_limit: u64,
        gas_used: u64,
        extra_data: Vec<u8>,
        mix_hash: [u8; 32],
        nonce: u64,
        base_fee_per_gas: Option<Decimal128<38, 0>>,
        withdrawals_root: Option<[u8; 32]>,
        blob_gas_used: Option<u64>,
        excess_blob_gas: Option<u64>,
        parent_beacon_root: Option<[u8; 32]>,
        requests_hash: Option<[u8; 32]>,
    }

    #[derive(typed_arrow::Record)]
    struct TransactionRecordView {
        _block_num: u64,
        block_hash: [u8; 32],
        block_num: u64,
        timestamp: typed_arrow::Timestamp<typed_arrow::Nanosecond>,
        tx_index: u32,
        tx_hash: [u8; 32],
        to: Option<[u8; 20]>,
        nonce: u64,
        gas_price: Option<Decimal128<38, 0>>,
        gas_limit: u64,
        value: String,
        input: Vec<u8>,
        r: [u8; 32],
        s: [u8; 32],
        v_parity: bool,
        chain_id: Option<u64>,
        gas_used: u64,
        r#type: i32,
        max_fee_per_gas: Option<Decimal128<38, 0>>,
        max_priority_fee_per_gas: Option<Decimal128<38, 0>>,
        max_fee_per_blob_gas: Option<Decimal128<38, 0>>,
        from: [u8; 20],
        status: bool,
        state_root: Option<[u8; 32]>,
        access_list: Option<List<AccessListItem>>,
        blob_versioned_hashes: Option<List<[u8; 32]>>,
        authorization_list: Option<List<AuthorizationTuple>>,
    }

    #[derive(typed_arrow::Record)]
    struct AccessListItem {
        address: [u8; 20],
        storage_keys: List<[u8; 32]>,
    }

    #[derive(typed_arrow::Record)]
    struct AuthorizationTuple {
        chain_id: u64,
        address: [u8; 20],
        nonce: u64,
        y_parity: bool,
        r: [u8; 32],
        s: [u8; 32],
    }

    #[derive(typed_arrow::Record)]
    struct LogRecordView {
        _block_num: u64,
        block_hash: [u8; 32],
        block_num: u64,
        timestamp: typed_arrow::Timestamp<typed_arrow::Nanosecond>,
        tx_hash: [u8; 32],
        tx_index: u32,
        log_index: u32,
        address: [u8; 20],
        topic0: Option<[u8; 32]>,
        topic1: Option<[u8; 32]>,
        topic2: Option<[u8; 32]>,
        topic3: Option<[u8; 32]>,
        data: Vec<u8>,
    }

    let start_block = *range.start();
    let end_block = *range.end();

    let mut block_data: Vec<Block> = Default::default();

    let mut logs_by_block: BTreeMap<BlockNumber, Vec<Log>> = Default::default();
    let logs_query = format!(
        "SELECT * FROM \"{dataset}\".logs WHERE block_num >= {start_block} AND block_num <= {end_block}"
    );
    let mut logs_response_stream = amp.query(&logs_query).await?;

    while let Some(batch) = logs_response_stream.next().await {
        let batch = batch.context("decode log record batch")?;

        for view in batch.iter_views::<LogRecordView>()? {
            let view: LogRecordView = view?.try_into()?;

            // Collapse topic0-topic3 into single Vec
            let topics: Vec<B256> = [view.topic0, view.topic1, view.topic2, view.topic3]
                .into_iter()
                .flatten()
                .map(B256::from)
                .collect();

            let log = Log {
                block_number: view.block_num,
                tx_index: view.tx_index,
                log_index: view.log_index,
                address: Address::from(view.address),
                topics,
                data: Bytes::from(view.data),
            };

            logs_by_block.entry(view.block_num).or_default().push(log);
        }
    }

    // Sort logs by (tx_index, log_index)
    for logs in logs_by_block.values_mut() {
        logs.sort_unstable_by_key(|log| (log.tx_index, log.log_index));
    }

    // Fetch transactions for the range
    let mut transactions_by_block: BTreeMap<BlockNumber, Vec<Transaction>> = Default::default();
    let tx_query = format!(
        "SELECT * FROM \"{dataset}\".transactions WHERE block_num >= {start_block} AND block_num <= {end_block}"
    );
    let mut tx_response_stream = amp.query(&tx_query).await?;

    while let Some(batch) = tx_response_stream.next().await {
        let batch = batch.context("decode transaction record batch")?;

        for view in batch.iter_views::<TransactionRecordView>()? {
            let view: TransactionRecordView = view?.try_into()?;

            let transaction = Transaction {
                block_number: view.block_num,
                tx_index: view.tx_index,
                tx_hash: B256::from(view.tx_hash),
                tx_type: view.r#type,
                nonce: view.nonce,
                gas_price: view
                    .gas_price
                    .map(|d| u128::try_from(d.into_value()).expect("gas_price should fit in u128")),
                max_fee_per_gas: view.max_fee_per_gas.map(|d| {
                    u128::try_from(d.into_value()).expect("max_fee_per_gas should fit in u128")
                }),
                max_priority_fee_per_gas: view.max_priority_fee_per_gas.map(|d| {
                    u128::try_from(d.into_value())
                        .expect("max_priority_fee_per_gas should fit in u128")
                }),
                max_fee_per_blob_gas: view.max_fee_per_blob_gas.map(|d| {
                    u128::try_from(d.into_value()).expect("max_fee_per_blob_gas should fit in u128")
                }),
                gas_limit: view.gas_limit,
                to: view.to.map(Address::from),
                value: U256::from_str_radix(&view.value, 10)
                    .expect("value should be valid decimal string"),
                input: Bytes::from(view.input),
                r: view.r.into(),
                s: view.s.into(),
                v_parity: view.v_parity,
                chain_id: view.chain_id,
                from: Address::from(view.from),
                access_list: view.access_list.map(|list| {
                    list.into_inner()
                        .into_iter()
                        .map(|item| {
                            let address = Address::from(item.address);
                            let storage_keys = item
                                .storage_keys
                                .into_inner()
                                .into_iter()
                                .map(B256::from)
                                .collect();
                            (address, storage_keys)
                        })
                        .collect()
                }),
                blob_versioned_hashes: view
                    .blob_versioned_hashes
                    .map(|hashes| hashes.into_inner().into_iter().map(B256::from).collect()),
                gas_used: view.gas_used,
                status: view.status,
                state_root: view.state_root.map(B256::from),
                authorization_list: view.authorization_list.map(|list| {
                    list.into_inner()
                        .into_iter()
                        .map(|item| {
                            (
                                item.chain_id,
                                Address::from(item.address),
                                item.nonce,
                                item.y_parity,
                                B256::from(item.r),
                                B256::from(item.s),
                            )
                        })
                        .collect()
                }),
            };

            transactions_by_block
                .entry(view.block_num)
                .or_default()
                .push(transaction);
        }
    }

    for txs in transactions_by_block.values_mut() {
        txs.sort_unstable_by_key(|tx| tx.tx_index);
    }

    // Fetch blocks and attach transactions and logs
    let block_query = format!(
        "SELECT * FROM \"{dataset}\".blocks WHERE block_num >= {start_block} AND block_num <= {end_block} ORDER BY block_num"
    );
    let mut block_response_stream = amp.query(&block_query).await?;

    while let Some(batch) = block_response_stream.next().await {
        let batch = batch.context("decode block record batch")?;

        for view in batch.iter_views::<BlockRecordView>()? {
            let view: BlockRecordView = view?.try_into()?;
            let block_num = view.block_num;
            let block = Block {
                number: block_num,
                timestamp: (view.timestamp.value() / 1_000_000_000) as u64,
                hash: BlockHash::from(view.hash),
                parent_hash: BlockHash::from(view.parent_hash),
                ommers_hash: BlockHash::from(view.ommers_hash),
                miner: Address::from(view.miner),
                state_root: BlockHash::from(view.state_root),
                transactions_root: BlockHash::from(view.transactions_root),
                receipts_root: BlockHash::from(view.receipt_root),
                logs_bloom: view.logs_bloom.into(),
                difficulty: U256::try_from(view.difficulty.into_value())
                    .expect("difficulty should be non-negative"),
                total_difficulty: view.total_difficulty.map(|d| {
                    U256::try_from(d.into_value()).expect("total_difficulty should be non-negative")
                }),
                gas_limit: view.gas_limit,
                gas_used: view.gas_used,
                extra_data: view.extra_data.into(),
                mix_hash: BlockHash::from(view.mix_hash),
                nonce: view.nonce,
                base_fee_per_gas: view.base_fee_per_gas.map(|d| {
                    u64::try_from(d.into_value()).expect("base_fee_per_gas should fit in u64")
                }),
                withdrawals_root: view.withdrawals_root.map(BlockHash::from),
                blob_gas_used: view.blob_gas_used,
                excess_blob_gas: view.excess_blob_gas,
                parent_beacon_root: view.parent_beacon_root.map(BlockHash::from),
                requests_hash: view.requests_hash.map(BlockHash::from),
                transactions: transactions_by_block.remove(&block_num).unwrap_or_default(),
                logs: logs_by_block.remove(&block_num).unwrap_or_default(),
            };

            block_data.push(block);
        }
    }

    block_data.sort_unstable_by_key(|b| b.number);

    assert!(
        block_data
            .windows(2)
            .all(|b| (b[0].number + 1) == b[1].number)
    );

    anyhow::ensure!(
        block_data.len() as u64 == (range.end() - range.start() + 1),
        "dataset missing block {}",
        *range.start() + block_data.len() as u64
    );

    Ok(block_data)
}

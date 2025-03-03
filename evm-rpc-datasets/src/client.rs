use std::time::Duration;

use alloy::eips::BlockNumberOrTag;
use alloy::providers::Provider as _;
use alloy::rpc::types::BlockTransactionsKind;
use alloy::rpc::types::Filter as LogsFilter;
use alloy::rpc::types::Header;
use alloy::rpc::types::Log as RpcLog;
use alloy::transports::http::reqwest::Url;
use common::evm::tables::blocks::Block;
use common::evm::tables::blocks::BlockRowsBuilder;
use common::evm::tables::logs::Log;
use common::evm::tables::logs::LogRowsBuilder;
use common::BlockNum;
use common::BlockStreamer;
use common::BoxError;
use common::DatasetRows;
use common::EvmCurrency;
use common::Timestamp;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::try_join;

#[derive(Error, Debug)]
pub enum ToRowError {
    #[error("missing field: {0}")]
    Missing(&'static str),
    #[error("overflow in field {0}: {1}")]
    Overflow(&'static str, BoxError),
}

#[derive(Clone)]
pub struct JsonRpcClient {
    client: alloy::providers::ReqwestProvider,
    network: String,
}

impl JsonRpcClient {
    pub fn new(url: Url, network: String) -> Result<Self, BoxError> {
        let client = alloy::providers::ProviderBuilder::new().on_http(url);
        Ok(Self { client, network })
    }

    async fn block_stream(
        self,
        start_block: u64,
        end_block: u64,
        tx: mpsc::Sender<DatasetRows>,
    ) -> Result<(), BoxError> {
        for block_num in start_block..=end_block {
            let filter = LogsFilter::new().select(block_num);
            let (block, logs) = try_join!(
                self.client.get_block_by_number(
                    BlockNumberOrTag::Number(block_num),
                    BlockTransactionsKind::Hashes,
                ),
                self.client.get_logs(&filter),
            )?;
            let block = match block {
                Some(block) => block,
                None => return Err(format!("block not found: {block_num}").into()),
            };

            let rows = rpc_to_rows(block.header, logs, &self.network)?;

            // Send the block and check if the receiver has gone away.
            if tx.send(rows).await.is_err() {
                break;
            }
        }
        Ok(())
    }
}

impl AsRef<alloy::providers::ReqwestProvider> for JsonRpcClient {
    fn as_ref(&self) -> &alloy::providers::ReqwestProvider {
        &self.client
    }
}

impl BlockStreamer for JsonRpcClient {
    async fn block_stream(
        self,
        start: BlockNum,
        end: BlockNum,
        tx: mpsc::Sender<common::DatasetRows>,
    ) -> Result<(), BoxError> {
        self.block_stream(start, end, tx).await
    }

    async fn latest_block(&mut self, finalized: bool) -> Result<BlockNum, BoxError> {
        let number = match finalized {
            true => BlockNumberOrTag::Finalized,
            false => BlockNumberOrTag::Latest,
        };
        let kind = BlockTransactionsKind::Hashes;
        let block = self.client.get_block_by_number(number, kind).await?;
        Ok(block.map(|b| b.header.number).unwrap_or(0))
    }
}

fn rpc_to_rows(block: Header, logs: Vec<RpcLog>, network: &str) -> Result<DatasetRows, BoxError> {
    let header = rpc_header_to_row(block)?;
    let logs = logs
        .into_iter()
        .map(|log| rpc_log_to_row(log, header.timestamp))
        .collect::<Result<Vec<_>, _>>()?;

    let header_row = {
        let mut builder = BlockRowsBuilder::with_capacity(1);
        builder.append(&header);
        builder.build(network.to_string())?
    };

    let logs_row = {
        let mut builder = LogRowsBuilder::with_capacity(logs.len());
        for log in logs {
            builder.append(&log);
        }
        builder.build(network.to_string())?
    };

    Ok(DatasetRows(vec![header_row, logs_row]))
}

fn rpc_header_to_row(header: Header) -> Result<Block, ToRowError> {
    Ok(Block {
        block_num: header.number,
        timestamp: Timestamp(Duration::from_secs(header.timestamp)),
        hash: header.hash.into(),
        parent_hash: header.parent_hash.into(),
        ommers_hash: header.ommers_hash.into(),
        miner: header.beneficiary.into(),
        state_root: header.state_root.into(),
        transactions_root: header.transactions_root.into(),
        receipt_root: header.receipts_root.into(),
        logs_bloom: <[u8; 256]>::from(header.logs_bloom).into(),
        difficulty: EvmCurrency::try_from(header.difficulty)
            .map_err(|e| ToRowError::Overflow("difficulty", e.into()))?,
        gas_limit: u64::try_from(header.gas_limit)
            .map_err(|e| ToRowError::Overflow("gas_limit", e.into()))?,
        gas_used: u64::try_from(header.gas_used)
            .map_err(|e| ToRowError::Overflow("gas_used", e.into()))?,
        extra_data: header.extra_data.0.to_vec(),
        mix_hash: header.mix_hash.into(),
        nonce: header.nonce.into(),
        base_fee_per_gas: header
            .base_fee_per_gas
            .map(|b| {
                EvmCurrency::try_from(b)
                    .map_err(|e| ToRowError::Overflow("base_fee_per_gas", e.into()))
            })
            .transpose()?,
    })
}

fn rpc_log_to_row(log: RpcLog, timestamp: Timestamp) -> Result<Log, ToRowError> {
    Ok(Log {
        block_hash: log
            .block_hash
            .ok_or(ToRowError::Missing("block_hash"))?
            .into(),
        block_num: log
            .block_number
            .ok_or(ToRowError::Missing("block_number"))?,
        timestamp,
        tx_index: u32::try_from(
            log.transaction_index
                .ok_or(ToRowError::Missing("transaction_index"))?,
        )
        .map_err(|e| ToRowError::Overflow("transaction_index", e.into()))?,
        tx_hash: log
            .transaction_hash
            .ok_or(ToRowError::Missing("transaction_hash"))?
            .into(),
        log_index: u32::try_from(log.log_index.ok_or(ToRowError::Missing("log_index"))?)
            .map_err(|e| ToRowError::Overflow("log_index", e.into()))?,
        address: log.address().into(),
        topic0: log.topics().get(0).cloned().map(Into::into),
        topic1: log.topics().get(1).cloned().map(Into::into),
        topic2: log.topics().get(2).cloned().map(Into::into),
        topic3: log.topics().get(3).cloned().map(Into::into),
        data: log.data().data.to_vec(),
    })
}

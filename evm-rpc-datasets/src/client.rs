use std::future::Future;
use std::time::Duration;

use alloy_rpc_types::Header;
use alloy_rpc_types::Log as RpcLog;
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
use jsonrpsee::core::client::ClientT;
use jsonrpsee::core::client::Error as RpcError;
use jsonrpsee::core::traits::ToRpcParams;
use jsonrpsee::core::DeserializeOwned;
use jsonrpsee::http_client::HeaderMap;
use jsonrpsee::http_client::HttpClient;
use jsonrpsee::http_client::HttpClientBuilder;
use serde_json::json;
use serde_json::value::RawValue;
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
    client: HttpClient,
    network: String,
}

impl JsonRpcClient {
    pub fn new(url: &str, network: String) -> Result<Self, BoxError> {
        let mut content_type = HeaderMap::new();
        content_type.insert("Content-Type", "application/json".parse().unwrap());
        let client = HttpClientBuilder::default()
            .set_headers(content_type)
            .build(url)?;
        Ok(Self { client, network })
    }

    async fn get_block_by_number(&self, block_number: BlockNum) -> Result<Header, RpcError> {
        let params = json!([format!("0x{:x}", block_number), false]);
        self.call("eth_getBlockByNumber", params).await
    }

    async fn get_logs(&self, block_number: BlockNum) -> Result<Vec<RpcLog>, RpcError> {
        let params = json!([{
            "fromBlock": format!("0x{:x}", block_number),
            "toBlock": format!("0x{:x}", block_number),
        }]);
        self.call("eth_getLogs", params).await
    }

    async fn call<T: DeserializeOwned>(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<T, RpcError> {
        // newtype so we can implement ToRpcParams for it
        struct JsonRpcParams(serde_json::Value);

        impl ToRpcParams for JsonRpcParams {
            fn to_rpc_params(self) -> Result<Option<Box<RawValue>>, serde_json::Error> {
                serde_json::value::to_raw_value(&self.0).map(Some)
            }
        }

        self.client.request(method, JsonRpcParams(params)).await
    }

    async fn block_stream(
        self,
        start_block: u64,
        end_block: u64,
        tx: mpsc::Sender<DatasetRows>,
    ) -> Result<(), BoxError> {
        for block_num in start_block..=end_block {
            let (header, logs) = try_join!(
                self.get_block_by_number(block_num),
                self.get_logs(block_num),
            )?;

            let rows = rpc_to_rows(header, logs, &self.network)?;

            // Send the block and check if the receiver has gone away.
            if tx.send(rows).await.is_err() {
                break;
            }
        }
        Ok(())
    }
}

impl BlockStreamer for JsonRpcClient {
    fn block_stream(
        self,
        start_block: u64,
        end_block: u64,
        tx: mpsc::Sender<common::DatasetRows>,
    ) -> impl Future<Output = Result<(), BoxError>> + Send {
        self.block_stream(start_block, end_block, tx)
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
        block_num: header.number.ok_or(ToRowError::Missing("number"))?,
        timestamp: Timestamp(Duration::from_secs(header.timestamp)),
        hash: header.hash.ok_or(ToRowError::Missing("hash"))?.into(),
        parent_hash: header.parent_hash.into(),
        ommers_hash: header.uncles_hash.into(),
        miner: header.miner.into(),
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
        extra_data: header.extra_data.0.to_vec().into(),
        mix_hash: header
            .mix_hash
            .ok_or(ToRowError::Missing("mix_hash"))?
            .into(),
        nonce: header.nonce.ok_or(ToRowError::Missing("nonce"))?.into(),
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
        data: log.data().data.to_vec().into(),
    })
}

use crate::client::Block;

pub fn create_block_diff(amp_block: &Block, rpc_block: &Block) -> String {
    let mut diff_parts = Vec::new();

    diff_parts.push("differences between amp and rpc data:\n".to_string());

    let mut header_diffs = Vec::new();

    if amp_block.hash != rpc_block.hash {
        header_diffs.push(format!(
            "  hash: amp={}, rpc={}",
            amp_block.hash, rpc_block.hash
        ));
    }
    if amp_block.parent_hash != rpc_block.parent_hash {
        header_diffs.push(format!(
            "  parent_hash: amp={}, rpc={}",
            amp_block.parent_hash, rpc_block.parent_hash
        ));
    }
    if amp_block.ommers_hash != rpc_block.ommers_hash {
        header_diffs.push(format!(
            "  ommers_hash: amp={}, rpc={}",
            amp_block.ommers_hash, rpc_block.ommers_hash
        ));
    }
    if amp_block.miner != rpc_block.miner {
        header_diffs.push(format!(
            "  miner: amp={}, rpc={}",
            amp_block.miner, rpc_block.miner
        ));
    }
    if amp_block.state_root != rpc_block.state_root {
        header_diffs.push(format!(
            "  state_root: amp={}, rpc={}",
            amp_block.state_root, rpc_block.state_root
        ));
    }
    if amp_block.transactions_root != rpc_block.transactions_root {
        header_diffs.push(format!(
            "  transactions_root: amp={}, rpc={}",
            amp_block.transactions_root, rpc_block.transactions_root
        ));
    }
    if amp_block.receipts_root != rpc_block.receipts_root {
        header_diffs.push(format!(
            "  receipts_root: amp={}, rpc={}",
            amp_block.receipts_root, rpc_block.receipts_root
        ));
    }
    if amp_block.logs_bloom != rpc_block.logs_bloom {
        header_diffs.push(format!(
            "  logs_bloom: differs (amp len={}, rpc len={})",
            amp_block.logs_bloom.len(),
            rpc_block.logs_bloom.len()
        ));
    }
    if amp_block.difficulty != rpc_block.difficulty {
        header_diffs.push(format!(
            "  difficulty: amp={}, rpc={}",
            amp_block.difficulty, rpc_block.difficulty
        ));
    }
    if amp_block.gas_limit != rpc_block.gas_limit {
        header_diffs.push(format!(
            "  gas_limit: amp={}, rpc={}",
            amp_block.gas_limit, rpc_block.gas_limit
        ));
    }
    if amp_block.gas_used != rpc_block.gas_used {
        header_diffs.push(format!(
            "  gas_used: amp={}, rpc={}",
            amp_block.gas_used, rpc_block.gas_used
        ));
    }
    if amp_block.timestamp != rpc_block.timestamp {
        header_diffs.push(format!(
            "  timestamp: amp={}, rpc={}",
            amp_block.timestamp, rpc_block.timestamp
        ));
    }
    if amp_block.extra_data != rpc_block.extra_data {
        header_diffs.push(format!(
            "  extra_data: differs (amp len={}, rpc len={})",
            amp_block.extra_data.len(),
            rpc_block.extra_data.len()
        ));
    }
    if amp_block.mix_hash != rpc_block.mix_hash {
        header_diffs.push(format!(
            "  mix_hash: amp={}, rpc={}",
            amp_block.mix_hash, rpc_block.mix_hash
        ));
    }
    if amp_block.nonce != rpc_block.nonce {
        header_diffs.push(format!(
            "  nonce: amp={}, rpc={}",
            amp_block.nonce, rpc_block.nonce
        ));
    }
    if amp_block.base_fee_per_gas != rpc_block.base_fee_per_gas {
        header_diffs.push(format!(
            "  base_fee_per_gas: amp={:?}, rpc={:?}",
            amp_block.base_fee_per_gas, rpc_block.base_fee_per_gas
        ));
    }
    if amp_block.withdrawals_root != rpc_block.withdrawals_root {
        header_diffs.push(format!(
            "  withdrawals_root: amp={:?}, rpc={:?}",
            amp_block.withdrawals_root, rpc_block.withdrawals_root
        ));
    }
    if amp_block.blob_gas_used != rpc_block.blob_gas_used {
        header_diffs.push(format!(
            "  blob_gas_used: amp={:?}, rpc={:?}",
            amp_block.blob_gas_used, rpc_block.blob_gas_used
        ));
    }
    if amp_block.excess_blob_gas != rpc_block.excess_blob_gas {
        header_diffs.push(format!(
            "  excess_blob_gas: amp={:?}, rpc={:?}",
            amp_block.excess_blob_gas, rpc_block.excess_blob_gas
        ));
    }
    if amp_block.parent_beacon_root != rpc_block.parent_beacon_root {
        header_diffs.push(format!(
            "  parent_beacon_root: amp={:?}, rpc={:?}",
            amp_block.parent_beacon_root, rpc_block.parent_beacon_root
        ));
    }
    if amp_block.requests_hash != rpc_block.requests_hash {
        header_diffs.push(format!(
            "  requests_hash: amp={:?}, rpc={:?}",
            amp_block.requests_hash, rpc_block.requests_hash
        ));
    }

    if header_diffs.is_empty() {
        diff_parts.push("  block header: all fields match\n".to_string());
    } else {
        diff_parts.push(format!(
            "  block header: {} field(s) differ:\n",
            header_diffs.len()
        ));
        for diff in header_diffs {
            diff_parts.push(format!("{}\n", diff));
        }
    }

    // Compare transactions
    let amp_tx_count = amp_block.transactions.len();
    let rpc_tx_count = rpc_block.transactions.len();

    if amp_tx_count != rpc_tx_count {
        diff_parts.push(format!(
            "  transactions: count differs (amp={}, rpc={})\n",
            amp_tx_count, rpc_tx_count
        ));
    } else {
        // Find differing transactions
        let mut differing_indices = Vec::new();
        for (idx, (amp_tx, rpc_tx)) in amp_block
            .transactions
            .iter()
            .zip(rpc_block.transactions.iter())
            .enumerate()
        {
            if amp_tx.tx_hash != rpc_tx.tx_hash
                || amp_tx.from != rpc_tx.from
                || amp_tx.to != rpc_tx.to
                || amp_tx.value != rpc_tx.value
                || amp_tx.input != rpc_tx.input
                || amp_tx.gas_used != rpc_tx.gas_used
                || amp_tx.status != rpc_tx.status
            {
                differing_indices.push(idx);
            }
        }

        if differing_indices.is_empty() {
            diff_parts.push(format!(
                "  transactions: {} total, all match\n",
                amp_tx_count
            ));
        } else {
            let preview = if differing_indices.len() > 10 {
                format!(
                    "{:?}... ({} more)",
                    &differing_indices[..10],
                    differing_indices.len() - 10
                )
            } else {
                format!("{:?}", differing_indices)
            };
            diff_parts.push(format!(
                "  transactions: {} total, {} differ at indices {}\n",
                amp_tx_count,
                differing_indices.len(),
                preview
            ));
        }
    }

    // Compare logs
    let amp_log_count = amp_block.logs.len();
    let rpc_log_count = rpc_block.logs.len();

    if amp_log_count != rpc_log_count {
        diff_parts.push(format!(
            "  logs: count differs (amp={}, rpc={})\n",
            amp_log_count, rpc_log_count
        ));
    } else {
        // Find differing logs
        let mut differing_indices = Vec::new();
        for (idx, (amp_log, rpc_log)) in
            amp_block.logs.iter().zip(rpc_block.logs.iter()).enumerate()
        {
            if amp_log.address != rpc_log.address
                || amp_log.topics != rpc_log.topics
                || amp_log.data != rpc_log.data
            {
                differing_indices.push(idx);
            }
        }

        if differing_indices.is_empty() {
            diff_parts.push(format!("  logs: {} total, all match\n", amp_log_count));
        } else {
            let preview = if differing_indices.len() > 10 {
                format!(
                    "{:?}... ({} more)",
                    &differing_indices[..10],
                    differing_indices.len() - 10
                )
            } else {
                format!("{:?}", differing_indices)
            };
            diff_parts.push(format!(
                "  logs: {} total, {} differ at indices {}\n",
                amp_log_count,
                differing_indices.len(),
                preview
            ));
        }
    }

    diff_parts.concat()
}

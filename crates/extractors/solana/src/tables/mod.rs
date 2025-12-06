use common::{BoxResult, RawDatasetRows, metadata::segments::BlockRange};
use serde::Deserialize;
use solana_clock::Slot;

use crate::rpc_client::{EncodedTransaction, UiConfirmedBlock, UiMessage};

pub mod block_headers;
pub mod messages;
pub mod transactions;

pub fn all(network: &str) -> Vec<common::Table> {
    vec![
        block_headers::table(network.to_string()),
        transactions::table(network.to_string()),
        messages::table(network.to_string()),
    ]
}

pub(crate) fn convert_of_data_to_db_rows(
    mut block: crate::of1_client::DecodedBlock,
    network: &str,
) -> BoxResult<RawDatasetRows> {
    let of_transactions = std::mem::take(&mut block.transactions);
    let of_transactions_meta = std::mem::take(&mut block.transaction_metas);
    let mut db_transactions = Vec::new();
    let mut db_messages = Vec::new();

    let slot = block.slot;

    for (tx_idx, (tx, tx_meta)) in of_transactions
        .into_iter()
        .zip(of_transactions_meta)
        .enumerate()
    {
        let tx_idx: u32 = tx_idx.try_into().expect("conversion error");
        let tx_message = tx.message.clone();

        let db_tx = transactions::Transaction::from_of1_transaction(slot, tx_idx, tx, tx_meta);
        db_transactions.push(db_tx);

        let db_message = messages::Message::from_of1_message(slot, tx_idx, tx_message);
        db_messages.push(db_message);
    }

    let header = block_headers::BlockHeader::from_of1_block(block);

    let range = BlockRange {
        // Using the slot as a block number since we don't skip empty slots.
        numbers: slot..=slot,
        network: network.to_string(),
        hash: header.block_hash.into(),
        // Previous slot could be skipped, do not set prev_hash here.
        prev_hash: None,
    };

    let block_headers_row = {
        let mut builder = block_headers::BlockHeaderRowsBuilder::new();
        builder.append(&header);
        builder.build(range.clone())?
    };
    let transactions_row = {
        let mut builder =
            transactions::TransactionRowsBuilder::with_capacity(db_transactions.len());
        for tx in db_transactions {
            builder.append(&tx);
        }
        builder.build(range.clone())?
    };
    let messages_row = {
        let mut builder =
            crate::tables::messages::MessageRowsBuilder::with_capacity(db_messages.len());
        for message in db_messages {
            builder.append(&message);
        }
        builder.build(range)?
    };

    Ok(RawDatasetRows::new(vec![
        block_headers_row,
        transactions_row,
        messages_row,
    ]))
}

pub(crate) fn convert_rpc_block_to_db_rows(
    slot: Slot,
    mut block: UiConfirmedBlock,
    network: &str,
) -> BoxResult<RawDatasetRows> {
    let rpc_transactions = std::mem::take(&mut block.transactions).unwrap_or_default();
    let mut db_transactions = Vec::new();
    let mut db_messages = Vec::new();

    for (tx_idx, tx_with_meta) in rpc_transactions.into_iter().enumerate() {
        // These should be set when requesting a block.
        let EncodedTransaction::Json(ref tx) = tx_with_meta.transaction else {
            let err = format!("unexpected transaction encoding at slot {slot}, tx index {tx_idx}",);
            return Err(err.into());
        };
        let UiMessage::Raw(ref message) = tx.message else {
            let err = format!("unexpected message format at slot {slot}, tx index {tx_idx}",);
            return Err(err.into());
        };

        let found_parsed_instruction = tx_with_meta.meta.as_ref().is_some_and(|meta| {
            meta.inner_instructions
                .as_ref()
                .map(|inners| inners)
                .is_some_and(|inners| {
                    inners.iter().any(|inner| {
                        inner
                            .instructions
                            .iter()
                            .any(|inst| matches!(inst, crate::rpc_client::UiInstruction::Parsed(_)))
                    })
                })
        });

        if found_parsed_instruction {
            let err = format!(
                "found parsed inner instruction at slot {slot}, tx index {tx_idx}, which is not supported"
            );
            return Err(err.into());
        }

        let tx_idx: u32 = tx_idx.try_into().expect("conversion error");
        let db_tx = transactions::Transaction::from_rpc_transaction(
            slot,
            tx_idx,
            tx,
            tx_with_meta.meta.as_ref(),
        );
        db_transactions.push(db_tx);

        let db_message = messages::Message::from_rpc_message(slot, tx_idx, message);
        db_messages.push(db_message);
    }

    let header = block_headers::BlockHeader::from_rpc_block(slot, &block);

    let range = BlockRange {
        // Using the slot as a block number since we don't skip empty slots.
        numbers: slot..=slot,
        network: network.to_string(),
        hash: header.block_hash.into(),
        // Previous slot could be skipped, do not set prev_hash here.
        prev_hash: None,
    };

    let block_headers_row = {
        let mut builder = block_headers::BlockHeaderRowsBuilder::new();
        builder.append(&header);
        builder.build(range.clone())?
    };
    let transactions_row = {
        let mut builder =
            transactions::TransactionRowsBuilder::with_capacity(db_transactions.len());
        for tx in db_transactions {
            builder.append(&tx);
        }
        builder.build(range.clone())?
    };
    let messages_row = {
        let mut builder =
            crate::tables::messages::MessageRowsBuilder::with_capacity(db_messages.len());
        for message in db_messages {
            builder.append(&message);
        }
        builder.build(range)?
    };

    Ok(RawDatasetRows::new(vec![
        block_headers_row,
        transactions_row,
        messages_row,
    ]))
}

pub(crate) fn empty_db_rows(slot: Slot, network: &str) -> BoxResult<RawDatasetRows> {
    let range = BlockRange {
        // Using the slot as a block number since we don't skip empty slots.
        numbers: slot..=slot,
        network: network.to_string(),
        hash: [0u8; 32].into(),
        // Previous slot could be skipped, do not set prev_hash here.
        prev_hash: None,
    };

    let header = block_headers::BlockHeader::empty(slot);

    let block_headers_row = {
        let mut builder = block_headers::BlockHeaderRowsBuilder::new();
        builder.append(&header);
        builder.build(range.clone())?
    };

    let transactions_row = {
        let builder = transactions::TransactionRowsBuilder::with_capacity(0);
        builder.build(range)?
    };

    Ok(RawDatasetRows::new(vec![
        block_headers_row,
        transactions_row,
    ]))
}

#[derive(Debug, Default, Deserialize, Clone)]
pub(crate) struct Instruction {
    pub(crate) program_id_index: u8,
    pub(crate) accounts: Vec<u8>,
    pub(crate) data: String,
    pub(crate) stack_height: Option<u32>,
}

use datasets_common::block_range::BlockRange;
use datasets_raw::rows::Rows;
use solana_clock::Slot;

use crate::{
    error::RowConversionError,
    rpc_client::{EncodedTransaction, UiConfirmedBlock, UiMessage},
};

pub mod block_headers;
pub mod instructions;
pub mod messages;
pub mod transactions;

/// Maximum number of ASCII characters in a base58-encoded 32-byte hash.
pub(crate) const BASE58_ENCODED_HASH_LEN: usize = 44;

pub fn all(network: &str) -> Vec<datasets_common::dataset::Table> {
    vec![
        block_headers::table(network.to_string()),
        transactions::table(network.to_string()),
        messages::table(network.to_string()),
        instructions::table(network.to_string()),
    ]
}

pub(crate) fn convert_of_data_to_db_rows(
    mut block: crate::of1_client::DecodedBlock,
    network: &str,
) -> Result<Rows, RowConversionError> {
    let of_transactions = std::mem::take(&mut block.transactions);
    let of_transactions_meta = std::mem::take(&mut block.transaction_metas);
    let mut db_transactions = Vec::new();
    let mut db_messages = Vec::new();
    let mut db_instructions = Vec::new();

    let slot = block.slot;

    for (tx_idx, (tx, tx_meta)) in of_transactions
        .into_iter()
        .zip(of_transactions_meta)
        .enumerate()
    {
        let tx_idx: u32 = tx_idx.try_into().expect("conversion error");
        let tx_message = tx.message.clone();

        let db_tx = transactions::Transaction::from_of1_transaction(slot, tx_idx, tx_meta, tx);
        if let Some(meta) = db_tx.tx_meta.as_ref()
            && let Some(inner_instructions) = meta.inner_instructions.as_ref()
        {
            for inner in inner_instructions {
                db_instructions.extend_from_slice(inner);
            }
        }
        db_transactions.push(db_tx);

        let db_message = messages::Message::from_of1_message(slot, tx_idx, tx_message);
        db_instructions.extend_from_slice(&db_message.instructions);
        db_messages.push(db_message);
    }

    let range = BlockRange {
        // Using the slot as a block number since we don't skip empty slots.
        numbers: slot..=slot,
        network: network.to_string(),
        hash: block.blockhash.into(),
        // Previous slot could be skipped, do not set prev_hash here.
        prev_hash: None,
    };

    let block_headers_row = {
        let mut builder = block_headers::BlockHeaderRowsBuilder::new();
        let header = block_headers::BlockHeader::from_of1_block(block);
        builder.append(&header);
        builder
            .build(range.clone())
            .map_err(RowConversionError::TableBuild)?
    };
    let transactions_row = {
        let mut builder =
            transactions::TransactionRowsBuilder::with_capacity(db_transactions.len());
        for tx in db_transactions {
            builder.append(&tx);
        }
        builder
            .build(range.clone())
            .map_err(RowConversionError::TableBuild)?
    };
    let messages_row = {
        let mut builder =
            crate::tables::messages::MessageRowsBuilder::with_capacity(db_messages.len());
        for message in db_messages {
            builder.append(&message);
        }
        builder
            .build(range.clone())
            .map_err(RowConversionError::TableBuild)?
    };
    let instructions_row = {
        let mut builder = crate::tables::instructions::InstructionRowsBuilder::with_capacity(
            db_instructions.len(),
        );
        for instruction in db_instructions {
            builder.append(&instruction);
        }
        builder
            .build(range)
            .map_err(RowConversionError::TableBuild)?
    };

    Ok(Rows::new(vec![
        block_headers_row,
        transactions_row,
        messages_row,
        instructions_row,
    ]))
}

pub(crate) fn convert_rpc_block_to_db_rows(
    slot: Slot,
    mut block: UiConfirmedBlock,
    network: &str,
) -> Result<Rows, RowConversionError> {
    let rpc_transactions = std::mem::take(&mut block.transactions).unwrap_or_default();
    let mut db_transactions = Vec::new();
    let mut db_messages = Vec::new();

    for (tx_idx, tx_with_meta) in rpc_transactions.into_iter().enumerate() {
        // These should be set when requesting a block.
        let EncodedTransaction::Json(ref tx) = tx_with_meta.transaction else {
            return Err(RowConversionError::UnexpectedTransactionEncoding { slot, tx_idx });
        };
        let UiMessage::Raw(ref message) = tx.message else {
            return Err(RowConversionError::UnexpectedMessageFormat { slot, tx_idx });
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
            return Err(RowConversionError::ParsedInnerInstructionNotSupported { slot, tx_idx });
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

    let block_hash: [u8; 32] = bs58::decode(&block.blockhash)
        .into_vec()
        .map(TryInto::try_into)
        .expect("failed to decode block hash from base58")
        .expect("decoded block hash should be 32 bytes");

    let range = BlockRange {
        // Using the slot as a block number since we don't skip empty slots.
        numbers: slot..=slot,
        network: network.to_string(),
        hash: block_hash.into(),
        // Previous slot could be skipped, do not set prev_hash here.
        prev_hash: None,
    };

    let block_headers_row = {
        let mut builder = block_headers::BlockHeaderRowsBuilder::new();
        let header = block_headers::BlockHeader::from_rpc_block(slot, &block);
        builder.append(&header);
        builder
            .build(range.clone())
            .map_err(RowConversionError::TableBuild)?
    };
    let transactions_row = {
        let mut builder =
            transactions::TransactionRowsBuilder::with_capacity(db_transactions.len());
        for tx in db_transactions {
            builder.append(&tx);
        }
        builder
            .build(range.clone())
            .map_err(RowConversionError::TableBuild)?
    };
    let messages_row = {
        let mut builder =
            crate::tables::messages::MessageRowsBuilder::with_capacity(db_messages.len());
        for message in db_messages {
            builder.append(&message);
        }
        builder
            .build(range)
            .map_err(RowConversionError::TableBuild)?
    };

    Ok(Rows::new(vec![
        block_headers_row,
        transactions_row,
        messages_row,
    ]))
}

pub(crate) fn empty_db_rows(slot: Slot, network: &str) -> Result<Rows, RowConversionError> {
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
        builder
            .build(range.clone())
            .map_err(RowConversionError::TableBuild)?
    };

    let transactions_row = {
        let builder = transactions::TransactionRowsBuilder::with_capacity(0);
        builder
            .build(range.clone())
            .map_err(RowConversionError::TableBuild)?
    };

    let messages_row = {
        let builder = crate::tables::messages::MessageRowsBuilder::with_capacity(0);
        builder
            .build(range.clone())
            .map_err(RowConversionError::TableBuild)?
    };

    let instructions_row = {
        let builder = crate::tables::instructions::InstructionRowsBuilder::with_capacity(0);
        builder
            .build(range)
            .map_err(RowConversionError::TableBuild)?
    };

    Ok(Rows::new(vec![
        block_headers_row,
        transactions_row,
        messages_row,
        instructions_row,
    ]))
}

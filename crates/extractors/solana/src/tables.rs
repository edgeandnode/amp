use datasets_common::block_range::BlockRange;
use datasets_raw::rows::Rows;
use solana_clock::Slot;

use crate::error::RowConversionError;

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

/// A Solana slot that contains a confirmed block.
pub(crate) struct NonEmptySlot {
    pub(crate) slot: Slot,
    pub(crate) parent_slot: Slot,
    pub(crate) blockhash: [u8; 32],
    pub(crate) prev_blockhash: [u8; 32],
    pub(crate) block_height: Option<u64>,
    pub(crate) blocktime: Option<i64>,
    pub(crate) transactions: Vec<transactions::Transaction>,
    pub(crate) messages: Vec<messages::Message>,
}

pub(crate) fn convert_slot_to_db_rows(
    non_empty_slot: NonEmptySlot,
    network: &str,
) -> Result<Rows, RowConversionError> {
    let NonEmptySlot {
        slot,
        parent_slot,
        blockhash,
        prev_blockhash,
        block_height,
        blocktime,
        transactions,
        messages,
    } = non_empty_slot;

    let range = BlockRange {
        // Using the slot as a block number since we don't skip empty slots.
        numbers: slot..=slot,
        network: network.to_string(),
        hash: blockhash.into(),
        // Previous slot could be skipped, do not set prev_hash.
        prev_hash: None,
    };

    let block_headers_row = {
        let mut builder = block_headers::BlockHeaderRowsBuilder::new();

        let header = block_headers::BlockHeader {
            block_height,
            slot,
            parent_slot,
            block_hash: bs58::encode(blockhash).into_string(),
            previous_block_hash: bs58::encode(prev_blockhash).into_string(),
            block_time: blocktime,
        };

        builder.append(&header);
        builder
            .build(range.clone())
            .map_err(RowConversionError::TableBuild)?
    };

    let transactions_row = {
        let mut builder = transactions::TransactionRowsBuilder::with_capacity(transactions.len());
        for tx in &transactions {
            builder.append(tx);
        }
        builder
            .build(range.clone())
            .map_err(RowConversionError::TableBuild)?
    };

    let messages_row = {
        let mut builder = messages::MessageRowsBuilder::with_capacity(messages.len());
        for message in &messages {
            builder.append(message);
        }
        builder
            .build(range.clone())
            .map_err(RowConversionError::TableBuild)?
    };

    let instructions_row = {
        let all_instructions: Vec<_> = transactions
            .iter()
            .filter_map(|tx| {
                tx.transaction_status_meta
                    .as_ref()
                    .and_then(|meta| meta.inner_instructions.as_ref())
                    .map(|inner_instructions| {
                        inner_instructions
                            .iter()
                            .flat_map(|instructions| instructions.iter())
                    })
            })
            .flatten()
            .chain(messages.iter().flat_map(|msg| msg.instructions.iter()))
            .collect();

        let mut builder =
            instructions::InstructionRowsBuilder::with_capacity(all_instructions.len());
        for instruction in all_instructions {
            builder.append(instruction);
        }
        builder
            .build(range.clone())
            .map_err(RowConversionError::TableBuild)?
    };

    Ok(Rows::new(vec![
        block_headers_row,
        transactions_row,
        messages_row,
        instructions_row,
    ]))
}

pub(crate) fn empty_db_rows(slot: Slot, network: &str) -> Result<Rows, RowConversionError> {
    let range = BlockRange {
        // Using the slot as a block number since we don't skip empty slots.
        numbers: slot..=slot,
        network: network.to_string(),
        hash: [0u8; 32].into(),
        // Previous slot could be skipped, do not set prev_hash.
        prev_hash: None,
    };

    let header = block_headers::BlockHeader {
        slot,
        parent_slot: slot.saturating_sub(1),
        ..Default::default()
    };

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

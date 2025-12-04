use common::{BoxResult, RawDatasetRows, metadata::segments::BlockRange};
use solana_clock::Slot;

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
    mut block: crate::extractor::DecodedBlock,
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

        let db_tx = transactions::Transaction::from_of_transaction(slot, tx_idx, tx, tx_meta);
        db_transactions.push(db_tx);

        let db_message = messages::Message::from_of_message(slot, tx_idx, tx_message);
        db_messages.push(db_message);
    }

    let header = block_headers::BlockHeader::from_of_block(block);

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

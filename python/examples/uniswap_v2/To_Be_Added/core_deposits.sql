SELECT
    'DEPOSIT-' || arrow_cast(encode(ime.transaction_hash::bytea, 'hex'), 'Utf8') || '-' || arrow_cast(ime.log_index, 'Utf8') AS id,
    idap.id AS psm__id,
    ime.contract_address AS pool__id,
    '' AS user__id,
    t.value AS output_token_amount,
    input_token_0,
    ime.amount0 AS amount0,
    input_token_1,
    ime.amount1 AS amount1,
    ime.block_num,  -- Ensure this is correct
    ime.transaction_index,
    ime.transaction_hash,
    ime.log_index,
    ime.timestamp AS block_timestamp,
    now() AS _load_timestamp_utc,
    now() AS _last_run_timestamp_utc
FROM {core_psm_meta} idap,
     events_mint ime
LEFT JOIN {core_liquidity_pools} ilp ON ime.contract_address = ilp.id
INNER JOIN events_transfer t ON to_char(to_timestamp_nanos(ime.timestamp, 'YYYY-MM-dd HH:mm:ssZ'), '%Y-%m-%d') = to_char(to_timestamp_nanos(t.timestamp,'YYYY-MM-dd HH:mm:ssZ'),'%Y-%m-%d')
AND ime.transaction_hash = t.transaction_hash
AND ime.log_index = t.log_index + 2
AND t.sender = arrow_cast('0000000000000000000000000000000000000000', 'Utf8')
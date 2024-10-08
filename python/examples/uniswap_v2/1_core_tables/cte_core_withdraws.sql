    SELECT
        'WITHDRAW-' || arrow_cast(encode(ibe.transaction_hash::bytea, 'hex'), 'Utf8') || '-' || arrow_cast(ibe.log_index, 'Utf8') AS id,           
        'uniswap-v2_ethereum_mainnet_allium'          AS psm__id,
        ibe.contract_address AS pool__id,
        '' AS user__id, 
        ilp.input_token_0,
        ibe.amount0,
        ilp.input_token_1,
        ibe.amount1,
        t.value AS output_token_amount,

        ibe.block_num,
        ibe.transaction_index,
        ibe.transaction_hash,
        ibe.log_index,
        ibe.timestamp AS block_timestamp,
        NOW() AS _load_timestamp_utc,
        NOW() AS _last_run_timestamp_utc,
        ROW_NUMBER() OVER (
          PARTITION BY ibe.block_num, ibe.transaction_index, ibe.log_index 
          ORDER BY (ibe.log_index - t.log_index)
           ) AS rn
    FROM 
    events_burn ibe 
    LEFT JOIN {core_liquidity_pools} ilp ON ibe.contract_address = ilp.id
    INNER JOIN events_transfer t ON
        ibe.timestamp = t.timestamp AND
        ibe.transaction_hash = t.transaction_hash AND
        ibe.log_index > t.log_index AND
        ibe.contract_address = t.contract_address AND
        t.`to` = arrow_cast('0x0000000000000000000000000000000000000000', 'Utf8')
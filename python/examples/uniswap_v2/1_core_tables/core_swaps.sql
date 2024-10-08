    SELECT
        'SWAP-0x' || arrow_cast(encode(ise.transaction_hash::bytea, 'hex'), 'Utf8') || '-' || arrow_cast(ise.log_index, 'Utf8') AS id,           
        'uniswap-v2_ethereum_mainnet_allium'          AS psm__id,
        ise.contract_address AS pool__id,
        '' AS user__id,
        
        CASE 
            WHEN ise.amount0In > 0 THEN ise.amount0In 
            ELSE ise.amount1In 
        END AS amount_in,
        
        CASE 
            WHEN ise.amount0In > 0 THEN ABS(ise.amount1Out) 
            ELSE ABS(ise.amount0Out) 
        END AS amount_out,
        
        CASE 
            WHEN ise.amount0In > 0 THEN ilp.input_token_0
            ELSE ilp.input_token_1  
        END AS token_in__id,
        
        CASE 
            WHEN ise.amount0In > 0 THEN ilp.input_token_1
            ELSE ilp.input_token_0  
        END AS token_out__id,
        
        ise.block_num,
        ise.transaction_index,
        ise.transaction_hash,
        ise.log_index,
        ise.timestamp AS block_timestamp,
        now() AS _load_timestamp_utc,
        now() AS _last_run_timestamp_utc
    FROM events_swap ise 
    LEFT JOIN {core_liquidity_pools} ilp ON ise.contract_address = ilp.id
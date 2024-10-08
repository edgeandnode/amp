
        SELECT 
            ipce.param_pair AS id
            , idap.id                                       AS psm__id
            , nas.name
            , nas._symbol                                   AS symbol
            , arrow_cast(encode(param_token0::bytea, 'hex'), 'Utf8') AS input_token_0
            , arrow_cast(encode(param_token1::bytea, 'hex'), 'Utf8') AS input_token_1
            , block_number
            , transaction_index
            , transaction_hash
            , log_index
            , timestamp AS block_timestamp
            , now() AS _load_timestamp_utc
            , now() AS _last_run_timestamp_utc
        FROM {core_psm_meta} idap, 
             events_factory_contract_pair_created ipce
        LEFT JOIN {cte_name_and_symbol} nas ON ipce.param_pair = nas.pool
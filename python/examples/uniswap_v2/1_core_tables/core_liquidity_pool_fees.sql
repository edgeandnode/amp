        SELECT
            arrow_cast(encode(pce.param_pair::bytea, 'hex'), 'Utf8') || '-FIXED_TRADING_FEE-SUPPLY'   AS id
            
            , 'uniswap-v2_ethereum_mainnet_allium'          AS psm__id
            , pce.param_pair                                AS pool__id

            , 'SUPPLY'                                      AS fee_recipient
            , 'FIXED_TRADING_FEE'                           AS fee_type

            , pce.block_number
            , pce.transaction_index
            , pce.transaction_hash
            , pce.log_index
            , pce.timestamp AS block_timestamp
            , pce._load_timestamp_utc
            , pce._last_run_timestamp_utc
        FROM events_factory_contract_pair_created pce

        UNION ALL

        SELECT
            arrow_cast(encode(pce.param_pair::bytea, 'hex'), 'Utf8') || '-FIXED_TRADING_FEE-PROTOCOL' AS id
            
            , 'uniswap-v2_ethereum_mainnet_allium'          AS psm__id
            , pce.param_pair                                AS pool__id

            , 'PROTOCOL'                                    AS fee_recipient
            , 'FIXED_TRADING_FEE'                           AS fee_type

            , pce.block_number
            , pce.transaction_index
            , pce.transaction_hash
            , pce.log_index
            , pce.timestamp AS block_timestamp
            , pce._load_timestamp_utc
            , pce._last_run_timestamp_utc
        FROM events_factory_contract_pair_created pce
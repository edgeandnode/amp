        SELECT
             'uniswap-v2_ethereum_mainnet_allium' AS psm__id
            , csd.pool__id
            , csd.token__id

            , csd.delta

            , csd.block_number AS block_num
            , csd.transaction_index
            , csd.transaction_hash
            , csd.log_index
            , csd.block_timestamp
            , csd._load_timestamp_utc
            , csd._last_run_timestamp_utc
        FROM {cte_combined_sync_deltas} csd
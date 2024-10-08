        SELECT * FROM (
                        SELECT
                irs.contract_address AS pool__id,
                ilp.input_token_0 AS token__id,
                (irs.reserve0 - COALESCE(
                    LAG(irs.reserve0) OVER (
                        PARTITION BY irs.contract_address ORDER BY irs.block_number, irs.transaction_index, irs.log_index
                    ), 0)) AS delta,
                irs.block_number,
                irs.transaction_index,
                irs.transaction_hash,
                irs.log_index,
                irs.timestamp AS block_timestamp,
                irs._load_timestamp_utc,
                irs._last_run_timestamp_utc
            FROM auxiliary_partitioned_events_pair_contract_sync irs
            LEFT JOIN {core_liquidity_pools} ilp 
            ON irs.contract_address = ilp.id
        )
        UNION ALL
        SELECT * FROM (

                SELECT
        irs.contract_address AS pool__id,
        ilp.input_token_1 AS token__id,
        (irs.reserve1 - COALESCE(
            LAG(irs.reserve1) OVER (
                PARTITION BY irs.contract_address ORDER BY irs.block_number, irs.transaction_index, irs.log_index
            ), 0)) AS delta,
        irs.block_number,
        irs.transaction_index,
        irs.transaction_hash,
        irs.log_index,
        irs.timestamp AS block_timestamp,
        irs._load_timestamp_utc,
        irs._last_run_timestamp_utc
    FROM auxiliary_partitioned_events_pair_contract_sync irs
    LEFT JOIN {core_liquidity_pools} ilp 
    ON irs.contract_address = ilp.id
        )
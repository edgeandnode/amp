        SELECT
        contract_address,
        DATE_TRUNC('day', timestamp) AS day,
        block_num AS block_number,
        log_index,
        ROW_NUMBER() OVER (
            PARTITION BY DATE_TRUNC('day', timestamp), contract_address
            ORDER BY block_num DESC, log_index DESC
        ) AS row_num,
        transaction_index,
        transaction_hash,
        timestamp,
        _load_timestamp_utc,
        _last_run_timestamp_utc,
        reserve0,
        reserve1
    FROM 
        events_sync
    
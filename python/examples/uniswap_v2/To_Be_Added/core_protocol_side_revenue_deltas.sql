        SELECT
            id 

            , delta
            , psm__id
            , pool__id
            , token__id
            
            , block_num
            , transaction_index
            , transaction_hash
            , log_index
            , block_timestamp
            , _load_timestamp_utc
            , _last_run_timestamp_utc
        FROM {core_volume_deltas}
        WHERE 1 = 0 -- Fee is always 0, this just prevents us from processing the entire table when compiling metrics

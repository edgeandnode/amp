SELECT
            id 
            , delta  -- this should be delta * ((0.3)::FLOAT / 100) AS delta
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
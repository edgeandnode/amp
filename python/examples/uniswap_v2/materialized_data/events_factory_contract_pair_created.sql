WITH token_pairs_created AS (
        SELECT
            l.block_num AS block_num,
            l.timestamp,
            l.tx_index AS transaction_index, 
            l.tx_hash AS transaction_hash,
            l.log_index,
            l.address AS contract_address,
            evm_decode(l.topic1, l.topic2, l.topic3, l.data, 'PairCreated(address indexed token0, address indexed token1, address pair, uint256 )') AS params
        FROM
            eth_firehose.logs l
        WHERE
            l.address = arrow_cast(x'5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f', 'FixedSizeBinary(20)')
            AND l.topic0 = arrow_cast(x'0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9', 'FixedSizeBinary(32)')
            AND l.timestamp > arrow_cast('2020-05-04', 'Date32')
)
SELECT        
    contract_address,
    block_num AS block_number,
    transaction_hash,
    transaction_index,
    log_index,
    timestamp,
    params['token0'] AS param_token0,
    params['token1'] AS param_token1,
    params['pair'] AS param_pair,
    now() AS _load_timestamp_utc,
    now() AS _last_run_timestamp_utc
FROM token_pairs_created


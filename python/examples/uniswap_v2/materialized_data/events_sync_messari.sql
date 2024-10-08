WITH uniswap_token_pairs AS
    (
            SELECT
                evm_decode(l.topic1, l.topic2, l.topic3, l.data, 'PairCreated(address indexed token0, address indexed token1, address pair, uint256 )')['pair'] AS param_pair_contract_address
            FROM
                eth_firehose.logs l
            WHERE
                l.address = arrow_cast(x'5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f', 'FixedSizeBinary(20)')
                AND l.topic0 = arrow_cast(x'0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9', 'FixedSizeBinary(32)')
                AND l.timestamp >= arrow_cast('2020-05-04', 'Date32')  -- Ensure consistency
    )

SELECT 
    block_num,
    timestamp,
    transaction_index,
    transaction_hash,
    log_index,
    contract_address,
    params['reserve0'] AS reserve0, 
    params['reserve1'] AS reserve1
FROM uniswap_events.sync l
WHERE l.timestamp >= arrow_cast('2024-01-01', 'Date32')
AND l.contract_address IN 
(SELECT param_pair_contract_address
FROM  uniswap_token_pairs  )
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
    params['sender'] AS sender, 
    params['amount0In'] AS amount0In,
    params['amount1In'] AS amount1In,
    params['amount0Out'] AS amount0Out,
    params['amount1Out'] AS amount1Out,
    params['to'] AS `to`
FROM uniswap_events.swap l
JOIN
    uniswap_token_pairs u ON l.contract_address = u.param_pair_contract_address 
WHERE l.timestamp >= arrow_cast('2020-05-04', 'Date32')
LIMIT 200
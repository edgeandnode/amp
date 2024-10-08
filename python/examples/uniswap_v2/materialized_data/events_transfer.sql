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
    params['from'] AS sender, 
    params['to'] AS `to`,
    params['value'] AS value
FROM (
SELECT
    l.block_num AS block_num,
    l.timestamp,
    l.tx_index AS transaction_index, 
    l.tx_hash AS transaction_hash,
    l.log_index,
    l.address AS contract_address,
    evm_decode(l.topic1, l.topic2, l.topic3, l.data, 'Transfer(address indexed from, address indexed to, uint256 value)') AS params
FROM
    eth_firehose.logs l
JOIN
    uniswap_token_pairs u ON l.address = u.param_pair_contract_address 
WHERE
    l.topic0 = arrow_cast(x'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef', 'FixedSizeBinary(32)')
    AND l.timestamp >= arrow_cast('2024-05-04', 'Date32')
)
SELECT
  t.block_num,
  t.tx_hash,
  t.from,
  t.value
FROM {{ ref("eth", "transactions") }} AS t
WHERE t.value >= CAST({{ var("min", "1") }} AS BIGINT)

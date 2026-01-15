import { defineDataset } from "@edgeandnode/amp"

export default defineDataset(() => ({
  name: "joins",
  network: "mainnet",
  dependencies: {
    eth_rpc: "_/eth_rpc@0.0.0",
  },
  tables: {
    // This table uses JOIN which is now supported incrementally
    join_blocks_txs: {
      sql: `
        SELECT
          b.block_num,
          b.hash as block_hash,
          b.miner,
          t.tx_hash,
          t.tx_index,
          t.from as tx_from,
          t.to as tx_to
        FROM eth_rpc.blocks b
        JOIN eth_rpc.transactions t ON b.block_num = t.block_num
      `,
      network: "mainnet",
    },
    // Test that _block_num is propagated through SubqueryAlias (CTE) in JOINs
    subquery_alias: {
      sql: `
        WITH tx_value AS (
          SELECT value FROM eth_rpc.transactions WHERE value = '99227573441855374'
        )
        SELECT block_num, tv.value FROM tx_value tv, eth_rpc.blocks
      `,
      network: "mainnet",
    },
  },
  functions: {},
}))

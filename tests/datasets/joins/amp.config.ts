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
  },
  functions: {},
}))

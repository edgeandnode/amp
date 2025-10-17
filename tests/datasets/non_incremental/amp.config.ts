import { defineDataset } from "@edgeandnode/amp"

export default defineDataset(() => ({
  name: "non_incremental",
  network: "mainnet",
  version: "0.1.0",
  dependencies: {
    eth_rpc: "_/eth_rpc@0.0.0",
  },
  tables: {
    // This table uses JOIN which is a non-incremental operation
    join_blocks_txs: {
      sql: `
        SELECT
          b.block_num,
          b.hash as block_hash,
          b.miner
        FROM eth_rpc.blocks b
        JOIN eth_rpc.transactions t ON b.block_num = t.block_num
      `,
      network: "mainnet",
    },
  },
  functions: {},
}))

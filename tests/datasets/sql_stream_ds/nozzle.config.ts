import { defineDataset } from "nozzl"

export default defineDataset(() => ({
  name: "sql_stream_ds",
  network: "mainnet",
  version: "0.1.0",
  dependencies: {
    eth_rpc: {
      owner: "graphprotocol",
      name: "eth_rpc",
      version: "0.0.0",
    },
  },
  tables: {
    even_blocks: {
      sql: `select *
    from eth_rpc.blocks
    where
    block_num % 2 = 0`,
      network: "mainnet",
    },
    even_blocks_hashes_only: {
      sql: `select hash
      from eth_rpc.blocks
      where
      block_num % 2 = 0`,
      network: "mainnet",
    },
  },
  functions: {},
}))

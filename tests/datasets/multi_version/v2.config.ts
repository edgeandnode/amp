import { defineDataset } from "nozzl"

export default defineDataset((ctx) => ({
  name: "multi_version",  
  network: "mainnet",
  version: "0.0.2",
  dependencies: {
    eth_firehose: {
      owner: "graphprotocol",
      name: "eth_firehose",
      version: "0.1.0",
    },
  },
  tables: {
    blocks: {
      sql: "SELECT block_num, gas_limit, hash, parent_hash FROM eth_firehose.blocks",
      network: "mainnet",
    },
  },
  functions: {},
}))

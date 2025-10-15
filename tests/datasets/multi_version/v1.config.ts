import { defineDataset } from "@edgeandnode/amp"

export default defineDataset(() => ({
  name: "multi_version",
  network: "mainnet",
  version: "0.0.1",
  dependencies: {
    eth_firehose: {
      name: "eth_firehose",
      version: "0.0.0",
    },
  },
  tables: {
    blocks: {
      sql: "SELECT block_num, gas_limit, gas_used, nonce, miner, hash, parent_hash FROM eth_firehose.blocks",
      network: "mainnet",
    },
  },
  functions: {},
}))

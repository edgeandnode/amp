import { defineDataset } from "@edgeandnode/amp"

export default defineDataset((ctx) => ({
  namespace: "freecandylabs",
  name: "function_in_table",
  network: "mainnet",
  dependencies: {
    eth_firehose: "_/eth_firehose@0.0.0",
  },
  tables: {
    blocks_with_suffix: {
      sql: `
        SELECT 
          block_num, 
          self.addSuffix(lower(encode(arrow_cast(miner, 'Binary'), 'hex'))) as miner_tagged,
          hash 
        FROM eth_firehose.blocks
      `,
      network: "mainnet",
    },
  },
  functions: {
    addSuffix: {
      inputTypes: ["Utf8"],
      outputType: "Utf8",
      source: ctx.functionSource("add_suffix.js"),
    },
  },
}))
